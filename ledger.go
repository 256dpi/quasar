package quasar

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"sync"

	"github.com/tecbot/gorocksdb"
)

// ErrNotMonotonic is returned for write attempts that are not monotonic.
var ErrNotMonotonic = errors.New("not monotonic")

// ErrLimitReached is returned for write attempts that go beyond the allowed
// ledger length.
var ErrLimitReached = errors.New("limit reached")

// Entry is a single entry in the ledger.
type Entry struct {
	// The entries sequence (must be greater than zero).
	Sequence uint64

	// The entries payload that is written to disk.
	Payload []byte

	// The reference to a shared object. This can be used with cached ledgers
	// to retain a reference to a decoded object of the entry.
	Object interface{}
}

// LedgerConfig is used to configure a ledger.
type LedgerConfig struct {
	// The prefix for all ledger keys.
	Prefix string

	// The amount of entries to cache in memory.
	Cache int

	// The maximum length of the ledger. Write() will return ErrLimitReached if
	// the ledger is longer than this value.
	Limit int
}

// Ledger manages the storage of sequential entries.
type Ledger struct {
	db     *DB
	config LedgerConfig

	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions

	cache       *Buffer
	entryPrefix []byte
	fieldPrefix []byte

	receivers   sync.Map
	writeMutex  sync.Mutex
	deleteMutex sync.Mutex

	length int
	head   uint64
	mutex  sync.Mutex
}

// CreateLedger will create a ledger that stores entries in the provided db.
// Read, write and delete requested can be issued concurrently to maximize
// performance. However, only one goroutine may write entries at the same time.
func CreateLedger(db *DB, config LedgerConfig) (*Ledger, error) {
	// check prefix
	if config.Prefix == "" {
		panic("quasar: missing prefix")
	}

	// prepare cache
	var cache *Buffer
	if config.Cache > 0 {
		cache = NewBuffer(config.Cache)
	}

	// create ledger
	l := &Ledger{
		db:          db,
		config:      config,
		ro:          gorocksdb.NewDefaultReadOptions(),
		wo:          gorocksdb.NewDefaultWriteOptions(),
		cache:       cache,
		entryPrefix: append([]byte(config.Prefix), '#'),
		fieldPrefix: append([]byte(config.Prefix), '!'),
	}

	// init ledger
	err := l.init()
	if err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Ledger) init() error {
	// prepare length and head
	var length int
	var head uint64

	// prepare opts
	opts := gorocksdb.NewDefaultReadOptions()
	defer opts.Destroy()

	// iterate over all keys
	iter := l.db.NewIterator(opts)
	defer iter.Close()

	// compute start
	start := l.makeEntryKey(0)

	// read all keys
	for iter.Seek(start); iter.ValidForPrefix(l.entryPrefix); iter.Next() {
		// increment length
		length++

		// parse key and set head
		seq, err := DecodeSequence(iter.Key().Data()[len(l.entryPrefix):])
		if err != nil {
			return err
		}

		// cache entry
		if l.cache != nil {
			// prepare value
			value := make([]byte, iter.Value().Size())
			copy(value, iter.Value().Data())

			// store entry
			l.cache.Push(Entry{
				Sequence: seq,
				Payload:  value,
			})
		}

		// set head
		head = seq
	}

	// check errors
	err := iter.Err()
	if err != nil {
		panic(err)
	}

	// read stored head if collapsed
	if length <= 0 {
		// read stored head
		item, err := l.db.Get(l.ro, l.makeFieldKey("head"))
		if err != nil {
			return err
		}

		// parse if present
		if item.Exists() {
			// parse length
			head, err = DecodeSequence(item.Data())
			if err != nil {
				return err
			}
		}
	}

	// set length and head
	l.length = length
	l.head = head

	return nil
}

// Write will write the specified entries to the ledger. No entries have been
// written if an error has been returned. Monotonicity of the written entries
// is checked against the current head.
func (l *Ledger) Write(entries ...Entry) error {
	// acquire mutex
	l.writeMutex.Lock()
	defer l.writeMutex.Unlock()

	// get head
	l.mutex.Lock()
	head := l.head
	length := l.length
	l.mutex.Unlock()

	// check length
	if l.config.Limit > 0 && length+len(entries) > l.config.Limit {
		return ErrLimitReached
	}

	// check and update head
	for _, entry := range entries {
		// return immediately if not monotonic
		if entry.Sequence <= head {
			return ErrNotMonotonic
		}

		// set head
		head = entry.Sequence
	}

	// prepare batch
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	// add all entries
	for _, entry := range entries {
		batch.Put(l.makeEntryKey(entry.Sequence), entry.Payload)
	}

	// add head
	batch.Put(l.makeFieldKey("head"), []byte(strconv.FormatUint(head, 10)))

	// perform write
	err := l.db.Write(l.wo, batch)
	if err != nil {
		return err
	}

	// set length and head
	l.mutex.Lock()
	l.length += len(entries)
	l.head = head
	l.mutex.Unlock()

	// cache all entries
	if l.cache != nil {
		l.cache.Push(entries...)
	}

	// send notifications to all receivers and skip full receivers
	l.receivers.Range(func(_, value interface{}) bool {
		select {
		case value.(chan<- uint64) <- head:
		default:
		}

		return true
	})

	return nil
}

// Read will read entries from and including the specified sequence up to the
// requested amount of entries.
func (l *Ledger) Read(sequence uint64, amount int) ([]Entry, error) {
	// prepare list
	list := make([]Entry, 0, amount)

	// attempt to read from cache if available
	if l.cache != nil {
		// prepare counter
		i := 0

		// iterate through buffer
		l.cache.Scan(func(entry Entry) bool {
			// stop if start sequence is not in cache
			if i == 0 && sequence < entry.Sequence {
				return false
			}

			// otherwise add item if in range and stop if list is full
			if entry.Sequence >= sequence {
				list = append(list, entry)
				if len(list) >= amount {
					return false
				}
			}

			// increment counter
			i++

			return true
		})
	}

	// return cached entries immediately
	if len(list) > 0 {
		return list, nil
	}

	// prepare opts
	opts := gorocksdb.NewDefaultReadOptions()
	defer opts.Destroy()

	// iterate over all keys
	iter := l.db.NewIterator(opts)
	defer iter.Close()

	// compute start
	start := l.makeEntryKey(sequence)

	// read all keys
	for iter.Seek(start); iter.ValidForPrefix(l.entryPrefix) && len(list) < amount; iter.Next() {
		// prepare value
		value := make([]byte, iter.Value().Size())
		copy(value, iter.Value().Data())

		// parse key
		seq, err := DecodeSequence(iter.Key().Data()[len(l.entryPrefix):])
		if err != nil {
			return nil, err
		}

		// add entry
		list = append(list, Entry{
			Sequence: seq,
			Payload:  value,
		})
	}

	// check errors
	err := iter.Err()
	if err != nil {
		panic(err)
	}

	return list, nil
}

// Index will return the sequence of the specified index in the ledger. Negative
// indexes are counted backwards from the head. If the index exceeds the current
// length, the sequence of the last entry and false is returned. If the ledger
// is empty the current head (zero if unused) and false will be returned.
func (l *Ledger) Index(index int) (uint64, bool, error) {
	// compute direction
	backward := index < 0

	// make absolute if backward
	if backward {
		index *= -1
		index--
	}

	// lookup backward index in cache if available
	if backward && l.cache != nil {
		entry, ok := l.cache.Index((index + 1) * -1)
		if ok {
			return entry.Sequence, true, nil
		}
	}

	// prepare sequence and existing
	var sequence uint64
	var existing bool

	// prepare opts
	opts := gorocksdb.NewDefaultReadOptions()
	defer opts.Destroy()

	// iterate over all keys
	iter := l.db.NewIterator(opts)
	defer iter.Close()

	// compute start
	start := l.makeEntryKey(0)
	if backward {
		start = l.makeEntryKey(math.MaxUint64)
	}

	// prepare seek
	seek := func() { iter.Seek(start) }
	if backward {
		seek = func() { iter.SeekForPrev(start) }
	}

	// prepare next
	next := func() { iter.Next() }
	if backward {
		next = func() { iter.Prev() }
	}

	// prepare counter
	counter := 0

	// read all keys
	for seek(); iter.ValidForPrefix(l.entryPrefix); next() {
		// otherwise parse key
		seq, err := DecodeSequence(iter.Key().Data()[len(l.entryPrefix):])
		if err != nil {
			return 0, false, err
		}

		// set sequence
		sequence = seq

		// increment length
		counter++

		// check counter
		if counter >= (index + 1) {
			existing = true
			break
		}
	}

	// check errors
	err := iter.Err()
	if err != nil {
		return 0, false, err
	}

	// check existing
	if !existing {
		return 0, false, nil
	}

	return sequence, true, nil
}

// Delete will remove all entries up to and including the specified sequence
// from the ledger.
func (l *Ledger) Delete(sequence uint64) (int, error) {
	// acquire mutex
	l.deleteMutex.Lock()
	defer l.deleteMutex.Unlock()

	// get head
	l.mutex.Lock()
	head := l.head
	l.mutex.Unlock()

	// never delete beyond the current head
	if sequence > head {
		sequence = head
	}

	// we can do a fast delete if the ledger is fully cached
	fast := l.config.Cache > 0 && l.config.Limit > 0 && l.config.Cache >= l.config.Limit

	// prepare values
	var n int
	var err error

	// perform delete
	if fast {
		n, err = l.cachedDelete(sequence)
	} else {
		n, err = l.uncachedDelete(sequence)
	}

	// check error
	if err != nil {
		return 0, err
	}

	// decrement length
	l.mutex.Lock()
	l.length -= n
	l.mutex.Unlock()

	// remove deleted entries from cache
	if l.cache != nil {
		l.cache.Trim(func(entry Entry) bool {
			return entry.Sequence <= sequence
		})
	}

	return n, nil
}

func (l *Ledger) uncachedDelete(sequence uint64) (int, error) {
	// prepare opts
	opts := gorocksdb.NewDefaultReadOptions()
	defer opts.Destroy()

	// iterate over all keys
	iter := l.db.NewIterator(opts)
	defer iter.Close()

	// compute start
	start := l.makeEntryKey(0)

	// compute needle
	needle := l.makeEntryKey(sequence)

	// prepare counter
	var counter int

	// count all deletable entries
	for iter.Seek(start); iter.ValidForPrefix(l.entryPrefix); iter.Next() {
		// stop if key is after needle
		if bytes.Compare(iter.Key().Data(), needle) > 0 {
			break
		}

		// increment counter
		counter++
	}

	// check errors
	err := iter.Err()
	if err != nil {
		return 0, err
	}

	// prepare batch
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	// add range delete
	batch.DeleteRange(start, l.makeEntryKey(sequence+1))

	// write batch
	err = l.db.Write(l.wo, batch)
	if err != nil {
		return 0, err
	}

	return counter, nil
}

func (l *Ledger) cachedDelete(sequence uint64) (int, error) {
	// prepare counter
	var counter int

	// read from cache
	l.cache.Scan(func(entry Entry) bool {
		// check if done
		if entry.Sequence > sequence {
			return false
		}

		// increment counter
		counter++

		return true
	})

	// prepare batch
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	// delete entries
	batch.DeleteRange(l.makeEntryKey(0), l.makeEntryKey(sequence+1))

	// write batch
	err := l.db.Write(l.wo, batch)
	if err != nil {
		return 0, err
	}

	return counter, nil
}

// Length will return the number of stored entries.
func (l *Ledger) Length() int {
	// get length
	l.mutex.Lock()
	length := l.length
	l.mutex.Unlock()

	return length
}

// Head will return the last committed sequence. This value can be checked
// periodically to asses whether new entries have been added.
func (l *Ledger) Head() uint64 {
	// get head
	l.mutex.Lock()
	head := l.head
	l.mutex.Unlock()

	return head
}

// Subscribe will subscribe the specified channel to changes to the last
// sequence stored in the ledger. Notifications will be skipped if the specified
// channel is not writable for some reason.
func (l *Ledger) Subscribe(receiver chan<- uint64) {
	l.receivers.Store(receiver, receiver)
}

// Unsubscribe will remove a previously subscribed receiver.
func (l *Ledger) Unsubscribe(receiver chan<- uint64) {
	l.receivers.Delete(receiver)
}

func (l *Ledger) makeEntryKey(seq uint64) []byte {
	b := make([]byte, 0, len(l.entryPrefix)+EncodedSequenceLength)
	return append(append(b, l.entryPrefix...), EncodeSequence(seq, false)...)
}

func (l *Ledger) makeFieldKey(name string) []byte {
	b := make([]byte, 0, len(l.fieldPrefix)+len(name))
	return append(append(b, l.fieldPrefix...), name...)
}
