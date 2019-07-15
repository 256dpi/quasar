package quasar

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"sync"

	"github.com/petermattis/pebble"
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

	cache       *Buffer
	entryPrefix []byte
	fieldPrefix []byte

	receivers map[chan<- uint64]struct{}

	length int
	head   uint64
	tail   uint64
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
		cache:       cache,
		entryPrefix: append([]byte(config.Prefix), '#'),
		fieldPrefix: append([]byte(config.Prefix), '!'),
		receivers:   make(map[chan<- uint64]struct{}),
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
	var tail uint64

	// create iterator
	iter := l.db.NewIter(prefixIterator(l.entryPrefix))
	defer iter.Close()

	// compute start
	start := l.makeEntryKey(0)

	// read all entries
	for iter.SeekGE(start); iter.Valid(); iter.Next() {
		// increment length
		length++

		// parse key
		seq, err := DecodeSequence(iter.Key()[len(l.entryPrefix):])
		if err != nil {
			return err
		}

		// set head
		head = seq

		// set tail
		if tail == 0 {
			tail = seq
		}

		// cache entry
		if l.cache != nil {
			// prepare value
			value := make([]byte, len(iter.Value()))
			copy(value, iter.Value())

			// store entry
			l.cache.Push(Entry{
				Sequence: seq,
				Payload:  value,
			})
		}
	}

	// check errors
	err := iter.Error()
	if err != nil {
		return err
	}

	// read stored head if collapsed
	if length <= 0 {
		// read stored head
		value, err := l.db.Get(l.makeFieldKey("head"))
		if err != nil && err != pebble.ErrNotFound {
			return err
		}

		// parse head if present
		if value != nil {
			head, err = DecodeSequence(value)
			if err != nil {
				return err
			}
		}
	}

	// set tail to head if collapsed
	if tail == 0 {
		tail = head
	}

	// set length and head
	l.length = length
	l.head = head
	l.tail = tail

	return nil
}

// Write will write the specified entries to the ledger. No entries have been
// written if an error has been returned. Monotonicity of the written entries
// is checked against the current head.
func (l *Ledger) Write(entries ...Entry) error {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// get head and length
	head := l.head
	length := l.length

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
	batch := l.db.NewBatch()
	defer batch.Close()

	// add all entries
	for _, entry := range entries {
		err := batch.Set(l.makeEntryKey(entry.Sequence), entry.Payload, l.db.wo)
		if err != nil {
			return err
		}
	}

	// add head
	err := batch.Set(l.makeFieldKey("head"), []byte(strconv.FormatUint(head, 10)), l.db.wo)
	if err != nil {
		return err
	}

	// perform write
	err = batch.Commit(l.db.wo)
	if err != nil {
		return err
	}

	// set length and head
	l.length += len(entries)
	l.head = head

	// cache all entries
	if l.cache != nil {
		l.cache.Push(entries...)
	}

	// send notifications to all receivers and skip full receivers
	for receiver := range l.receivers {
		select {
		case receiver <- head:
		default:
		}
	}

	return nil
}

// Read will read entries from and including the specified sequence up to the
// requested amount of entries.
func (l *Ledger) Read(sequence uint64, amount int) ([]Entry, error) {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

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

	// create iterator
	iter := l.db.NewIter(prefixIterator(l.entryPrefix))
	defer iter.Close()

	// compute start
	start := l.makeEntryKey(sequence)

	// read all keys
	for iter.SeekGE(start); iter.Valid() && len(list) < amount; iter.Next() {
		// prepare value
		value := make([]byte, len(iter.Value()))
		copy(value, iter.Value())

		// parse key
		seq, err := DecodeSequence(iter.Key()[len(l.entryPrefix):])
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
	err := iter.Error()
	if err != nil {
		return nil, err
	}

	return list, nil
}

// Index will return the sequence of the specified index in the ledger. Negative
// indexes are counted backwards from the head. If the index exceeds the current
// length, the sequence of the last entry and false is returned. If the ledger
// is empty the current head (zero if unused) and false will be returned.
func (l *Ledger) Index(index int) (uint64, bool, error) {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

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

	// create iterator
	iter := l.db.NewIter(prefixIterator(l.entryPrefix))
	defer iter.Close()

	// compute start
	start := l.makeEntryKey(0)
	if backward {
		start = l.makeEntryKey(math.MaxUint64)
	}

	// prepare seek
	seek := func() { iter.SeekGE(start) }
	if backward {
		seek = func() { iter.SeekLT(start) }
	}

	// prepare next
	next := func() { iter.Next() }
	if backward {
		next = func() { iter.Prev() }
	}

	// prepare counter
	counter := 0

	// read all keys
	for seek(); iter.Valid(); next() {
		// increment counter
		counter++

		// check counter
		if counter >= (index + 1) {
			// parse key
			seq, err := DecodeSequence(iter.Key()[len(l.entryPrefix):])
			if err != nil {
				return 0, false, err
			}

			// set sequence
			sequence = seq

			// set flag
			existing = true

			break
		}
	}

	// check errors
	err := iter.Error()
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
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// never delete beyond the current head
	if sequence > l.head {
		sequence = l.head
	}

	// skip if ledger is empty or sequence is at or behind tail
	if l.length == 0 || sequence <= l.tail {
		return 0, nil
	}

	// prepare counter
	var counter int

	// determine if ledger is fully cached
	fullyCached := l.config.Cache > 0 && l.config.Limit > 0 && l.config.Cache >= l.config.Limit

	// count entries from index or database
	if fullyCached {
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
	} else {
		// create iterator
		iter := l.db.NewIter(prefixIterator(l.entryPrefix))
		defer iter.Close()

		// compute start
		start := l.makeEntryKey(0)

		// compute needle
		needle := l.makeEntryKey(sequence)

		// count all deletable entries
		for iter.SeekGE(start); iter.Valid(); iter.Next() {
			// stop if key is after needle
			if bytes.Compare(iter.Key(), needle) > 0 {
				break
			}

			// increment counter
			counter++
		}

		// check errors
		err := iter.Error()
		if err != nil {
			return 0, err
		}
	}

	// prepare batch
	batch := l.db.NewBatch()
	defer batch.Close()

	// delete entries
	err := batch.DeleteRange(l.makeEntryKey(l.tail+1), l.makeEntryKey(sequence+1), l.db.wo)
	if err != nil {
		return 0, err
	}

	// write batch
	err = batch.Commit(l.db.wo)
	if err != nil {
		return 0, err
	}

	// decrement length
	l.length -= counter

	// set tail
	l.tail = sequence

	// remove deleted entries from cache
	if l.cache != nil {
		l.cache.Trim(func(entry Entry) bool {
			return entry.Sequence <= sequence
		})
	}

	return counter, nil
}

// Length will return the number of stored entries.
func (l *Ledger) Length() int {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.length
}

// Head will return the last committed sequence. This value can be checked
// periodically to asses whether new entries have been added.
func (l *Ledger) Head() uint64 {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.head
}

// Tail will return the last deleted sequence. This value can be checked
// periodically to asses whether entries haven been deleted.
func (l *Ledger) Tail() uint64 {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.tail
}

// Subscribe will subscribe the specified channel to changes to the last
// sequence stored in the ledger. Notifications will be skipped if the specified
// channel is not writable for some reason.
func (l *Ledger) Subscribe(receiver chan<- uint64) {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// store receiver
	l.receivers[receiver] = struct{}{}
}

// Unsubscribe will remove a previously subscribed receiver.
func (l *Ledger) Unsubscribe(receiver chan<- uint64) {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// delete receiver
	delete(l.receivers, receiver)
}

func (l *Ledger) makeEntryKey(seq uint64) []byte {
	b := make([]byte, 0, len(l.entryPrefix)+EncodedSequenceLength)
	return append(append(b, l.entryPrefix...), EncodeSequence(seq, false)...)
}

func (l *Ledger) makeFieldKey(name string) []byte {
	b := make([]byte, 0, len(l.fieldPrefix)+len(name))
	return append(append(b, l.fieldPrefix...), name...)
}
