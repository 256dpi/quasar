package quasar

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"sync"

	"github.com/dgraph-io/badger/v2"
)

// ErrNotMonotonic is returned for write attempts that are not monotonic.
var ErrNotMonotonic = errors.New("not monotonic")

// Entry is a single entry in the ledger.
type Entry struct {
	// The entries sequence (must be greater than zero).
	Sequence uint64

	// The entries payload.
	Payload []byte
}

// LedgerConfig are used to configure a ledger.
type LedgerConfig struct {
	// The prefix for all ledger keys.
	Prefix string

	// The amount of entries to cache in memory.
	Cache int
}

// Ledger manages the storage of sequential entries.
type Ledger struct {
	db     *DB
	config LedgerConfig

	cache       *Cache
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
	// prepare cache
	var cache *Cache
	if config.Cache > 0 {
		cache = NewCache(config.Cache)
	}

	// create ledger
	l := &Ledger{
		db:          db,
		config:      config,
		cache:       cache,
		entryPrefix: append([]byte(config.Prefix), []byte(":#")...),
		fieldPrefix: append([]byte(config.Prefix), []byte(":!")...),
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

	// read length and head from entries and fields
	err := l.db.View(func(txn *badger.Txn) error {
		// create iterator (key only)
		iter := txn.NewIterator(badger.IteratorOptions{
			Prefix: l.entryPrefix,
		})
		defer iter.Close()

		// compute start
		start := l.makeEntryKey(0)

		// iterate over all keys
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// increment length
			length++

			// parse key and set head
			seq, err := DecodeSequence(iter.Item().Key()[len(l.entryPrefix):])
			if err != nil {
				return err
			}

			// set head
			head = seq
		}

		// read stored head if collapsed
		if length <= 0 {
			// read stored head
			item, err := txn.Get(l.makeFieldKey("head"))
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}

			// parse if present
			if item != nil {
				// parse length
				err = item.Value(func(val []byte) error {
					head, err = strconv.ParseUint(string(val), 10, 64)
					return err
				})
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
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
	l.mutex.Unlock()

	// check and update head
	for _, entry := range entries {
		// return immediately if not monotonic
		if entry.Sequence <= head {
			return ErrNotMonotonic
		}

		// set head
		head = entry.Sequence
	}

	// begin database update
	err := retryUpdate(l.db, func(txn *badger.Txn) error {
		// add all entries
		for _, entry := range entries {
			// add entry
			err := txn.SetEntry(&badger.Entry{
				Key:   l.makeEntryKey(entry.Sequence),
				Value: entry.Payload,
			})
			if err != nil {
				return err
			}
		}

		// store head
		return txn.SetEntry(&badger.Entry{
			Key:   l.makeFieldKey("head"),
			Value: []byte(strconv.FormatUint(head, 10)),
		})
	})
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
		l.cache.Add(entries...)
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
		l.cache.Scan(func(i int, entry Entry) bool {
			// stop if start sequence is not in cache
			if i == 0 && sequence < entry.Sequence {
				return false
			}

			// otherwise add item if in range
			if entry.Sequence >= sequence {
				list = append(list, entry)
			}

			// stop if list is full
			if len(list) >= amount {
				return false
			}

			return true
		})
	}

	// return cache list immediately
	if len(list) > 0 {
		return list, nil
	}

	// read entries
	err := l.db.View(func(txn *badger.Txn) error {
		// prepare options
		opts := badger.DefaultIteratorOptions
		opts.Prefix = l.entryPrefix

		// create iterator
		iter := txn.NewIterator(opts)
		defer iter.Close()

		// compute start
		start := l.makeEntryKey(sequence)

		// iterate until enough entries have been loaded
		for iter.Seek(start); iter.Valid() && len(list) < amount; iter.Next() {
			// copy values
			pld, err := iter.Item().ValueCopy(nil)
			if err != nil {
				return err
			}

			// parse key
			seq, err := DecodeSequence(iter.Item().Key()[len(l.entryPrefix):])
			if err != nil {
				return err
			}

			// add entry
			list = append(list, Entry{
				Sequence: seq,
				Payload:  pld,
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return list, nil
}

// Index will return the sequence of the specified index in the ledger. Negative
// indexes are counted backwards from the head.
func (l *Ledger) Index(index int) (uint64, error) {
	// compute direction
	backward := index < 0

	// make absolute if backward
	if backward {
		index *= -1
		index--
	}

	// prepare sequence
	var sequence uint64

	// find requested entry
	err := l.db.View(func(txn *badger.Txn) error {
		// create iterator (key only)
		iter := txn.NewIterator(badger.IteratorOptions{
			Reverse: backward,
			Prefix:  l.entryPrefix,
		})
		defer iter.Close()

		// compute start
		start := l.makeEntryKey(0)
		if backward {
			start = l.makeEntryKey(math.MaxUint64)
		}

		// prepare counter
		counter := 0

		// iterate over all keys
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// increment length
			counter++

			// check counter
			if counter < (index + 1) {
				continue
			}

			// otherwise parse key
			seq, err := DecodeSequence(iter.Item().Key()[len(l.entryPrefix):])
			if err != nil {
				return err
			}

			// set sequence
			sequence = seq

			break
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return sequence, nil
}

// Delete will remove all entries up to and including the specified sequence
// from the ledger.
func (l *Ledger) Delete(sequence uint64) error {
	// acquire mutex
	l.deleteMutex.Lock()
	defer l.deleteMutex.Unlock()

	// compute start and needle
	start := l.makeEntryKey(0)
	needle := l.makeEntryKey(sequence)

	// prepare counter
	var counter int

	// delete in multiple attempts to honor max batch count
	for {
		// perform partial delete
		end, n, err := l.partialDelete(start, needle)
		if err != nil {
			return err
		}

		// increment counter
		counter += n

		// check if end has been reached
		if end == nil {
			break
		}

		// set new start
		start = end
	}

	// decrement length
	l.mutex.Lock()
	l.length -= counter
	l.mutex.Unlock()

	// remove entries from cache
	if l.cache != nil {
		l.cache.Trim(func(entry Entry) bool {
			return entry.Sequence <= sequence
		})
	}

	return nil
}

func (l *Ledger) partialDelete(start, needle []byte) ([]byte, int, error) {
	// prepare counter
	var counter int

	// prepare end
	var end []byte

	// begin database update
	err := retryUpdate(l.db, func(txn *badger.Txn) error {
		// reset effects
		counter = 0
		end = nil

		// prepare options
		opts := badger.DefaultIteratorOptions
		opts.Prefix = l.entryPrefix

		// create iterator
		iter := txn.NewIterator(badger.IteratorOptions{})
		defer iter.Close()

		// delete all entries
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// copy key
			key := iter.Item().KeyCopy(nil)

			// delete entry
			err := txn.Delete(key)
			if err != nil {
				return err
			}

			// increment counter
			counter++

			// stop if found element is needle
			if bytes.Equal(needle, key) {
				break
			}

			// stop if exhausted
			if counter >= int(l.db.MaxBatchCount()-10) {
				end = key
				break
			}
		}

		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return end, counter, nil
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

// Clear will drop all ledger entries while maintaining the head. Clear will
// temporarily block concurrent writes and deletes and lock the underlying
// database. Other users uf the same database may receive errors due to the
// locked database.
func (l *Ledger) Clear() error {
	// acquire delete mutex
	l.deleteMutex.Lock()
	defer l.deleteMutex.Unlock()

	// acquire write mutex
	l.writeMutex.Lock()
	defer l.writeMutex.Unlock()

	// drop all entries
	err := l.db.DropPrefix(l.entryPrefix)
	if err != nil {
		return err
	}

	// reset cache
	if l.cache != nil {
		l.cache.Reset()
	}

	// reset length
	l.mutex.Lock()
	l.length = 0
	l.mutex.Unlock()

	return nil
}

// Reset will drop all ledger entries and reset the head. Reset  will
// temporarily block concurrent writes and deletes and lock the underlying
// database. Other users uf the same database may receive errors due to the
// locked database.
func (l *Ledger) Reset() error {
	// acquire delete mutex
	l.deleteMutex.Lock()
	defer l.deleteMutex.Unlock()

	// acquire write mutex
	l.writeMutex.Lock()
	defer l.writeMutex.Unlock()

	// drop entries
	err := l.db.DropPrefix(l.entryPrefix)
	if err != nil {
		return err
	}

	// drop fields
	err = l.db.DropPrefix(l.fieldPrefix)
	if err != nil {
		return err
	}

	// reset cache
	if l.cache != nil {
		l.cache.Reset()
	}

	// reset length
	l.mutex.Lock()
	l.length = 0
	l.head = 0
	l.mutex.Unlock()

	return nil
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
	b := make([]byte, 0, len(l.entryPrefix)+SequenceLength)
	return append(append(b, l.entryPrefix...), EncodeSequence(seq, false)...)
}

func (l *Ledger) makeFieldKey(name string) []byte {
	b := make([]byte, 0, len(l.fieldPrefix)+SequenceLength)
	return append(append(b, l.fieldPrefix...), name...)
}
