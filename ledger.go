package quasar

import (
	"bytes"
	"errors"
	"strconv"
	"sync"

	"github.com/dgraph-io/badger"
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

// Ledger manages the storage of sequential entries.
type Ledger struct {
	db *DB

	entryPrefix []byte
	cachePrefix []byte

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
func CreateLedger(db *DB, prefix string) (*Ledger, error) {
	// create ledger
	l := &Ledger{
		db:          db,
		entryPrefix: append([]byte(prefix), []byte(":#")...),
		cachePrefix: append([]byte(prefix), []byte(":!")...),
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
	var length int64
	var head uint64

	// read length and head from entries and cache
	err := l.db.View(func(txn *badger.Txn) error {
		// create iterator (key only)
		iter := txn.NewIterator(badger.IteratorOptions{})
		defer iter.Close()

		// compute start
		start := l.makeEntryKey(0)

		// iterate over all keys
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// stop if prefix does not match
			if !bytes.HasPrefix(iter.Item().Key(), l.entryPrefix) {
				break
			}

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

		// read cached head if collapsed
		if length <= 0 {
			// read cached head
			item, err := txn.Get(l.makeCacheKey("head"))
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
	l.length = int(length)
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
	head := l.Head()

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
	err := l.db.Update(func(txn *badger.Txn) error {
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

		// cache head
		return txn.SetEntry(&badger.Entry{
			Key:   l.makeCacheKey("head"),
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

	// read entries
	err := l.db.View(func(txn *badger.Txn) error {
		// create iterator
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		// compute start
		start := l.makeEntryKey(sequence)

		// iterate until enough entries have been loaded
		for iter.Seek(start); iter.Valid() && len(list) < amount; iter.Next() {
			// stop if prefix does not match
			if !bytes.HasPrefix(iter.Item().Key(), l.entryPrefix) {
				break
			}

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

// Delete will remove all entries up to and including the specified sequence
// from the ledger.
func (l *Ledger) Delete(sequence uint64) error {
	// acquire mutex
	l.deleteMutex.Lock()
	defer l.deleteMutex.Unlock()

	// prepare counter
	var counter int

	// begin database update
	err := l.db.Update(func(txn *badger.Txn) error {
		// create iterator
		iter := txn.NewIterator(badger.IteratorOptions{})
		defer iter.Close()

		// compute start and needle
		start := l.makeEntryKey(0)
		needle := l.makeEntryKey(sequence)

		// delete all entries
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// stop if prefix does not match
			if !bytes.HasPrefix(iter.Item().Key(), l.entryPrefix) {
				break
			}

			// delete entry
			err := txn.Delete(iter.Item().KeyCopy(nil))
			if err != nil {
				return err
			}

			// increment counter
			counter++

			// stop if found element is needle
			if bytes.Equal(needle, iter.Item().Key()) {
				break
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// decrement length
	l.mutex.Lock()
	l.length -= counter
	l.mutex.Unlock()

	return nil
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

	// drop cache
	err = l.db.DropPrefix(l.cachePrefix)
	if err != nil {
		return err
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
	return append(append(b, l.entryPrefix...), EncodeSequence(seq)...)
}

func (l *Ledger) makeCacheKey(name string) []byte {
	b := make([]byte, 0, len(l.cachePrefix)+SequenceLength)
	return append(append(b, l.cachePrefix...), name...)
}
