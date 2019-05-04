package quasar

import (
	"bytes"
	"errors"
	"sync"

	"github.com/dgraph-io/badger"
)

// ErrNotMonotonic is returned for write attempts that are not monotonic.
var ErrNotMonotonic = errors.New("not monotonic")

// Entry is a single entry in the ledger.
type Entry struct {
	Sequence uint64
	Payload  []byte
}

// Ledger manages the storage of sequential entries.
type Ledger struct {
	db     *DB
	prefix []byte

	receivers sync.Map

	length int
	head   uint64
	mutex  sync.Mutex

	writeMutex sync.Mutex
}

// CreateLedger will create a ledger that stores entries in the provided db.
func CreateLedger(db *DB, prefix string) (*Ledger, error) {
	// create ledger
	l := &Ledger{
		db:     db,
		prefix: append([]byte(prefix), ':'),
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

	// count all items and find head
	err := l.db.View(func(txn *badger.Txn) error {
		// create iterator (key only)
		iter := txn.NewIterator(badger.IteratorOptions{})
		defer iter.Close()

		// compute start
		start := l.makeKey(0)

		// iterate over all keys
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// stop if prefix does not match
			if !bytes.HasPrefix(iter.Item().Key(), l.prefix) {
				break
			}

			// increment length
			length++

			// parse key and set head
			seq, err := DecodeSequence(iter.Item().Key()[len(l.prefix):])
			if err != nil {
				return err
			}

			// set head
			head = seq
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
	head := l.Head()

	// check and collect head
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
				Key:   l.makeKey(entry.Sequence),
				Value: entry.Payload,
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// acquire mutex
	l.mutex.Lock()

	// increment length
	l.length += len(entries)

	// set head sequence
	if head > l.head {
		l.head = head
	}

	// release mutex
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
		start := l.makeKey(sequence)

		// iterate until enough entries have been loaded
		for iter.Seek(start); iter.Valid() && len(list) < amount; iter.Next() {
			// stop if prefix does not match
			if !bytes.HasPrefix(iter.Item().Key(), l.prefix) {
				break
			}

			// copy values
			pld, err := iter.Item().ValueCopy(nil)
			if err != nil {
				return err
			}

			// parse key
			seq, err := DecodeSequence(iter.Item().Key()[len(l.prefix):])
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
	// prepare counter
	var counter int

	// begin database update
	err := l.db.Update(func(txn *badger.Txn) error {
		// create iterator
		iter := txn.NewIterator(badger.IteratorOptions{})
		defer iter.Close()

		// compute start and needle
		start := l.makeKey(0)
		needle := l.makeKey(sequence)

		// delete all entries
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// stop if prefix does not match
			if !bytes.HasPrefix(iter.Item().Key(), l.prefix) {
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

func (l *Ledger) makeKey(seq uint64) []byte {
	b := make([]byte, 0, len(l.prefix)+SequenceLength)
	return append(append(b, l.prefix...), EncodeSequence(seq)...)
}
