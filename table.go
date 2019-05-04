package quasar

import (
	"bytes"

	"github.com/dgraph-io/badger"
)

// Table manages the storage of positions in a ledger.
type Table struct {
	db     *DB
	prefix []byte
}

// CreateTable will create a table that stores positions in the provided db.
func CreateTable(db *DB, prefix string) (*Table, error) {
	// create table
	t := &Table{
		db:     db,
		prefix: append([]byte(prefix), ':'),
	}

	return t, nil
}

// Set will write the specified position to the table.
func (t *Table) Set(name string, position uint64) error {
	// set entry
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(&badger.Entry{
			Key:   t.makeKey(name),
			Value: EncodeSequence(position),
		})
	})
	if err != nil {
		return err
	}

	return nil
}

// Get will read the specified position from the table.
func (t *Table) Get(name string) (uint64, bool, error) {
	// prepare position and found
	var position uint64
	var found bool

	// prepare error
	var decodeErr error

	// read entries
	err := t.db.View(func(txn *badger.Txn) error {
		// get item
		item, err := txn.Get(t.makeKey(name))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		// parse key
		err = item.Value(func(val []byte) error {
			position, decodeErr = DecodeSequence(val)
			found = true
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return 0, false, err
	} else if decodeErr != nil {
		return 0, false, decodeErr
	}

	return position, found, nil
}

// Delete will remove the specified position from the table.
func (t *Table) Delete(name string) error {
	// delete item
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(t.makeKey(name))
	})
	if err != nil {
		return err
	}

	return nil
}

// Count will return the number of stored positions.
func (t *Table) Count() (int, error) {
	// prepare counter
	var count int

	// iterate over all keys
	err := t.db.View(func(txn *badger.Txn) error {
		// create iterator (key only)
		iter := txn.NewIterator(badger.IteratorOptions{})
		defer iter.Close()

		// compute start
		start := t.makeKey("")

		// iterate over all keys
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// stop if key does not match prefix
			if !bytes.HasPrefix(iter.Item().Key(), t.prefix) {
				break
			}

			// increment counter
			count++
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (t *Table) makeKey(name string) []byte {
	b := make([]byte, 0, len(t.prefix)+len(name))
	return append(append(b, t.prefix...), []byte(name)...)
}
