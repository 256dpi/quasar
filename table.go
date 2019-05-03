package quasar

import (
	"os"

	"github.com/dgraph-io/badger"
)

// Table manages the storage of consumer offsets.
type Table struct {
	db *badger.DB
}

// OpenLedger will open the table in the specified directory. If no table exists
// a new one will be created.
func OpenTable(dir string) (*Table, error) {
	// prepare options
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	opts.Logger = nil

	// ensure directory
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return nil, err
	}

	// open db
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	// create table
	t := &Table{
		db: db,
	}

	return t, nil
}

// Write will write the specified offset to the table.
func (t *Table) Set(name string, offset uint64) error {
	// set entry
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(&badger.Entry{
			Key:   []byte(name),
			Value: EncodeSequence(offset),
		})
	})
	if err != nil {
		return err
	}

	return nil
}

// Get will read the specified offset from the table.
func (t *Table) Get(name string) (uint64, bool, error) {
	// prepare offset & found
	var offset uint64
	var found bool

	// prepare error
	var decodeErr error

	// read entries
	err := t.db.View(func(txn *badger.Txn) error {
		// get item
		item, err := txn.Get([]byte(name))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		// parse key
		err = item.Value(func(val []byte) error {
			offset, decodeErr = DecodeSequence(val)
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

	return offset, found, nil
}

// Delete will remove the specified offset from the table.
func (t *Table) Delete(name string) error {
	// delete item
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(name))
	})
	if err != nil {
		return err
	}

	return nil
}

// Count will return the number of stored offsets.
func (t *Table) Count() (uint64, error) {
	// prepare counter
	var count uint64

	// iterate over all keys
	err := t.db.View(func(txn *badger.Txn) error {
		// create iterator (key only)
		iter := txn.NewIterator(badger.IteratorOptions{})
		defer iter.Close()

		// iterate over all keys
		for iter.Rewind(); iter.Valid(); iter.Next() {
			count++
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return count, nil
}

// Clear will delete all offsets from the table.
func (t *Table) Clear() error {
	// drop all offsets
	err := t.db.DropAll()
	if err != nil {
		return err
	}

	return nil
}

// Close will close the table.
func (t *Table) Close() error {
	// close db
	err := t.db.Close()
	if err != nil {
		return err
	}

	return nil
}
