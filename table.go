package quasar

import (
	"github.com/dgraph-io/badger"
)

// Table manages the storage of positions in a ledger.
type Table struct {
	db *badger.DB
}

// CreateTable will create a table that stores positions in the provided db.
func CreateTable(db *DB) (*Table, error) {
	// create table
	t := &Table{
		db: db,
	}

	return t, nil
}

// Set will write the specified position to the table.
func (t *Table) Set(name string, position uint64) error {
	// set entry
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(&badger.Entry{
			Key:   []byte(name),
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
		item, err := txn.Get([]byte(name))
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
		return txn.Delete([]byte(name))
	})
	if err != nil {
		return err
	}

	return nil
}

// Count will return the number of stored positions.
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
