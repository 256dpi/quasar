package quasar

import (
	"bytes"

	"github.com/dgraph-io/badger"
)

// TableOptions are used to configure a table.
type TableOptions struct {
	// The prefix for all table keys.
	Prefix string
}

// Table manages the storage of positions.
type Table struct {
	db     *DB
	opts   TableOptions
	prefix []byte
}

// CreateTable will create a table that stores positions in the provided db.
func CreateTable(db *DB, opts TableOptions) (*Table, error) {
	// create table
	t := &Table{
		db:     db,
		opts:   opts,
		prefix: append([]byte(opts.Prefix), ':'),
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
func (t *Table) Get(name string) (uint64, error) {
	// prepare position and found
	var position uint64

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
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return 0, err
	} else if decodeErr != nil {
		return 0, decodeErr
	}

	return position, nil
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

// Range will return the range of stored positions.
func (t *Table) Range() (uint64, uint64, error) {
	// prepare counter
	var min, max uint64

	// iterate over all keys
	err := t.db.View(func(txn *badger.Txn) error {
		// create iterator
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		// compute start
		start := t.makeKey("")

		// iterate over all keys
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// stop if key does not match prefix
			if !bytes.HasPrefix(iter.Item().Key(), t.prefix) {
				break
			}

			// prepare position
			var position uint64
			var err error

			// parse key
			err = iter.Item().Value(func(val []byte) error {
				position, err = DecodeSequence(val)
				return err
			})
			if err != nil {
				return err
			}

			// set min
			if min == 0 || position < min {
				min = position
			}

			// set max
			if max == 0 || position > max {
				max = position
			}
		}

		return nil
	})
	if err != nil {
		return 0, 0, err
	}

	return min, max, nil
}

func (t *Table) makeKey(name string) []byte {
	b := make([]byte, 0, len(t.prefix)+len(name))
	return append(append(b, t.prefix...), []byte(name)...)
}
