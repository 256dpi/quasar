package quasar

import (
	"github.com/dgraph-io/badger/v2"
)

// TableConfig are used to configure a table.
type TableConfig struct {
	// The prefix for all table keys.
	Prefix string
}

// Table manages the storage of positions.
type Table struct {
	db     *DB
	config TableConfig
	prefix []byte
}

// CreateTable will create a table that stores positions in the provided db.
func CreateTable(db *DB, config TableConfig) (*Table, error) {
	// create table
	t := &Table{
		db:     db,
		config: config,
		prefix: append([]byte(config.Prefix), ':'),
	}

	return t, nil
}

// Set will write the specified position to the table if it is higher as the
// already stored position.
func (t *Table) Set(name string, position uint64) error {
	// set entry
	err := retryUpdate(t.db, func(txn *badger.Txn) error {
		// compute key
		key := t.makeKey(name)

		// get current entry
		item, err := txn.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		// check against current value
		if item != nil {
			// decode stored value
			var seq uint64
			err = item.Value(func(val []byte) error {
				seq, err = DecodeSequence(val)
				return err
			})
			if err != nil {
				return err
			}

			// return immediately if lower or equal
			if position <= seq {
				return nil
			}
		}

		// write new entry
		err = txn.SetEntry(&badger.Entry{
			Key:   t.makeKey(name),
			Value: EncodeSequence(position, true),
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// Get will read the specified position from the table.
func (t *Table) Get(name string) (uint64, error) {
	// prepare position
	var position uint64

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
			position, err = DecodeSequence(val)
			return err
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return position, nil
}

// Delete will remove the specified position from the table.
func (t *Table) Delete(name string) error {
	// delete item
	err := retryUpdate(t.db, func(txn *badger.Txn) error {
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
	var counter int

	// iterate over all keys
	err := t.db.View(func(txn *badger.Txn) error {
		// create iterator (key only)
		iter := txn.NewIterator(badger.IteratorOptions{
			Prefix: t.prefix,
		})
		defer iter.Close()

		// compute start
		start := t.makeKey("")

		// iterate over all keys and increment counter
		for iter.Seek(start); iter.Valid(); iter.Next() {
			counter++
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return counter, nil
}

// Range will return the range of stored positions.
func (t *Table) Range() (uint64, uint64, error) {
	// prepare min and max
	var min, max uint64

	// iterate over all keys
	err := t.db.View(func(txn *badger.Txn) error {
		// prepare options
		opts := badger.DefaultIteratorOptions
		opts.Prefix = t.prefix

		// create iterator
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		// compute start
		start := t.makeKey("")

		// iterate over all keys
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// parse key
			var position uint64
			var err error
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

// Clear will drop all table entries. Clear will temporarily block concurrent
// writes and deletes and lock the underlying database. Other users uf the same
// database may receive errors due to the locked database.
func (t *Table) Clear() error {
	// drop all entries
	err := t.db.DropPrefix(t.prefix)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) makeKey(name string) []byte {
	b := make([]byte, 0, len(t.prefix)+len(name))
	return append(append(b, t.prefix...), []byte(name)...)
}
