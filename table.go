package quasar

import (
	"github.com/dgraph-io/badger"
)

// TableConfig is used to configure a table.
type TableConfig struct {
	// The prefix for all table keys.
	Prefix string
}

// Table manages the storage of positions markers.
type Table struct {
	db     *DB
	config TableConfig
	prefix []byte
}

// CreateTable will create a table that stores positions in the provided db.
func CreateTable(db *DB, config TableConfig) (*Table, error) {
	// check prefix
	if config.Prefix == "" {
		panic("quasar: missing prefix")
	}

	// create table
	t := &Table{
		db:     db,
		config: config,
		prefix: append([]byte(config.Prefix), '!'),
	}

	return t, nil
}

// Set will write the specified sequences to the table.
func (m *Table) Set(name string, sequences []uint64) error {
	// set entry
	err := retryUpdate(m.db, func(txn *badger.Txn) error {
		return txn.SetEntry(&badger.Entry{
			Key:   m.makeKey(name),
			Value: EncodeSequences(sequences),
		})
	})
	if err != nil {
		return err
	}

	return nil
}

// Get will read the specified sequences from the table.
func (m *Table) Get(name string) ([]uint64, error) {
	// prepare sequences
	var sequences []uint64

	// read entries
	err := m.db.View(func(txn *badger.Txn) error {
		// get item
		item, err := txn.Get(m.makeKey(name))
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		// parse key
		err = item.Value(func(val []byte) error {
			sequences, err = DecodeSequences(val)
			return err
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return sequences, nil
}

// Delete will remove the specified sequences from the table.
func (m *Table) Delete(name string) error {
	// delete item
	err := retryUpdate(m.db, func(txn *badger.Txn) error {
		return txn.Delete(m.makeKey(name))
	})
	if err != nil {
		return err
	}

	return nil
}

// Count will return the number of stored sequences.
func (m *Table) Count() (int, error) {
	// prepare counter
	var counter int

	// iterate over all keys
	err := m.db.View(func(txn *badger.Txn) error {
		// create iterator (key only)
		iter := txn.NewIterator(badger.IteratorOptions{
			Prefix: m.prefix,
		})
		defer iter.Close()

		// compute start
		start := m.makeKey("")

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
func (m *Table) Range() (uint64, uint64, error) {
	// prepare counter
	var min, max uint64

	// iterate over all keys
	err := m.db.View(func(txn *badger.Txn) error {
		// prepare options
		opts := badger.DefaultIteratorOptions
		opts.Prefix = m.prefix

		// create iterator
		iter := txn.NewIterator(badger.IteratorOptions{
			Prefix:         m.prefix,
			PrefetchValues: true,
			PrefetchSize:   100,
		})
		defer iter.Close()

		// compute start
		start := m.makeKey("")

		// iterate over all keys
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// parse key
			var sequences []uint64
			var err error
			err = iter.Item().Value(func(val []byte) error {
				sequences, err = DecodeSequences(val)
				return err
			})
			if err != nil {
				return err
			}

			// continue if empty
			if len(sequences) == 0 {
				continue
			}

			// set min
			if min == 0 || sequences[0] < min {
				min = sequences[0]
			}

			// set max
			if max == 0 || sequences[len(sequences)-1] > max {
				max = sequences[len(sequences)-1]
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
func (m *Table) Clear() error {
	// drop all entries
	err := m.db.DropPrefix(m.prefix)
	if err != nil {
		return err
	}

	return nil
}

func (m *Table) makeKey(name string) []byte {
	b := make([]byte, 0, len(m.prefix)+len(name))
	return append(append(b, m.prefix...), []byte(name)...)
}
