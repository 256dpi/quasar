package quasar

import (
	"github.com/dgraph-io/badger/v2"
)

// MatrixConfig are used to configure a matrix.
type MatrixConfig struct {
	// The prefix for all matrix keys.
	Prefix string
}

// Matrix manages the storage of positions markers.
type Matrix struct {
	db     *DB
	config MatrixConfig
	prefix []byte
}

// CreateMatrix will create a matrix that stores positions in the provided db.
func CreateMatrix(db *DB, config MatrixConfig) (*Matrix, error) {
	// create matrix
	t := &Matrix{
		db:     db,
		config: config,
		prefix: append([]byte(config.Prefix), ':'),
	}

	return t, nil
}

// Set will write the specified sequences to the matrix.
func (m *Matrix) Set(name string, sequences []uint64) error {
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

// Get will read the specified sequences from the matrix.
func (m *Matrix) Get(name string) ([]uint64, error) {
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

// Delete will remove the specified sequences from the matrix.
func (m *Matrix) Delete(name string) error {
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
func (m *Matrix) Count() (int, error) {
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
func (m *Matrix) Range() (uint64, uint64, error) {
	// prepare counter
	var min, max uint64

	// iterate over all keys
	err := m.db.View(func(txn *badger.Txn) error {
		// prepare options
		opts := badger.DefaultIteratorOptions
		opts.Prefix = m.prefix

		// create iterator
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
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

// Clear will drop all matrix entries. Clear will temporarily block concurrent
// writes and deletes and lock the underlying database. Other users uf the same
// database may receive errors due to the locked database.
func (m *Matrix) Clear() error {
	// drop all entries
	err := m.db.DropPrefix(m.prefix)
	if err != nil {
		return err
	}

	return nil
}

func (m *Matrix) makeKey(name string) []byte {
	b := make([]byte, 0, len(m.prefix)+len(name))
	return append(append(b, m.prefix...), []byte(name)...)
}
