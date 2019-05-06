package quasar

import (
	"github.com/dgraph-io/badger"
)

// MatrixOptions are used to configure a matrix.
type MatrixOptions struct {
	// The prefix for all matrix keys.
	Prefix string
}

// Matrix manages the storage of positions markers.
type Matrix struct {
	db     *DB
	opts   MatrixOptions
	prefix []byte
}

// CreateMatrix will create a matrix that stores positions in the provided db.
func CreateMatrix(db *DB, opts MatrixOptions) (*Matrix, error) {
	// create matrix
	t := &Matrix{
		db:     db,
		opts:   opts,
		prefix: append([]byte(opts.Prefix), ':'),
	}

	return t, nil
}

// Set will write the specified sequences to the matrix.
func (t *Matrix) Set(name string, sequences []uint64) error {
	// set entry
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(&badger.Entry{
			Key:   t.makeKey(name),
			Value: EncodeSequences(sequences),
		})
	})
	if err != nil {
		return err
	}

	return nil
}

// Get will read the specified sequences from the matrix.
func (t *Matrix) Get(name string) ([]uint64, error) {
	// prepare sequences
	var sequences []uint64

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
func (t *Matrix) Delete(name string) error {
	// delete item
	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(t.makeKey(name))
	})
	if err != nil {
		return err
	}

	return nil
}

// Count will return the number of stored sequences.
func (t *Matrix) Count() (int, error) {
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
func (t *Matrix) Range() (uint64, uint64, error) {
	// prepare counter
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

func (t *Matrix) makeKey(name string) []byte {
	b := make([]byte, 0, len(t.prefix)+len(name))
	return append(append(b, t.prefix...), []byte(name)...)
}
