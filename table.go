package quasar

import (
	"sync"

	"github.com/dgraph-io/badger"
)

// TableConfig is used to configure a table.
type TableConfig struct {
	// The prefix for all table keys.
	Prefix string

	// Enable to keep all positions in memory.
	Cache bool
}

// Table manages the storage of positions markers.
type Table struct {
	db     *DB
	config TableConfig
	prefix []byte
	cache  *sync.Map
	mutex  sync.RWMutex
}

// CreateTable will create a table that stores position markers.
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

	// set cache
	if config.Cache {
		t.cache = new(sync.Map)
	}

	// init table
	err := t.init()
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Table) init() error {
	// load existing positions if cache is available
	if t.cache != nil {
		err := t.db.View(func(txn *badger.Txn) error {
			// prepare options
			opts := badger.DefaultIteratorOptions
			opts.Prefix = t.prefix

			// create iterator
			iter := txn.NewIterator(badger.IteratorOptions{
				Prefix:         t.prefix,
				PrefetchValues: true,
				PrefetchSize:   100,
			})
			defer iter.Close()

			// compute start
			start := t.makeKey("")

			// iterate over all keys
			for iter.Seek(start); iter.Valid(); iter.Next() {
				// parse positions
				var positions []uint64
				var err error
				err = iter.Item().Value(func(val []byte) error {
					positions, err = DecodeSequences(val)
					return err
				})
				if err != nil {
					return err
				}

				// continue if empty
				if len(positions) == 0 {
					continue
				}

				// cache positions
				t.cache.Store(string(iter.Item().Key()[len(t.prefix):]), positions)
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// Set will write the specified positions to the table.
func (t *Table) Set(name string, positions []uint64) error {
	// acquire mutex
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// ignore empty positions
	if len(positions) == 0 {
		return nil
	}

	// set entry
	err := retryUpdate(t.db, func(txn *badger.Txn) error {
		return txn.SetEntry(&badger.Entry{
			Key:   t.makeKey(name),
			Value: EncodeSequences(positions),
		})
	})
	if err != nil {
		return err
	}

	// update cache if available
	if t.cache != nil {
		t.cache.Store(name, positions)
	}

	return nil
}

// Get will read the specified positions from the table.
func (t *Table) Get(name string) ([]uint64, error) {
	// acquire mutex
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// get positions from cache if available
	if t.cache != nil {
		value, ok := t.cache.Load(name)
		if !ok {
			return nil, nil
		}

		// coerce value
		return value.([]uint64), nil
	}

	// prepare positions
	var positions []uint64

	// read positions
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
			positions, err = DecodeSequences(val)
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

	return positions, nil
}

// All will return a map with all stored positions.
func (t *Table) All() (map[string][]uint64, error) {
	// acquire mutex
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// prepare table
	table := make(map[string][]uint64)

	// get positions from cache if available
	if t.cache != nil {
		t.cache.Range(func(key, value interface{}) bool {
			table[key.(string)] = value.([]uint64)
			return true
		})

		return table, nil
	}

	// read positions from database
	err := t.db.View(func(txn *badger.Txn) error {
		// prepare options
		opts := badger.DefaultIteratorOptions
		opts.Prefix = t.prefix

		// create iterator
		iter := txn.NewIterator(badger.IteratorOptions{
			Prefix:         t.prefix,
			PrefetchValues: true,
			PrefetchSize:   100,
		})
		defer iter.Close()

		// compute start
		start := t.makeKey("")

		// iterate over all keys
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// parse positions
			var positions []uint64
			var err error
			err = iter.Item().Value(func(val []byte) error {
				positions, err = DecodeSequences(val)
				return err
			})
			if err != nil {
				return err
			}

			// set positions
			table[string(iter.Item().Key()[len(t.prefix):])] = positions
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return table, nil
}

// Delete will remove the specified positions from the table.
func (t *Table) Delete(name string) error {
	// acquire mutex
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// delete item
	err := retryUpdate(t.db, func(txn *badger.Txn) error {
		return txn.Delete(t.makeKey(name))
	})
	if err != nil {
		return err
	}

	// update cache if available
	if t.cache != nil {
		t.cache.Delete(name)
	}

	return nil
}

// Range will return the range of stored positions and whether there are any
// stored positions at all.
func (t *Table) Range() (uint64, uint64, bool, error) {
	// acquire mutex
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// prepare counter
	var min, max uint64

	// prepare flag
	var found bool

	// get position range from cache if available
	if t.cache != nil {
		// iterate through all entries
		t.cache.Range(func(key, value interface{}) bool {
			// coerce positions
			positions := value.([]uint64)

			// continue if empty
			if len(positions) == 0 {
				return true
			}

			// set min
			if min == 0 || positions[0] < min {
				min = positions[0]
			}

			// set max
			if max == 0 || positions[len(positions)-1] > max {
				max = positions[len(positions)-1]
			}

			// set flag
			found = true

			return true
		})

		return min, max, found, nil
	}

	// read positions range from database
	err := t.db.View(func(txn *badger.Txn) error {
		// prepare options
		opts := badger.DefaultIteratorOptions
		opts.Prefix = t.prefix

		// create iterator
		iter := txn.NewIterator(badger.IteratorOptions{
			Prefix:         t.prefix,
			PrefetchValues: true,
			PrefetchSize:   100,
		})
		defer iter.Close()

		// compute start
		start := t.makeKey("")

		// iterate over all keys
		for iter.Seek(start); iter.Valid(); iter.Next() {
			// parse positions
			var positions []uint64
			var err error
			err = iter.Item().Value(func(val []byte) error {
				positions, err = DecodeSequences(val)
				return err
			})
			if err != nil {
				return err
			}

			// continue if empty
			if len(positions) == 0 {
				continue
			}

			// set min
			if min == 0 || positions[0] < min {
				min = positions[0]
			}

			// set max
			if max == 0 || positions[len(positions)-1] > max {
				max = positions[len(positions)-1]
			}

			// set flag
			found = true
		}

		return nil
	})
	if err != nil {
		return 0, 0, false, err
	}

	return min, max, found, nil
}

// Clear will drop all stored positions. Clear will temporarily block concurrent
// writes and deletes and lock the underlying database. Other users uf the same
// database may receive errors due to the locked database.
func (t *Table) Clear() error {
	// acquire mutex
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// drop all entries
	err := t.db.DropPrefix(t.prefix)
	if err != nil {
		return err
	}

	// reset cache if available
	if t.cache != nil {
		t.cache = new(sync.Map)
	}

	return nil
}

func (t *Table) makeKey(name string) []byte {
	b := make([]byte, 0, len(t.prefix)+len(name))
	return append(append(b, t.prefix...), []byte(name)...)
}
