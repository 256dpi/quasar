package quasar

import (
	"sync"
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
	cache  map[string][]uint64
	mutex  sync.Mutex
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
		t.cache = make(map[string][]uint64)
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
		// iterate over all keys
		iter := t.db.NewIterator(defaultReadOptions)
		defer iter.Close()

		// compute start
		start := t.makeKey("")

		// read all keys
		for iter.Seek(start); iter.ValidForPrefix(t.prefix); iter.Next() {
			// parse positions
			positions, err := DecodeSequences(iter.Value().Data())
			if err != nil {
				return err
			}

			// continue if empty
			if len(positions) == 0 {
				continue
			}

			// cache positions
			t.cache[string(iter.Key().Data()[len(t.prefix):])] = positions
		}

		// check errors
		err := iter.Err()
		if err != nil {
			panic(err)
		}
	}

	return nil
}

// Set will write the specified positions to the table.
func (t *Table) Set(name string, positions []uint64) error {
	// acquire mutex
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// ignore empty positions
	if len(positions) == 0 {
		return nil
	}

	// set entry
	err := t.db.Put(defaultWriteOptions, t.makeKey(name), EncodeSequences(positions))
	if err != nil {
		return err
	}

	// update cache if available
	if t.cache != nil {
		t.cache[name] = positions
	}

	return nil
}

// Get will read the specified positions from the table.
func (t *Table) Get(name string) ([]uint64, error) {
	// acquire mutex
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// get positions from cache if available
	if t.cache != nil {
		value, ok := t.cache[name]
		if !ok {
			return nil, nil
		}

		return value, nil
	}

	// read positions
	item, err := t.db.Get(defaultReadOptions, t.makeKey(name))
	if err != nil {
		return nil, err
	}

	// parse key
	positions, err := DecodeSequences(item.Data())
	if err != nil {
		return nil, err
	}

	return positions, nil
}

// All will return a map with all stored positions.
func (t *Table) All() (map[string][]uint64, error) {
	// acquire mutex
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// prepare table
	table := make(map[string][]uint64)

	// get positions from cache if available
	if t.cache != nil {
		for name, positions := range t.cache {
			table[name] = positions
		}

		return table, nil
	}

	// iterate over all keys
	iter := t.db.NewIterator(defaultReadOptions)
	defer iter.Close()

	// compute start
	start := t.makeKey("")

	// read all keys
	for iter.Seek(start); iter.ValidForPrefix(t.prefix); iter.Next() {
		// parse positions
		positions, err := DecodeSequences(iter.Value().Data())
		if err != nil {
			return nil, err
		}

		// set positions
		table[string(iter.Key().Data()[len(t.prefix):])] = positions
	}

	// check errors
	err := iter.Err()
	if err != nil {
		panic(err)
	}

	return table, nil
}

// Delete will remove the specified positions from the table.
func (t *Table) Delete(name string) error {
	// acquire mutex
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// delete item
	err := t.db.Delete(defaultWriteOptions, t.makeKey(name))
	if err != nil {
		return err
	}

	// update cache if available
	if t.cache != nil {
		delete(t.cache, name)
	}

	return nil
}

// Range will return the range of stored positions and whether there are any
// stored positions at all.
func (t *Table) Range() (uint64, uint64, bool, error) {
	// acquire mutex
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// prepare counter
	var min, max uint64

	// prepare flag
	var found bool

	// get position range from cache if available
	if t.cache != nil {
		// iterate through all entries
		for _, positions := range t.cache {
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

		return min, max, found, nil
	}

	// iterate over all keys
	iter := t.db.NewIterator(defaultReadOptions)
	defer iter.Close()

	// compute start
	start := t.makeKey("")

	// read all keys
	for iter.Seek(start); iter.ValidForPrefix(t.prefix); iter.Next() {
		// parse positions
		positions, err := DecodeSequences(iter.Value().Data())
		if err != nil {
			return 0, 0, false, err
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

	// check errors
	err := iter.Err()
	if err != nil {
		panic(err)
	}

	return min, max, found, nil
}

func (t *Table) makeKey(name string) []byte {
	b := make([]byte, 0, len(t.prefix)+len(name))
	return append(append(b, t.prefix...), []byte(name)...)
}
