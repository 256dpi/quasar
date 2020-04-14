package quasar

import (
	"sync"

	"github.com/256dpi/turing"

	"github.com/256dpi/quasar/qis"
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
	machine *turing.Machine
	config  TableConfig
	prefix  []byte
	cache   map[string][]uint64
	mutex   sync.Mutex
}

// CreateTable will create a table that stores position markers.
func CreateTable(m *turing.Machine, config TableConfig) (*Table, error) {
	// check prefix
	if config.Prefix == "" {
		panic("quasar: missing prefix")
	}

	// create table
	t := &Table{
		machine: m,
		config:  config,
		prefix:  []byte(config.Prefix),
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
		// prepare list
		list := qis.List{
			Prefix: t.prefix,
		}

		// execute list
		err := t.machine.Execute(&list)
		if err != nil {
			return err
		}

		// cache positions
		for name, positions := range list.Positions {
			t.cache[name] = positions
		}
	}

	return nil
}

// Set will write the specified positions to the table.
func (t *Table) Set(name string, positions []uint64) error {
	// acquire mutex
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// prepare store
	store := qis.Store{
		Prefix:    t.prefix,
		Name:      []byte(name),
		Positions: positions,
	}

	// execute store
	err := t.machine.Execute(&store)
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
		value, _ := t.cache[name]
		return value, nil
	}

	// prepare load
	load := qis.Load{
		Prefix: t.prefix,
		Name:   []byte(name),
	}

	// execute load
	err := t.machine.Execute(&load)
	if err != nil {
		return nil, err
	}

	return load.Positions, nil
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

	// prepare list
	list := qis.List{
		Prefix: t.prefix,
	}

	// execute list
	err := t.machine.Execute(&list)
	if err != nil {
		return nil, err
	}

	return list.Positions, nil
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

	// prepare list
	list := qis.List{
		Prefix: t.prefix,
	}

	// execute list
	err := t.machine.Execute(&list)
	if err != nil {
		return 0, 0, false, err
	}

	// check positions
	for _, positions := range list.Positions {
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

func (t *Table) makeKey(name string) []byte {
	b := make([]byte, 0, len(t.prefix)+len(name))
	return append(append(b, t.prefix...), []byte(name)...)
}
