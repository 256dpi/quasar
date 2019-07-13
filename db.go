package quasar

import (
	"os"

	"github.com/petermattis/pebble"
)

var defaultWriteOptions = pebble.NoSync

// DB is a generic database.
type DB = pebble.DB

// OpenDB will open or create the specified db. A function is returned that must
// be called before closing the returned db to close the GC routine.
func OpenDB(directory string) (*DB, error) {
	// check directory
	if directory == "" {
		panic("quasar: missing directory")
	}

	// ensure directory
	err := os.MkdirAll(directory, 0777)
	if err != nil {
		return nil, err
	}

	// open db
	db, err := pebble.Open(directory, nil)
	if err != nil {
		return nil, err
	}

	return db, nil
}
