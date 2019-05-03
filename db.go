package quasar

import (
	"os"

	"github.com/dgraph-io/badger"
)

// DB is a generic database.
type DB = badger.DB

// OpenDB will open or create the specified db.
func OpenDB(dir string) (*DB, error) {
	// ensure directory
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return nil, err
	}

	// prepare options
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	opts.Logger = nil

	// open db
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return db, nil
}
