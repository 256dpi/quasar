package quasar

import (
	"os"
	"path/filepath"
)

func openDB(name string, clear bool) *DB {
	// make dir absolute
	dir, err := filepath.Abs(filepath.Join("test", name))
	if err != nil {
		panic(err)
	}

	// clear directory
	if clear {
		err = os.RemoveAll(dir)
		if err != nil {
			panic(err)
		}
	}

	// open db
	db, err := OpenDB(dir)
	if err != nil {
		panic(err)
	}

	return db
}
