package quasar

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

func openDB(clear bool) *DB {
	// make dir absolute
	dir, err := filepath.Abs(filepath.Join("test"))
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
	db, err := OpenDB(dir, DBConfig{})
	if err != nil {
		panic(err)
	}

	return db
}

func closeDB(db *DB) {
	err := db.Close()
	if err != nil {
		panic(err)
	}
}

func set(db *DB, key, value string) {
	// set entry
	err := db.Set([]byte(key), []byte(value), pebble.Sync)
	if err != nil {
		panic(err)
	}
}

func dump(db *DB) map[string]string {
	// prepare map
	data := map[string]string{}

	// create iterator
	iter := db.NewIter(&pebble.IterOptions{})
	defer iter.Close()

	// read all keys
	for iter.SeekGE(nil); iter.Valid(); iter.Next() {
		data[string(iter.Key())] = string(iter.Value())
	}

	// check errors
	err := iter.Error()
	if err != nil {
		panic(err)
	}

	return data
}
