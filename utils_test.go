package quasar

import (
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v2"
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
	db, err := OpenDB(dir, DBConfig{GCInterval: time.Minute})
	if err != nil {
		panic(err)
	}

	return db
}

func set(db *DB, key, value string) {
	// set entry
	err := db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(&badger.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		})
	})
	if err != nil {
		panic(err)
	}
}

func dump(db *DB) map[string]string {
	// prepare map
	m := map[string]string{}

	// iterate over all keys
	err := db.View(func(txn *badger.Txn) error {
		// create iterator
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		// iterate over all keys
		for iter.Rewind(); iter.Valid(); iter.Next() {
			err := iter.Item().Value(func(val []byte) error {
				m[string(iter.Item().Key())] = string(val)
				return nil
			})
			if err != nil {
				panic(err)
			}
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	return m
}
