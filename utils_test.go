package quasar

import (
	"os"
	"path/filepath"

	"github.com/tecbot/gorocksdb"
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
	db, err := OpenDB(dir)
	if err != nil {
		panic(err)
	}

	return db
}

func set(db *DB, key, value string) {
	// prepare opts
	opts := gorocksdb.NewDefaultWriteOptions()
	defer opts.Destroy()

	// set entry
	err := db.Put(opts, []byte(key), []byte(value))
	if err != nil {
		panic(err)
	}
}

func dump(db *DB) map[string]string {
	// prepare map
	data := map[string]string{}

	// prepare opts
	opts := gorocksdb.NewDefaultReadOptions()
	defer opts.Destroy()

	// iterate over all keys
	iter := db.NewIterator(opts)
	defer iter.Close()

	// read all keys
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		data[string(iter.Key().Data())] = string(iter.Value().Data())
	}

	// check errors
	err := iter.Err()
	if err != nil {
		panic(err)
	}

	return data
}
