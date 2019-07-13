package quasar

import (
	"os"

	"github.com/tecbot/gorocksdb"
)

// DB is a generic database.
type DB = gorocksdb.DB

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

	// prepare options
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetUseFsync(true)
	opts.SetBytesPerSync(1048576)
	opts.SetMaxBackgroundCompactions(4)
	opts.SetMaxBackgroundFlushes(2)

	// use block based tables
	bbt := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbt.SetBlockSize(16 * 1024)
	bbt.SetCacheIndexAndFilterBlocks(true)
	bbt.SetBlockCache(gorocksdb.NewLRUCache(128 << 20))
	bbt.SetPinL0FilterAndIndexBlocksInCache(true)
	opts.SetBlockBasedTableFactory(bbt)

	// open db
	db, err := gorocksdb.OpenDb(opts, directory)
	if err != nil {
		return nil, err
	}

	return db, nil
}
