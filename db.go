package quasar

import (
	"os"
	"time"

	"github.com/dgraph-io/badger/v2"
)

// DB is a generic database.
type DB = badger.DB

// DBConfig are used to configure a DB.
type DBConfig struct {
	// The interval of the garbage collector.
	GCInterval time.Duration

	// The channel on which errors are sent.
	Errors chan<- error
}

// OpenDB will open or create the specified db.
func OpenDB(dir string, config DBConfig) (*DB, error) {
	// ensure directory
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return nil, err
	}

	// prepare options
	bo := badger.DefaultOptions
	bo.Dir = dir
	bo.ValueDir = dir
	bo.Logger = nil

	// open db
	db, err := badger.Open(bo)
	if err != nil {
		return nil, err
	}

	// run gc routine if requested
	if config.GCInterval > 0 {
		go func() {
			for {
				// sleep some time
				time.Sleep(config.GCInterval)

				// run gc
				err = db.RunValueLogGC(0.5)
				if err == badger.ErrRejected {
					return
				} else if err != nil && err != badger.ErrNoRewrite {
					select {
					case config.Errors <- err:
					default:
					}
				}
			}
		}()
	}

	return db, nil
}
