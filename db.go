package quasar

import (
	"os"

	"github.com/cockroachdb/pebble"
)

// DBConfig is used to configure a DB.
type DBConfig struct {
	// Whether all writes should be synced.
	SyncWrites bool

	// The sink used for logging.
	Logger func(format string, args ...interface{})
}

// DB is a generic database.
type DB struct {
	*pebble.DB
	ch *pebble.Cache
	wo *pebble.WriteOptions
}

// Close will close the db.
func (db *DB) Close() error {
	// unref cache
	db.ch.Unref()

	// close db
	return db.DB.Close()
}

// OpenDB will open or create the specified db. A function is returned that must
// be called before closing the returned db to close the GC routine.
func OpenDB(directory string, config DBConfig) (*DB, error) {
	// check directory
	if directory == "" {
		panic("quasar: missing directory")
	}

	// ensure directory
	err := os.MkdirAll(directory, 0777)
	if err != nil {
		return nil, err
	}

	// prepare logger
	logger := &logger{fn: config.Logger}

	// prepare cache
	cache := pebble.NewCache(64 << 20)

	// open db
	pdb, err := pebble.Open(directory, &pebble.Options{
		Cache:                       cache,
		MemTableSize:                16 << 20,
		MemTableStopWritesThreshold: 4,
		MinFlushRate:                4 << 20,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       16,
		LBaseMaxBytes:               16 << 20,
		Levels: []pebble.LevelOptions{{
			BlockSize: 32 << 10, // 32KB
		}},
		Logger:        logger,
		EventListener: pebble.MakeLoggingEventListener(logger),
	})
	if err != nil {
		return nil, err
	}

	// create db
	db := &DB{
		DB: pdb,
		ch: cache,
		wo: &pebble.WriteOptions{Sync: config.SyncWrites},
	}

	return db, nil
}

type logger struct {
	fn func(string, ...interface{})
}

func (l *logger) Infof(format string, args ...interface{}) {
	if l.fn != nil {
		l.fn(format, args...)
	}
}

func (l *logger) Fatalf(format string, args ...interface{}) {
	if l.fn != nil {
		l.fn(format, args...)
	}
}
