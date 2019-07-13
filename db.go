package quasar

import (
	"os"

	"github.com/petermattis/pebble"
	"github.com/petermattis/pebble/cache"
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
	db, err := pebble.Open(directory, &pebble.Options{
		Cache:                       cache.New(128 << 20), // 128MB
		MemTableSize:                64 << 20,             // 64MB
		MemTableStopWritesThreshold: 4,
		MinFlushRate:                4 << 20, // 4MB
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       32,
		LBaseMaxBytes:               64 << 20, // 64MB
		Levels: []pebble.LevelOptions{{
			BlockSize: 32 << 10, // 32KB
		}},
		EventListener: pebble.MakeLoggingEventListener(nil),
	})
	if err != nil {
		return nil, err
	}

	return db, nil
}
