package quasar

import (
	"os"
	"time"

	"github.com/dgraph-io/badger"
	"gopkg.in/tomb.v2"
)

// Level specifies a log level.
type Level int

// The available log levels.
const (
	Error Level = iota
	Warning
	Info
	Debug
)

// DB is a generic database.
type DB = badger.DB

// DBConfig is used to configure a DB.
type DBConfig struct {
	// The interval of the garbage collector.
	GCInterval time.Duration

	// The callback that receives garbage collector errors.
	GCErrors func(error)

	// The sink used for logging.
	Logger func(level Level, format string, args ...interface{})
}

// OpenDB will open or create the specified db. A function is returned that must
// be called before closing the returned db to close the GC routine.
func OpenDB(directory string, config DBConfig) (*DB, func(), error) {
	// check directory
	if directory == "" {
		panic("quasar: missing directory")
	}

	// ensure directory
	err := os.MkdirAll(directory, 0777)
	if err != nil {
		return nil, nil, err
	}

	// prepare options
	bo := badger.DefaultOptions(directory).
		WithLogger(nil)

	// set logger if available
	if config.Logger != nil {
		bo.Logger = createLogger(config.Logger)
	}

	// open db
	db, err := badger.Open(bo)
	if err != nil {
		return nil, nil, err
	}

	// prepare tomb
	var tmb tomb.Tomb

	// run gc routine if requested
	if config.GCInterval > 0 {
		tmb.Go(func() error {
			for {
				// sleep some time
				select {
				case <-time.After(config.GCInterval):
				case <-tmb.Dying():
					return tomb.ErrDying
				}

				// run gc
				err = db.RunValueLogGC(0.75)
				if err == badger.ErrRejected {
					continue
				} else if err != nil && err != badger.ErrNoRewrite && config.GCErrors != nil {
					config.GCErrors(err)
				}
			}
		})
	}

	// create closer
	closer := func() {
		tmb.Kill(nil)
		_ = tmb.Wait()
	}

	return db, closer, nil
}

type logger struct {
	sink func(Level, string, ...interface{})
}

func createLogger(sink func(Level, string, ...interface{})) *logger {
	return &logger{
		sink: sink,
	}
}

func (l *logger) Errorf(f string, v ...interface{}) {
	l.sink(Error, f, v...)
}

func (l *logger) Warningf(f string, v ...interface{}) {
	l.sink(Warning, f, v...)
}

func (l *logger) Infof(f string, v ...interface{}) {
	l.sink(Info, f, v...)
}

func (l *logger) Debugf(f string, v ...interface{}) {
	l.sink(Debug, f, v...)
}
