package quasar

import (
	"io"
	"log"
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

	// The channel on which garbage collector errors are sent.
	GCErrors chan<- error

	// The sink used for logging.
	LogSink io.Writer
}

// OpenDB will open or create the specified db.
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

	// prepare options
	bo := badger.DefaultOptions
	bo.Dir = directory
	bo.ValueDir = directory
	bo.Logger = nil

	// set logger if available
	if config.LogSink != nil {
		bo.Logger = createLogger(config.LogSink)
	}

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
				err = db.RunValueLogGC(0.75)
				if err == badger.ErrRejected {
					return
				} else if err != nil && err != badger.ErrNoRewrite && config.GCErrors != nil {
					select {
					case config.GCErrors <- err:
					default:
					}
				}
			}
		}()
	}

	return db, nil
}

type logger struct {
	*log.Logger
}

func createLogger(sink io.Writer) *logger {
	return &logger{
		Logger: log.New(sink, "quasar ", log.LstdFlags),
	}
}

func (l *logger) Errorf(f string, v ...interface{}) {
	l.Printf("ERROR: "+f, v...)
}

func (l *logger) Warningf(f string, v ...interface{}) {
	l.Printf("WARNING: "+f, v...)
}

func (l *logger) Infof(f string, v ...interface{}) {
	l.Printf("INFO: "+f, v...)
}

func (l *logger) Debugf(f string, v ...interface{}) {
	l.Printf("DEBUG: "+f, v...)
}
