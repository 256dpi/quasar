package quasar

import (
	"fmt"
	"time"
)

// QueueConfig is used to configure a queue.
type QueueConfig struct {
	// The prefix for all keys.
	Prefix string

	// The amount of ledger entries to cache in memory.
	LedgerCache int

	// The maximum length of the ledger.
	LedgerLimit int

	// Whether the table should be fully cached in memory.
	TableCache bool

	// The amount of entries to keep around.
	CleanRetention int

	// The point after which entries are dropped no matter what.
	CleanThreshold int

	// The interval of periodic cleanings.
	CleanInterval time.Duration

	// The callback used to yield cleaner errors.
	CleanErrors func(error)
}

// Queue is a managed ledger and table with a cleaner.
type Queue struct {
	config  QueueConfig
	ledger  *Ledger
	table   *Table
	cleaner *Cleaner
}

// CreateQueue will create and return a new queue based on the provided settings.
func CreateQueue(db *DB, config QueueConfig) (*Queue, error) {
	// create ledger
	ledger, err := CreateLedger(db, LedgerConfig{
		Prefix: fmt.Sprintf("%s:l", config.Prefix),
		Cache:  config.LedgerCache,
		Limit:  config.LedgerLimit,
	})
	if err != nil {
		return nil, err
	}

	// create table
	table, err := CreateTable(db, TableConfig{
		Prefix: fmt.Sprintf("%s:t", config.Prefix),
		Cache:  config.TableCache,
	})
	if err != nil {
		return nil, err
	}

	// prepare cleaner
	var cleaner *Cleaner

	// create cleaner if interval is set
	if config.CleanInterval > 0 {
		cleaner = NewCleaner(ledger, CleanerConfig{
			Retention: config.CleanRetention,
			Threshold: config.CleanThreshold,
			Interval:  config.CleanInterval,
			Tables:    []*Table{table},
			Errors:    config.CleanErrors,
		})
	}

	return &Queue{
		config:  config,
		ledger:  ledger,
		table:   table,
		cleaner: cleaner,
	}, nil
}

// Ledger will return the queues ledger.
func (q *Queue) Ledger() *Ledger {
	return q.ledger
}

// Table will return the queues table.
func (q *Queue) Table() *Table {
	return q.table
}

// Producer will create a new producer with the provided config.
func (q *Queue) Producer(config ProducerConfig) *Producer {
	return NewProducer(q.ledger, config)
}

// Consumer will create a new consumer with the provided config.
func (q *Queue) Consumer(config ConsumerConfig) *Consumer {
	return NewConsumer(q.ledger, q.table, config)
}

// Close will close the queue.
func (q *Queue) Close() {
	if q.cleaner != nil {
		q.cleaner.Close()
	}
}
