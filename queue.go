package quasar

import (
	"fmt"
	"time"
)

// QueueConfig is used to configure a queue.
type QueueConfig struct {
	// The prefix for all keys.
	Prefix string

	// The amount of entries to cache in memory.
	Cache int

	// The amount of entries to keep around.
	Retention int

	// The maximum length of the ledger.
	Limit int

	// The point after which entries are dropped no matter what.
	Threshold int

	// The interval of periodic cleanings.
	Interval time.Duration

	// The channel on which cleaning errors are sent.
	Errors chan<- error
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
		Cache:  config.Cache,
		Limit:  config.Limit,
	})
	if err != nil {
		return nil, err
	}

	// create table
	table, err := CreateTable(db, TableConfig{
		Prefix: fmt.Sprintf("%s:t", config.Prefix),
	})
	if err != nil {
		return nil, err
	}

	// create cleaner
	cleaner := NewCleaner(ledger, CleanerConfig{
		Retention: config.Retention,
		Threshold: config.Threshold,
		Interval:  config.Interval,
		Tables:    []*Table{table},
		Errors:    config.Errors,
	})

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
	q.cleaner.Close()
}
