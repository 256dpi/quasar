package quasar

import (
	"fmt"
	"time"
)

type QueueConfig struct {
	Prefix string

	Skip      int
	Batch     int
	Window    int
	Cache     int
	Retention int
	Limit     int

	Timeout  time.Duration
	Retry    int
	Delay    time.Duration
	Interval time.Duration

	Errors chan<- error
}

func DynamicConfig(rate int, loss time.Duration) QueueConfig {
	return QueueConfig{
		Skip:      100,
		Batch:     1000,   // * 10
		Window:    5000,   // * 5
		Cache:     10000,  // * 2
		Retention: 10000,  // * 1
		Limit:     100000, // * 10

		Timeout:  10 * time.Millisecond,
		Retry:    1000,
		Delay:    10 * time.Millisecond,
		Interval: time.Second,
	}
}

type Queue struct {
	config   QueueConfig
	ledger   *Ledger
	table    *Table
	matrix   *Matrix
	producer *Producer
	cleaner  *Cleaner
}

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

	// create matrix
	matrix, err := CreateMatrix(db, MatrixConfig{
		Prefix: fmt.Sprintf("%s:m", config.Prefix),
	})
	if err != nil {
		return nil, err
	}

	// create producer
	producer := NewProducer(ledger, ProducerConfig{
		Batch:   config.Batch,
		Timeout: config.Timeout,
		Retry:   config.Retry,
		Delay:   config.Delay,
	})

	// create cleaner
	cleaner := NewCleaner(ledger, CleanerConfig{
		Retention: config.Retention,
		Threshold: 0, // disable
		Interval:  config.Interval,
		Tables:    []*Table{table},
		Matrices:  []*Matrix{matrix},
		Errors:    config.Errors,
	})

	return &Queue{
		config:   config,
		ledger:   ledger,
		table:    table,
		matrix:   matrix,
		producer: producer,
		cleaner:  cleaner,
	}, nil
}

func (q *Queue) Write(entry Entry, ack func(error)) bool {
	return q.producer.Write(entry, ack)
}

func (q *Queue) Reader(start uint64, entries chan<- Entry, errors chan<- error) *Reader {
	return NewReader(q.ledger, ReaderConfig{
		Start:   start,
		Entries: entries,
		Errors:  errors,
		Batch:   q.config.Batch,
	})
}

func (q *Queue) Consumer(name string, entries chan<- Entry, errors chan<- error) *Consumer {
	return NewConsumer(q.ledger, q.table, ConsumerConfig{
		Name:    name,
		Entries: entries,
		Errors:  errors,
		Batch:   q.config.Batch,
		Skip:    q.config.Skip,
	})
}

func (q *Queue) Worker(name string, entries chan<- Entry, errors chan<- error) *Worker {
	return NewWorker(q.ledger, q.matrix, WorkerConfig{
		Name:    name,
		Entries: entries,
		Errors:  errors,
		Batch:   q.config.Batch,
		Window:  q.config.Window,
		Skip:    q.config.Skip,
	})
}

func (q *Queue) Close() {
	// close cleaner
	q.cleaner.Close()
}
