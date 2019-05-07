package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/256dpi/quasar"

	"github.com/montanaflynn/stats"
)

var wg sync.WaitGroup

var send int64
var recv int64
var diffs []float64
var mutex sync.Mutex

const batch = 1000

func producer(ledger *quasar.Ledger, done <-chan struct{}) {
	defer wg.Done()

	// create producer
	producer := quasar.NewProducer(ledger, quasar.ProducerOptions{
		Batch:   batch,
		Timeout: time.Millisecond,
	})

	// ensure closing
	defer producer.Close()

	// prepare sequence
	var sequence uint64

	// write entries forever
	for {
		// increment sequence
		sequence++

		// write entry
		producer.Write(quasar.Entry{
			Sequence: sequence,
			Payload:  []byte(time.Now().Format(time.RFC3339Nano)),
		}, nil)

		// increment
		mutex.Lock()
		send += 1
		mutex.Unlock()

		// limit rate
		select {
		case <-time.After(5 * time.Microsecond):
		case <-done:
			return
		}
	}
}

func consumer(ledger *quasar.Ledger, table *quasar.Table, done <-chan struct{}) {
	defer wg.Done()

	// prepare channels
	entries := make(chan quasar.Entry, batch)
	errors := make(chan error, 1)

	// create reader
	consumer := quasar.NewConsumer(ledger, table, quasar.ConsumerOptions{
		Name:    "example",
		Batch:   batch,
		Entries: entries,
		Errors:  errors,
	})

	// ensure closing
	defer consumer.Close()

	// prepare counter
	counter := 0

	for {
		// prepare entry
		var entry quasar.Entry

		// receive entry or error
		select {
		case entry = <-entries:
		case err := <-errors:
			panic(err)
		case <-done:
			return
		}

		// get timestamp
		ts, _ := time.Parse(time.RFC3339Nano, string(entry.Payload))

		// calculate diff
		diff := float64(time.Since(ts)) / float64(time.Millisecond)

		// increment and save diff
		mutex.Lock()
		recv++
		diffs = append(diffs, diff)
		mutex.Unlock()

		// increment counter
		counter++

		// check counter
		if counter > batch {
			// acknowledge
			err := consumer.Ack(entry.Sequence)
			if err != nil {
				panic(err)
			}

			// reset counter
			counter = 0
		}
	}
}

func printer(ledger *quasar.Ledger, table *quasar.Table, done <-chan struct{}) {
	defer wg.Done()

	// create ticker
	ticker := time.Tick(time.Second)

	for {
		// await signal
		select {
		case <-ticker:
		case <-done:
			return
		}

		// get data
		mutex.Lock()
		r := recv
		s := send
		d := diffs
		recv = 0
		send = 0
		diffs = nil
		mutex.Unlock()

		// get stats
		min, _ := stats.Min(d)
		max, _ := stats.Max(d)
		mean, _ := stats.Mean(d)
		p90, _ := stats.Percentile(d, 90)
		p95, _ := stats.Percentile(d, 95)
		p99, _ := stats.Percentile(d, 99)

		// get tail
		tail, _, err := table.Range()
		if err != nil {
			panic(err)
		}

		// print rate
		fmt.Printf("send: %d msg/s, ", s)
		fmt.Printf("recv %d msgs/s, ", r)
		fmt.Printf("min: %.2fms, ", min)
		fmt.Printf("mean: %.2fms, ", mean)
		fmt.Printf("p90: %.2fms, ", p90)
		fmt.Printf("p95: %.2fms, ", p95)
		fmt.Printf("p99: %.2fms, ", p99)
		fmt.Printf("max: %.2fms, ", max)
		fmt.Printf("length: %d, ", ledger.Length())
		fmt.Printf("diff: %d\n", ledger.Head()-tail)
	}
}

func cleaner(ledger *quasar.Ledger, table *quasar.Table, done <-chan struct{}) {
	defer wg.Done()

	// prepare channels
	errors := make(chan error, 1)

	// create cleaner
	cleaner := quasar.NewCleaner(ledger, quasar.CleanerOptions{
		MinRetention: 10000,
		MaxRetention: 100000,
		Tables:       []*quasar.Table{table},
		Delay:        100 * time.Millisecond,
		Errors:       errors,
	})

	// ensure closing
	defer cleaner.Close()

	// wait for close
	select {
	case err := <-errors:
		panic(err)
	case <-done:
		return
	}
}

func main() {
	// get dir
	dir, err := filepath.Abs("./data")
	if err != nil {
		panic(err)
	}

	// remove dir
	err = os.RemoveAll(dir)
	if err != nil {
		panic(err)
	}

	// open db
	db, err := quasar.OpenDB(dir, quasar.DBOptions{GCInterval: 10 * time.Second})
	if err != nil {
		panic(err)
	}

	// open ledger
	ledger, err := quasar.CreateLedger(db, quasar.LedgerOptions{
		Prefix: "ledger",
	})
	if err != nil {
		panic(err)
	}

	// open table
	table, err := quasar.CreateTable(db, quasar.TableOptions{
		Prefix: "table",
	})
	if err != nil {
		panic(err)
	}

	// create control channel
	done := make(chan struct{})

	// run routines
	wg.Add(4)
	go producer(ledger, done)
	go consumer(ledger, table, done)
	go cleaner(ledger, table, done)
	go printer(ledger, table, done)

	// prepare exit
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)
	<-exit

	// close control channel
	close(done)
	wg.Wait()

	// close db
	err = db.Close()
	if err != nil {
		panic(err)
	}
}
