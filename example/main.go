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
	// create producer
	producer := quasar.NewProducer(ledger, quasar.ProducerOptions{
		Batch:   batch,
		Timeout: time.Millisecond,
	})

	// ensure closing
	defer producer.Close()

	// write entries forever
	for {
		// write entry
		producer.Write(quasar.Entry{
			Sequence: quasar.GenerateSequence(1),
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
			wg.Done()
			return
		}
	}
}

func consumer(ledger *quasar.Ledger, table *quasar.Table, done <-chan struct{}) {
	// prepare channels
	entries := make(chan quasar.Entry, batch)
	errors := make(chan error, 1)

	// create reader
	consumer := quasar.NewConsumer(ledger, table, quasar.ConsumerOptions{
		Name:    "example",
		Entries: entries,
		Errors:  errors,
		Batch:   batch,
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
			wg.Done()
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

func printer(ledger *quasar.Ledger, done <-chan struct{}) {
	// create ticker
	ticker := time.Tick(time.Second)

	for {
		// await signal
		select {
		case <-ticker:
		case <-done:
			wg.Done()
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

		// print rate
		fmt.Printf("send: %d msg/s, ", s)
		fmt.Printf("recv %d msgs/s, ", r)
		fmt.Printf("min: %.2fms, ", min)
		fmt.Printf("mean: %.2fms, ", mean)
		fmt.Printf("p90: %.2fms, ", p90)
		fmt.Printf("p95: %.2fms, ", p95)
		fmt.Printf("p99: %.2fms, ", p99)
		fmt.Printf("max: %.2fms, ", max)
		fmt.Printf("size: %d\n", ledger.Length())
	}
}

func cleaner(ledger *quasar.Ledger, table *quasar.Table, done <-chan struct{}) {
	for {
		// sleep some time
		select {
		case <-time.After(100 * time.Millisecond):
		case <-done:
			wg.Done()
			return
		}

		// get range
		min, _, err := table.Range()
		if err != nil {
			panic(err)
		}

		// delete entries
		err = ledger.Delete(min)
		if err != nil {
			panic(err)
		}
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
	db, err := quasar.OpenDB(dir)
	if err != nil {
		panic(err)
	}

	// open ledger
	ledger, err := quasar.CreateLedger(db, "ledger")
	if err != nil {
		panic(err)
	}

	// open table
	table, err := quasar.CreateTable(db, "table")
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
	go printer(ledger, done)

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
