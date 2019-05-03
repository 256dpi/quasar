package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/256dpi/quasar"

	"github.com/montanaflynn/stats"
)

var send int64
var recv int64
var diffs []float64
var mutex sync.Mutex

const batch = 1000

func producer(ledger *quasar.Ledger) {
	// create producer
	producer := quasar.NewProducer(ledger, quasar.ProducerOptions{
		Batch:   batch,
		Timeout: time.Millisecond,
	})

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
		time.Sleep(5 * time.Microsecond)
	}
}

func consumer(ledger *quasar.Ledger, table *quasar.Table) {
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

func printer(ledger *quasar.Ledger) {
	// create ticker
	ticker := time.Tick(time.Second)

	for {
		// await signal
		<-ticker

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

func cleaner(ledger *quasar.Ledger) {
	for {
		// delete entries
		err := ledger.Delete(ledger.Head())
		if err != nil {
			panic(err)
		}

		// sleep some time
		time.Sleep(100 * time.Millisecond)
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

	// open ledger db
	ldb, err := quasar.OpenDB("./data/ledger")
	if err != nil {
		panic(err)
	}

	// open table db
	tdb, err := quasar.OpenDB("./data/table")
	if err != nil {
		panic(err)
	}

	// open ledger
	ledger, err := quasar.CreateLedger(ldb)
	if err != nil {
		panic(err)
	}

	// open table
	table, err := quasar.CreateTable(tdb)
	if err != nil {
		panic(err)
	}

	// run reader
	go producer(ledger)
	go consumer(ledger, table)
	go cleaner(ledger)
	go printer(ledger)

	select {}
}
