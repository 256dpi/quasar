package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/256dpi/god"
	"github.com/montanaflynn/stats"

	"github.com/256dpi/quasar"
)

var wg sync.WaitGroup

var send int64
var recv int64
var diffs []float64
var mutex sync.Mutex

func producer(queue *quasar.Queue) {
	// create producer
	producer := queue.Producer(quasar.ProducerConfig{
		Batch:   2500,
		Timeout: time.Millisecond,
		Retry:   100, // 10s
		Delay:   100 * time.Millisecond,
	})

	// ensure closing
	defer producer.Close()

	// write entries forever
	for {
		// write entry
		producer.Write(quasar.Entry{
			Sequence: quasar.GenerateSequence(1),
			Payload:  []byte(time.Now().Format(time.RFC3339Nano)),
		}, func(err error) {
			if err != nil {
				panic(err)
			}
		})

		// increment
		mutex.Lock()
		send += 1
		mutex.Unlock()
	}
}

func consumer(queue *quasar.Queue) {
	// prepare channels
	entries := make(chan quasar.Entry, 5000)

	// create consumer
	consumer := queue.Consumer(quasar.ConsumerConfig{
		Name:    "example",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch:    2500,
		Window:   5000,
		Skip:     2500,
		Timeout:  time.Second,
		Deadline: 10 * time.Second,
	})

	// ensure closing
	defer consumer.Close()

	for {
		// receive entry
		entry := <-entries

		// get timestamp
		ts, _ := time.Parse(time.RFC3339Nano, string(entry.Payload))

		// calculate diff
		diff := float64(time.Since(ts)) / float64(time.Millisecond)

		// increment and save diff
		mutex.Lock()
		recv++
		diffs = append(diffs, diff)
		mutex.Unlock()

		// mark sequence
		consumer.Mark(entry.Sequence, false, func(err error) {
			if err != nil {
				panic(err)
			}
		})
	}
}

func printer(queue *quasar.Queue) {
	defer wg.Done()

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
		fmt.Printf("length: %d\n", queue.Ledger().Length())
	}
}

func main() {
	// debug
	god.Debug()

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
	defer db.Close()

	// create queue
	queue, err := quasar.CreateQueue(db, quasar.QueueConfig{
		Prefix:        "queue",
		LedgerCache:   500000,
		LedgerLimit:   500000,
		TableCache:    true,
		CleanInterval: 500 * time.Millisecond,
		CleanErrors: func(err error) {
			panic(err)
		},
	})
	if err != nil {
		panic(err)
	}

	// run routines
	wg.Add(3)
	go producer(queue)
	go consumer(queue)
	go printer(queue)

	// prepare exit
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)
	<-exit

	// just exit
	os.Exit(0)
}
