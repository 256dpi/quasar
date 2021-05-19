package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/256dpi/god"
	"github.com/256dpi/turing"
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
		Batch:   10_000 - 1,
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
			Payload: time2Bin(time.Now()),
		}, func(err error) {
			if err != nil {
				panic(err)
			}
		})

		// increment
		mutex.Lock()
		send++
		mutex.Unlock()
	}
}

func consumer(queue *quasar.Queue) {
	// prepare channels
	entries := make(chan quasar.Entry, 25_000)

	// create consumer
	consumer := queue.Consumer(quasar.ConsumerConfig{
		Name:    "example",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch:    25_000,
		Window:   50_000,
		Skip:     25_000,
		Timeout:  time.Second,
		Deadline: 10 * time.Second,
	})

	// ensure closing
	defer consumer.Close()

	// receive entries
	for entry := range entries {
		// get timestamp
		ts := bin2Time(entry.Payload)

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
	god.Init(god.Options{})

	// disable logging
	turing.SetLogger(nil)

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

	// start machine
	machine, err := turing.Start(turing.Config{
		Standalone:   true,
		Directory:    dir,
		Instructions: quasar.Instructions,
	})
	if err != nil {
		panic(err)
	}

	// create queue
	queue, err := quasar.CreateQueue(machine, quasar.QueueConfig{
		Prefix:        "queue",
		LedgerCache:   500_000,
		LedgerLimit:   500_000,
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

func time2Bin(t time.Time) []byte {
	u := uint64(t.UnixNano())
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b
}

func bin2Time(b []byte) time.Time {
	i := int64(binary.BigEndian.Uint64(b))
	return time.Unix(0, i)
}
