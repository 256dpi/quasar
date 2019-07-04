package quasar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	db := openDB(true)

	queue, err := CreateQueue(db, QueueConfig{
		Prefix:    "queue",
		Cache:     10,
		Retention: 20,
		Limit:     30,
		Interval:  5 * time.Millisecond,
		Errors: func(err error) {
			panic(err)
		},
	})
	assert.NoError(t, err)

	entries := make(chan Entry, 1)

	consumer := queue.Consumer(ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch:  10,
		Window: 10,
	})

	time.Sleep(50 * time.Millisecond)

	producer := queue.Producer(ProducerConfig{
		Batch:   5,
		Timeout: time.Millisecond,
		Retry:   100,
		Delay:   10 * time.Millisecond,
	})

	done := make(chan struct{})

	go func() {
		for i := 1; i <= 50; i++ {
			ok := producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, func(err error) {
				if err != nil {
					panic(err)
				}
			})
			assert.True(t, ok)
		}

		close(done)
	}()

	i := 1
	for {
		entry := <-entries
		assert.Equal(t, uint64(i), entry.Sequence)

		consumer.Mark(entry.Sequence, true, func(err error) {
			if err != nil {
				panic(err)
			}
		})

		if i == 25 {
			break
		}

		i++
	}

	<-done
	time.Sleep(50 * time.Millisecond)

	producer.Close()
	consumer.Close()

	time.Sleep(50 * time.Millisecond)

	length := queue.Ledger().Length()
	assert.Equal(t, 25, length)

	head := queue.Ledger().Head()
	assert.Equal(t, 50, int(head))

	low, high, _, err := queue.Table().Range()
	assert.NoError(t, err)
	assert.Equal(t, 25, int(low))
	assert.Equal(t, 25, int(high))

	queue.Close()
}
