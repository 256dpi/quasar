package quasar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProducer(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	done := make(chan struct{})

	producer := NewProducer(ledger, ProducerConfig{
		BatchSize:    10,
		BatchTimeout: time.Millisecond,
	})

	for i := 1; i <= 20; i++ {
		if i == 20 {
			producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, func(err error) {
				assert.NoError(t, err)
				close(done)
			})
		} else {
			producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, nil)
		}
	}

	<-done

	producer.Close()

	length := ledger.Length()
	assert.Equal(t, 20, length)

	err = db.Close()
	assert.NoError(t, err)
}

func TestProducerRetry(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger", Limit: 10})
	assert.NoError(t, err)

	producer := NewProducer(ledger, ProducerConfig{
		BatchSize:     3,
		BatchTimeout:  time.Millisecond,
		RetryTimeout:  time.Second,
		RetryInterval: time.Millisecond,
	})

	done := make(chan struct{})
	errors := make(chan error, 21)

	go func() {
		for {
			select {
			default:
				_ = ledger.Delete(20)
				time.Sleep(10 * time.Millisecond)
			case <-done:
				return
			}
		}
	}()

	for i := 1; i <= 20; i++ {
		producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, func(err error) {
			errors <- err

			if len(errors) >= 20 {
				close(done)
			}
		})
	}

	<-done
	close(errors)

	for err := range errors {
		assert.NoError(t, err)
	}

	producer.Close()

	err = db.Close()
	assert.NoError(t, err)
}

func BenchmarkProducer(b *testing.B) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	if err != nil {
		panic(err)
	}

	done := make(chan struct{})

	producer := NewProducer(ledger, ProducerConfig{
		BatchSize:    100,
		BatchTimeout: time.Millisecond,
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i == b.N {
			producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, func(err error) {
				if err != nil {
					panic(err)
				}

				close(done)
			})
		} else {
			producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, nil)
		}
	}

	b.StopTimer()

	producer.Close()

	err = db.Close()
	if err != nil {
		panic(err)
	}
}
