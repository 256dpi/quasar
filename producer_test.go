package quasar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProducer(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, "ledger")
	assert.NoError(t, err)

	done := make(chan struct{})

	producer := NewProducer(ledger, ProducerOptions{
		Batch:   10,
		Timeout: time.Millisecond,
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

	n := ledger.Length()
	assert.Equal(t, 20, n)

	err = db.Close()
	assert.NoError(t, err)
}

func BenchmarkProducer(b *testing.B) {
	db := openDB(true)

	ledger, err := CreateLedger(db, "ledger")
	if err != nil {
		panic(err)
	}

	done := make(chan struct{})

	producer := NewProducer(ledger, ProducerOptions{
		Batch:   100,
		Timeout: time.Millisecond,
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
