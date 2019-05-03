package quasar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProducer(t *testing.T) {
	ldb := openDB("ledger", true)

	ledger, err := CreateLedger(ldb)
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

	err = ldb.Close()
	assert.NoError(t, err)
}

func BenchmarkProducer(b *testing.B) {
	ldb := openDB("ledger", true)

	ledger, err := CreateLedger(ldb)
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

	err = ldb.Close()
	if err != nil {
		panic(err)
	}
}
