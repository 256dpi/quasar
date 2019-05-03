package quasar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProducer(t *testing.T) {
	clear("ledger")

	ledger, err := OpenLedger(dir("ledger"))
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

	err = ledger.Close()
	assert.NoError(t, err)
}

func BenchmarkProducer(b *testing.B) {
	clear("ledger")

	ledger, err := OpenLedger(dir("ledger"))
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

	err = ledger.Close()
	if err != nil {
		panic(err)
	}
}
