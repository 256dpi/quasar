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
		Batch:   10,
		Timeout: time.Millisecond,
	})

	for i := 1; i <= 20; i++ {
		if i == 20 {
			ok := producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, func(err error) {
				assert.NoError(t, err)
				close(done)
			})
			assert.True(t, ok)
		} else {
			ok := producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, func(err error) {
				assert.NoError(t, err)
			})
			assert.True(t, ok)
		}
	}

	<-done

	producer.Close()

	length := ledger.Length()
	assert.Equal(t, 20, length)

	err = db.Close()
	assert.NoError(t, err)
}

func TestProducerFilter(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	producer := NewProducer(ledger, ProducerConfig{
		Batch:   5,
		Timeout: time.Millisecond,
		Filter:  true,
	})

	done1 := make(chan struct{})

	for i := 1; i <= 10; i++ {
		if i == 10 {
			ok := producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, func(err error) {
				assert.NoError(t, err)
				close(done1)
			})
			assert.True(t, ok)
		} else {
			ok := producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, func(err error) {
				assert.NoError(t, err)
			})
			assert.True(t, ok)
		}
	}

	<-done1

	done2 := make(chan struct{})

	for i := 1; i <= 20; i++ {
		if i == 20 {
			ok := producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, func(err error) {
				assert.NoError(t, err)
				close(done2)
			})
			assert.True(t, ok)
		} else {
			ok := producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, func(err error) {
				assert.NoError(t, err)
			})
			assert.True(t, ok)
		}
	}

	<-done2

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
		Batch:   3,
		Timeout: time.Millisecond,
		Retry:   100,
		Delay:   time.Millisecond,
	})

	done := make(chan struct{})
	errors := make(chan error, 21)

	go func() {
		for {
			select {
			default:
				_, _ = ledger.Delete(20)
				time.Sleep(10 * time.Millisecond)
			case <-done:
				return
			}
		}
	}()

	for i := 1; i <= 20; i++ {
		ok := producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, func(err error) {
			errors <- err

			if len(errors) >= 20 {
				close(done)
			}
		})
		assert.True(t, ok)
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
			producer.Write(Entry{Sequence: uint64(i), Payload: []byte("foo")}, func(err error) {
				if err != nil {
					panic(err)
				}
			})
		}
	}

	b.StopTimer()

	producer.Close()

	err = db.Close()
	if err != nil {
		panic(err)
	}
}
