package quasar

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorker(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerOptions{Prefix: "ledger"})
	assert.NoError(t, err)

	matrix, err := CreateMatrix(db, MatrixOptions{Prefix: "matrix"})
	assert.NoError(t, err)

	for i := 1; i <= 100; i++ {
		err = ledger.Write(Entry{
			Sequence: uint64(i),
			Payload:  []byte("foo"),
		})
		assert.NoError(t, err)
	}

	counter := 0
	entries := make(chan Entry, 1)
	errors := make(chan error, 1)

	opts := WorkerOptions{
		Name:    "foo",
		Batch:   5,
		Window:  10,
		Entries: entries,
		Errors:  errors,
	}

	worker := NewWorker(ledger, matrix, opts)

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		worker.Ack(entry.Sequence)

		if counter == 50 {
			break
		}
	}

	time.Sleep(100 * time.Millisecond)

	worker.Close()
	assert.Empty(t, errors)

	entries = make(chan Entry, 10)
	opts.Entries = entries

	worker = NewWorker(ledger, matrix, opts)

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence, counter)
		assert.Equal(t, []byte("foo"), entry.Payload)

		worker.Ack(entry.Sequence)

		if counter == 100 {
			break
		}
	}

	time.Sleep(100 * time.Millisecond)

	worker.Close()
	assert.Empty(t, errors)

	sequences, err := matrix.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{100}, sequences)

	err = db.Close()
	assert.NoError(t, err)
}

func TestWorkerRandom(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerOptions{Prefix: "ledger"})
	assert.NoError(t, err)

	matrix, err := CreateMatrix(db, MatrixOptions{Prefix: "matrix"})
	assert.NoError(t, err)

	for i := 1; i <= 100; i++ {
		err = ledger.Write(Entry{
			Sequence: uint64(i),
			Payload:  []byte("foo"),
		})
		assert.NoError(t, err)
	}

	entries := make(chan Entry, 1)
	errors := make(chan error, 1)
	signal := make(chan struct{}, 100)

	opts := WorkerOptions{
		Name:    "foo",
		Batch:   5,
		Window:  10,
		Entries: entries,
		Errors:  errors,
	}

	worker := NewWorker(ledger, matrix, opts)

	for i := 0; i < 5; i++ {
		go func() {
			for {
				entry, ok := <-entries
				if !ok {
					return
				}

				time.Sleep(time.Duration(rand.Intn(10000)) * time.Microsecond)

				worker.Ack(entry.Sequence)
				signal <- struct{}{}
			}
		}()
	}

	counter := 0

	for {
		<-signal

		counter++
		if counter == 100 {
			break
		}
	}

	time.Sleep(100 * time.Millisecond)

	worker.Close()
	assert.Empty(t, errors)

	sequences, err := matrix.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{100}, sequences)

	err = db.Close()
	assert.NoError(t, err)
}

func TestWorkerOnDemand(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerOptions{Prefix: "ledger"})
	assert.NoError(t, err)

	matrix, err := CreateMatrix(db, MatrixOptions{Prefix: "matrix"})
	assert.NoError(t, err)

	entries := make(chan Entry, 1)
	errors := make(chan error, 1)

	opts := WorkerOptions{
		Name:    "foo",
		Batch:   5,
		Window:  10,
		Entries: entries,
		Errors:  errors,
	}

	worker := NewWorker(ledger, matrix, opts)

	go func() {
		for i := 1; i <= 100; i++ {
			time.Sleep(time.Duration(rand.Intn(10000)) * time.Microsecond)

			err = ledger.Write(Entry{
				Sequence: uint64(i),
				Payload:  []byte("foo"),
			})
			assert.NoError(t, err)
		}
	}()

	counter := 0

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		worker.Ack(entry.Sequence)

		if counter == 100 {
			break
		}
	}

	time.Sleep(100 * time.Millisecond)

	worker.Close()
	assert.Empty(t, errors)

	sequences, err := matrix.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{100}, sequences)

	err = db.Close()
	assert.NoError(t, err)
}
