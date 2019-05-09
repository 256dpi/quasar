package quasar

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConsumer(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerOptions{Prefix: "ledger"})
	assert.NoError(t, err)

	table, err := CreateTable(db, TableOptions{Prefix: "table"})
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

	opts := ConsumerOptions{
		Name:    "foo",
		Batch:   10,
		Entries: entries,
		Errors:  errors,
	}

	consumer := NewConsumer(ledger, table, opts)

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		err = consumer.Ack(entry.Sequence)
		assert.NoError(t, err)

		if counter == 50 {
			break
		}
	}

	consumer.Close()
	assert.Empty(t, errors)

	entries = make(chan Entry, 10)
	opts.Entries = entries

	consumer = NewConsumer(ledger, table, opts)

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		err = consumer.Ack(entry.Sequence)
		assert.NoError(t, err)

		if counter == 100 {
			break
		}
	}

	consumer.Close()
	assert.Empty(t, errors)

	position, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), position)

	err = db.Close()
	assert.NoError(t, err)
}

func TestConsumerOnDemand(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerOptions{Prefix: "ledger"})
	assert.NoError(t, err)

	table, err := CreateTable(db, TableOptions{Prefix: "table"})
	assert.NoError(t, err)

	counter := 0
	entries := make(chan Entry, 1)
	errors := make(chan error, 1)

	opts := ConsumerOptions{
		Name:    "foo",
		Batch:   10,
		Entries: entries,
		Errors:  errors,
	}

	consumer := NewConsumer(ledger, table, opts)

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

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		err = consumer.Ack(entry.Sequence)
		assert.NoError(t, err)

		if counter == 100 {
			break
		}
	}

	consumer.Close()
	assert.Empty(t, errors)

	position, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), position)

	err = db.Close()
	assert.NoError(t, err)
}

func TestConsumerSkipping(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerOptions{Prefix: "ledger"})
	assert.NoError(t, err)

	table, err := CreateTable(db, TableOptions{Prefix: "table"})
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

	opts := ConsumerOptions{
		Name:    "foo",
		Batch:   10,
		Skip:    2,
		Entries: entries,
		Errors:  errors,
	}

	consumer := NewConsumer(ledger, table, opts)

	entry := <-entries

	err = consumer.Ack(entry.Sequence)
	assert.NoError(t, err)

	position, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), position)

	entry = <-entries

	err = consumer.Ack(entry.Sequence)
	assert.NoError(t, err)

	position, err = table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), position)

	entry = <-entries

	err = consumer.Ack(entry.Sequence)
	assert.NoError(t, err)

	position, err = table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), position)

	consumer.Close()
	assert.Empty(t, errors)

	err = db.Close()
	assert.NoError(t, err)
}
