package quasar

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConsumer(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
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

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors:  errors,
		Batch:   10,
	})

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

	consumer = NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors:  errors,
		Batch:   10,
	})

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

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
	assert.NoError(t, err)

	counter := 0
	entries := make(chan Entry, 1)
	errors := make(chan error, 1)

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors:  errors,
		Batch:   10,
	})

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

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
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

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors:  errors,
		Batch:   10,
		Skip:    2,
	})

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

	entry = <-entries

	err = consumer.Ack(entry.Sequence)
	assert.NoError(t, err)

	consumer.Close()
	assert.Empty(t, errors)

	position, err = table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), position)

	err = db.Close()
	assert.NoError(t, err)
}

func TestConsumerUnblock(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
	assert.NoError(t, err)

	err = ledger.Write(Entry{
		Sequence: uint64(1),
		Payload:  []byte("foo"),
	})
	assert.NoError(t, err)

	err = table.Set("foo", 1)
	assert.NoError(t, err)

	entries := make(chan Entry, 1)
	errors := make(chan error, 1)

	reader := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors:  errors,
		Batch:   1,
	})

	err = ledger.Write(Entry{
		Sequence: uint64(2),
		Payload:  []byte("foo"),
	})
	assert.NoError(t, err)

	entry := <-entries
	assert.Equal(t, uint64(2), entry.Sequence)
	assert.Equal(t, []byte("foo"), entry.Payload)

	reader.Close()
	assert.Empty(t, errors)

	err = db.Close()
	assert.NoError(t, err)
}
