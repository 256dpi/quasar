package quasar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsumer(t *testing.T) {
	ldb := openDB("ledger", true)
	tdb := openDB("table", true)

	ledger, err := CreateLedger(ldb)
	assert.NoError(t, err)

	table, err := CreateTable(tdb, "table")
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
		Entries: entries,
		Errors:  errors,
		Batch:   10,
	}

	consumer := NewConsumer(ledger, table, opts)

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		if counter == 50 {
			err = consumer.Ack(entry.Sequence)
			assert.NoError(t, err)

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

		if counter == 100 {
			err = consumer.Ack(entry.Sequence)
			assert.NoError(t, err)

			break
		}
	}

	consumer.Close()
	assert.Empty(t, errors)

	n, _, _ := table.Get("foo")
	assert.Equal(t, uint64(100), n)

	err = ldb.Close()
	assert.NoError(t, err)

	err = tdb.Close()
	assert.NoError(t, err)
}
