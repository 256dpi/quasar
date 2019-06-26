package quasar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReader(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
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

	reader := NewReader(ledger, ReaderConfig{
		Start:   0,
		Entries: entries,
		Errors:  errors,
		Batch:   10,
	})

	var next uint64

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		next = entry.Sequence + 1

		if counter == 50 {
			break
		}
	}

	reader.Close()
	assert.Empty(t, errors)

	entries = make(chan Entry, 10)

	reader = NewReader(ledger, ReaderConfig{
		Start:   next,
		Entries: entries,
		Errors:  errors,
		Batch:   10,
	})

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		if counter == 100 {
			break
		}
	}

	reader.Close()
	assert.Empty(t, errors)

	err = db.Close()
	assert.NoError(t, err)
}

func TestReaderUnblock(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	err = ledger.Write(Entry{
		Sequence: uint64(1),
		Payload:  []byte("foo"),
	})
	assert.NoError(t, err)

	entries := make(chan Entry, 1)
	errors := make(chan error, 1)

	reader := NewReader(ledger, ReaderConfig{
		Start:   ledger.Head() + 1,
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
