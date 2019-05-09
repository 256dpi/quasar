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
		Batch:   10,
		Entries: entries,
		Errors:  errors,
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
		Batch:   10,
		Entries: entries,
		Errors:  errors,
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
}
