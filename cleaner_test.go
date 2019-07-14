package quasar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCleanerAll(t *testing.T) {
	db := openDB(true)
	defer closeDB(db)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	for i := 1; i <= 10; i++ {
		err = ledger.Write(Entry{
			Sequence: uint64(i),
			Payload:  []byte("foo"),
		})
		assert.NoError(t, err)
	}

	cleaner := NewCleaner(ledger, CleanerConfig{
		Interval: 10 * time.Millisecond,
	})

	time.Sleep(15 * time.Millisecond)

	cleaner.Close()

	assert.Equal(t, 0, ledger.Length())
}

func TestCleanerMinRetention(t *testing.T) {
	db := openDB(true)
	defer closeDB(db)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	for i := 1; i <= 10; i++ {
		err = ledger.Write(Entry{
			Sequence: uint64(i),
			Payload:  []byte("foo"),
		})
		assert.NoError(t, err)
	}

	cleaner := NewCleaner(ledger, CleanerConfig{
		Retention: 5,
		Interval:  10 * time.Millisecond,
	})

	time.Sleep(15 * time.Millisecond)

	cleaner.Close()

	assert.Equal(t, 5, ledger.Length())
}

func TestCleanerMaxRetention(t *testing.T) {
	db := openDB(true)
	defer closeDB(db)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	for i := 1; i <= 20; i++ {
		err = ledger.Write(Entry{
			Sequence: uint64(i),
			Payload:  []byte("foo"),
		})
		assert.NoError(t, err)
	}

	cleaner := NewCleaner(ledger, CleanerConfig{
		Retention: 10,
		Threshold: 5,
		Interval:  10 * time.Millisecond,
	})

	time.Sleep(15 * time.Millisecond)

	cleaner.Close()

	assert.Equal(t, 5, ledger.Length())
}

func TestCleanerTablePositions(t *testing.T) {
	db := openDB(true)
	defer closeDB(db)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
	assert.NoError(t, err)

	for i := 1; i <= 20; i++ {
		err = ledger.Write(Entry{
			Sequence: uint64(i),
			Payload:  []byte("foo"),
		})
		assert.NoError(t, err)
	}

	err = table.Set("foo", []uint64{10, 12})
	assert.NoError(t, err)

	cleaner := NewCleaner(ledger, CleanerConfig{
		Retention: 5,
		Threshold: 15,
		Tables:    []*Table{table},
		Interval:  10 * time.Millisecond,
	})

	time.Sleep(15 * time.Millisecond)

	cleaner.Close()

	assert.Equal(t, 10, ledger.Length())
}
