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
	entries := make(chan Entry, 10)

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch: 10,
	})

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		ret := make(chan error, 1)
		ok := consumer.Mark(entry.Sequence, false, func(err error) {
			ret <- err
		})
		assert.NoError(t, <-ret)
		assert.True(t, ok)

		if counter == 50 {
			break
		}
	}

	consumer.Close()

	entries = make(chan Entry, 10)

	consumer = NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch: 10,
	})

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence, counter)
		assert.Equal(t, []byte("foo"), entry.Payload)

		ret := make(chan error, 1)
		ok := consumer.Mark(entry.Sequence, false, func(err error) {
			ret <- err
		})
		assert.NoError(t, <-ret)
		assert.True(t, ok)

		if counter == 100 {
			break
		}
	}

	consumer.Close()

	sequences, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{100}, sequences)

	err = db.Close()
	assert.NoError(t, err)
}

func TestConsumerWindow(t *testing.T) {
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

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch:  5,
		Window: 10,
	})

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		ret := make(chan error, 1)
		ok := consumer.Mark(entry.Sequence, false, func(err error) {
			ret <- err
		})
		assert.NoError(t, <-ret)
		assert.True(t, ok)

		if counter == 50 {
			break
		}
	}

	consumer.Close()

	entries = make(chan Entry, 10)

	consumer = NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch:  5,
		Window: 10,
	})

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence, counter)
		assert.Equal(t, []byte("foo"), entry.Payload)

		ret := make(chan error, 1)
		ok := consumer.Mark(entry.Sequence, false, func(err error) {
			ret <- err
		})
		assert.NoError(t, <-ret)
		assert.True(t, ok)

		if counter == 100 {
			break
		}
	}

	consumer.Close()

	sequences, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{100}, sequences)

	err = db.Close()
	assert.NoError(t, err)
}

func TestConsumerCumulativeMarks(t *testing.T) {
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
	entries := make(chan Entry, 10)

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch:  10,
		Window: 20,
	})

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		if counter%10 == 0 {
			ret := make(chan error, 1)
			ok := consumer.Mark(entry.Sequence, true, func(err error) {
				ret <- err
			})
			assert.NoError(t, <-ret)
			assert.True(t, ok)
		}

		if counter == 100 {
			break
		}
	}

	consumer.Close()

	sequences, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{100}, sequences)

	err = db.Close()
	assert.NoError(t, err)
}

func TestConsumerMissedMark(t *testing.T) {
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
	entries := make(chan Entry, 10)
	errs := make(chan error, 1)

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			errs <- err
		},
		Batch:    10,
		Window:   20,
		Deadline: 10 * time.Millisecond,
	})

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		if counter <= 5 {
			consumer.Mark(entry.Sequence, false, nil)
		}

		if counter == 20 {
			break
		}
	}

	err = <-errs
	assert.Equal(t, ErrConsumerDeadlock, err)

	consumer.Close()

	sequences, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{5}, sequences)

	err = db.Close()
	assert.NoError(t, err)
}

func TestConsumerTemporary(t *testing.T) {
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

	consumer := NewConsumer(ledger, nil, ConsumerConfig{
		Start:   0,
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch: 10,
	})

	var last uint64

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		last = entry.Sequence

		if counter == 50 {
			break
		}
	}

	consumer.Close()

	entries = make(chan Entry, 10)

	consumer = NewConsumer(ledger, nil, ConsumerConfig{
		Start:   last + 1,
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch: 10,
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

	consumer.Close()

	err = db.Close()
	assert.NoError(t, err)
}

func TestConsumerUnorderedMarks(t *testing.T) {
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
	signal := make(chan struct{}, 100)

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch:  5,
		Window: 10,
	})

	for i := 0; i < 5; i++ {
		go func() {
			for {
				entry, ok := <-entries
				if !ok {
					return
				}

				time.Sleep(time.Duration(rand.Intn(10000)) * time.Microsecond)

				ret := make(chan error, 1)
				ok = consumer.Mark(entry.Sequence, false, func(err error) {
					ret <- err
				})
				assert.NoError(t, <-ret)
				assert.True(t, ok)

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

	consumer.Close()

	sequences, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{100}, sequences)

	err = db.Close()
	assert.NoError(t, err)
}

func TestConsumerSlowLedger(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
	assert.NoError(t, err)

	entries := make(chan Entry, 1)

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch:  5,
		Window: 10,
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

	counter := 0

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		ret := make(chan error, 1)
		ok := consumer.Mark(entry.Sequence, false, func(err error) {
			ret <- err
		})
		assert.NoError(t, <-ret)
		assert.True(t, ok)

		if counter == 100 {
			break
		}
	}

	consumer.Close()

	sequences, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{100}, sequences)

	err = db.Close()
	assert.NoError(t, err)
}

func TestConsumerSkipMark(t *testing.T) {
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

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch:  5,
		Window: 10,
		Skip:   2,
	})

	entry := <-entries

	ret1 := make(chan error, 1)
	ok := consumer.Mark(entry.Sequence, false, func(err error) {
		ret1 <- err
	})
	assert.True(t, ok)

	time.Sleep(10 * time.Millisecond)

	sequences, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{0}, sequences)

	entry = <-entries

	ret2 := make(chan error, 1)
	ok = consumer.Mark(entry.Sequence, false, func(err error) {
		ret2 <- err
	})
	assert.True(t, ok)

	time.Sleep(10 * time.Millisecond)

	sequences, err = table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{0}, sequences)

	entry = <-entries

	ret3 := make(chan error, 1)
	ok = consumer.Mark(entry.Sequence, false, func(err error) {
		ret3 <- err
	})
	assert.True(t, ok)

	time.Sleep(10 * time.Millisecond)

	assert.NoError(t, <-ret1)
	assert.NoError(t, <-ret2)
	assert.NoError(t, <-ret3)

	sequences, err = table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{3}, sequences)

	entry = <-entries

	ret4 := make(chan error, 1)
	ok = consumer.Mark(entry.Sequence, false, func(err error) {
		ret4 <- err
	})
	assert.True(t, ok)

	time.Sleep(10 * time.Millisecond)

	consumer.Close()

	assert.NoError(t, <-ret4)

	sequences, err = table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{4}, sequences)

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

	err = table.Set("foo", []uint64{1})
	assert.NoError(t, err)

	entries := make(chan Entry, 1)

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch:  1,
		Window: 10,
	})

	err = ledger.Write(Entry{
		Sequence: uint64(2),
		Payload:  []byte("foo"),
	})
	assert.NoError(t, err)

	entry := <-entries
	assert.Equal(t, uint64(2), entry.Sequence)
	assert.Equal(t, []byte("foo"), entry.Payload)

	consumer.Close()

	err = db.Close()
	assert.NoError(t, err)
}

func TestConsumerResumeOutOfRange(t *testing.T) {
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

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch: 10,
	})

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		ret := make(chan error, 1)
		ok := consumer.Mark(entry.Sequence, false, func(err error) {
			ret <- err
		})
		assert.NoError(t, <-ret)
		assert.True(t, ok)

		if counter == 50 {
			break
		}
	}

	consumer.Close()

	position, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{50}, position)

	_, err = ledger.Delete(60)
	assert.NoError(t, err)

	counter += 10

	entries = make(chan Entry, 10)

	consumer = NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: entries,
		Errors: func(err error) {
			panic(err)
		},
		Batch: 10,
	})

	for {
		counter++

		entry := <-entries
		assert.Equal(t, uint64(counter), entry.Sequence)
		assert.Equal(t, []byte("foo"), entry.Payload)

		ret := make(chan error, 1)
		ok := consumer.Mark(entry.Sequence, false, func(err error) {
			ret <- err
		})
		assert.NoError(t, <-ret)
		assert.True(t, ok)

		if counter == 90 {
			break
		}
	}

	consumer.Close()

	position, err = table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{90}, position)

	err = db.Close()
	assert.NoError(t, err)
}

func TestConsumerInvalidSequence(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
	assert.NoError(t, err)

	consumer := NewConsumer(ledger, table, ConsumerConfig{
		Name:    "foo",
		Entries: make(chan Entry, 1),
		Errors: func(err error) {
			panic(err)
		},
		Batch:  1,
		Window: 10,
	})

	err = ledger.Write(Entry{
		Sequence: uint64(1),
		Payload:  []byte("foo"),
	})
	assert.NoError(t, err)

	ret := make(chan error, 1)
	ok := consumer.Mark(2, false, func(err error) {
		ret <- err
	})
	assert.Equal(t, ErrInvalidSequence, <-ret)
	assert.True(t, ok)

	consumer.Close()

	err = db.Close()
	assert.NoError(t, err)
}
