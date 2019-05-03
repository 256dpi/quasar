package quasar

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLedger(t *testing.T) {
	ldb := openDB("ledger", true)

	// open

	ledger, err := CreateLedger(ldb)
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	ch := make(chan uint64, 10)
	ledger.Subscribe(ch)

	n := ledger.Length()
	assert.Equal(t, 0, n)

	last := ledger.Head()
	assert.Equal(t, uint64(0), last)

	// write single

	err = ledger.Write(Entry{Sequence: 1, Payload: []byte("foo")})
	assert.NoError(t, err)

	notification := <-ch
	assert.Equal(t, uint64(1), notification)

	entries, err := ledger.Read(0, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 1, Payload: []byte("foo")},
	}, entries)

	n = ledger.Length()
	assert.Equal(t, 1, n)

	last = ledger.Head()
	assert.Equal(t, uint64(1), last)

	// write multiple

	err = ledger.Write(
		Entry{Sequence: 2, Payload: []byte("bar")},
		Entry{Sequence: 3, Payload: []byte("baz")},
		Entry{Sequence: 4, Payload: []byte("baz")},
	)
	assert.NoError(t, err)

	notification = <-ch
	assert.Equal(t, uint64(4), notification)

	entries, err = ledger.Read(0, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 1, Payload: []byte("foo")},
		{Sequence: 2, Payload: []byte("bar")},
		{Sequence: 3, Payload: []byte("baz")},
		{Sequence: 4, Payload: []byte("baz")},
	}, entries)

	n = ledger.Length()
	assert.Equal(t, 4, n)

	last = ledger.Head()
	assert.Equal(t, uint64(4), last)

	// read partial

	entries, err = ledger.Read(2, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 2, Payload: []byte("bar")},
		{Sequence: 3, Payload: []byte("baz")},
		{Sequence: 4, Payload: []byte("baz")},
	}, entries)

	// delete

	err = ledger.Delete(3)
	assert.NoError(t, err)

	entries, err = ledger.Read(0, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 4, Payload: []byte("baz")},
	}, entries)

	n = ledger.Length()
	assert.Equal(t, 1, n)

	last = ledger.Head()
	assert.Equal(t, uint64(4), last)

	// close

	err = ldb.Close()
	assert.NoError(t, err)
}

func TestLedgerReopen(t *testing.T) {
	ldb := openDB("ledger", true)

	ledger, err := CreateLedger(ldb)
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	err = ledger.Write(Entry{Sequence: 1, Payload: []byte("foo")})
	assert.NoError(t, err)

	err = ldb.Close()
	assert.NoError(t, err)

	ldb = openDB("ledger", false)

	ledger, err = CreateLedger(ldb)
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	entries, err := ledger.Read(0, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 1, Payload: []byte("foo")},
	}, entries)

	n := ledger.Length()
	assert.Equal(t, 1, n)

	last := ledger.Head()
	assert.Equal(t, uint64(1), last)

	err = ldb.Close()
	assert.NoError(t, err)
}

func BenchmarkLedgerWrite(b *testing.B) {
	ldb := openDB("ledger", true)

	ledger, err := CreateLedger(ldb)
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	size := 1000
	payload := []byte("hello world!")

	b.ResetTimer()
	b.ReportAllocs()

	batch := make([]Entry, 0, size)

	for i := 0; i < b.N; i++ {
		batch = append(batch, Entry{Sequence: uint64(b.N), Payload: payload})

		if len(batch) == size {
			err = ledger.Write(batch...)
			if err != nil {
				panic(err)
			}

			batch = make([]Entry, 0, size)
		}
	}

	if len(batch) > 0 {
		err = ledger.Write(batch...)
		if err != nil {
			panic(err)
		}
	}

	b.StopTimer()

	err = ldb.Close()
	if err != nil {
		panic(err)
	}
}

func BenchmarkLedgerRead(b *testing.B) {
	ldb := openDB("ledger", true)

	ledger, err := CreateLedger(ldb)
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	size := 1000
	payload := []byte("hello world!")

	batch := make([]Entry, 0, size)
	for i := 0; i < size; i++ {
		batch = append(batch, Entry{Sequence: uint64(i), Payload: payload})
	}

	err = ledger.Write(batch...)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := ledger.Read(uint64(rand.Int63n(int64(size))), 1)
		if err != nil {
			panic(err)
		}
	}

	b.StopTimer()

	err = ldb.Close()
	if err != nil {
		panic(err)
	}
}

func BenchmarkLedgerDelete(b *testing.B) {
	ldb := openDB("ledger", true)

	ledger, err := CreateLedger(ldb)
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	size := 1000
	payload := []byte("hello world!")

	batch := make([]Entry, 0, size)
	for i := 0; i < size; i++ {
		batch = append(batch, Entry{Sequence: uint64(i), Payload: payload})
	}

	err = ledger.Write(batch...)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := ledger.Delete(uint64(rand.Int63n(int64(size))))
		if err != nil {
			panic(err)
		}
	}

	b.StopTimer()

	err = ldb.Close()
	if err != nil {
		panic(err)
	}
}
