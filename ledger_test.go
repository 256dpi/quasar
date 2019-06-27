package quasar

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLedger(t *testing.T) {
	db := openDB(true)

	// open

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	ch := make(chan uint64, 10)
	ledger.Subscribe(ch)

	length := ledger.Length()
	assert.Equal(t, 0, length)

	head := ledger.Head()
	assert.Equal(t, uint64(0), head)

	assert.Equal(t, map[string]string{}, dump(db))

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

	length = ledger.Length()
	assert.Equal(t, 1, length)

	head = ledger.Head()
	assert.Equal(t, uint64(1), head)

	assert.Equal(t, map[string]string{
		"ledger!head":                 "1",
		"ledger#00000000000000000001": "foo",
	}, dump(db))

	// write multiple

	err = ledger.Write(
		Entry{Sequence: 2, Payload: []byte("bar")},
		Entry{Sequence: 3, Payload: []byte("baz")},
		Entry{Sequence: 4, Payload: []byte("qux")},
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
		{Sequence: 4, Payload: []byte("qux")},
	}, entries)

	length = ledger.Length()
	assert.Equal(t, 4, length)

	head = ledger.Head()
	assert.Equal(t, uint64(4), head)

	assert.Equal(t, map[string]string{
		"ledger!head":                 "4",
		"ledger#00000000000000000001": "foo",
		"ledger#00000000000000000002": "bar",
		"ledger#00000000000000000003": "baz",
		"ledger#00000000000000000004": "qux",
	}, dump(db))

	// read partial

	entries, err = ledger.Read(2, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 2, Payload: []byte("bar")},
		{Sequence: 3, Payload: []byte("baz")},
		{Sequence: 4, Payload: []byte("qux")},
	}, entries)

	// delete

	n, err := ledger.Delete(3)
	assert.NoError(t, err)
	assert.Equal(t, 3, n)

	entries, err = ledger.Read(0, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 4, Payload: []byte("qux")},
	}, entries)

	length = ledger.Length()
	assert.Equal(t, 1, length)

	head = ledger.Head()
	assert.Equal(t, uint64(4), head)

	assert.Equal(t, map[string]string{
		"ledger!head":                 "4",
		"ledger#00000000000000000004": "qux",
	}, dump(db))

	// close

	err = db.Close()
	assert.NoError(t, err)
}

func TestLedgerDeleteOutOfRange(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	err = ledger.Write(
		Entry{Sequence: 2, Payload: []byte("foo")},
		Entry{Sequence: 3, Payload: []byte("bar")},
		Entry{Sequence: 4, Payload: []byte("baz")},
		Entry{Sequence: 5, Payload: []byte("qux")},
	)
	assert.NoError(t, err)

	n, err := ledger.Delete(1)
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	length := ledger.Length()
	assert.Equal(t, 4, length)

	err = db.Close()
	assert.NoError(t, err)
}

func TestLedgerIndex(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	index, found, err := ledger.Index(0)
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, uint64(0), index)

	index, found, err = ledger.Index(2)
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, uint64(0), index)

	index, found, err = ledger.Index(-2)
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, uint64(0), index)

	err = ledger.Write(
		Entry{Sequence: 1, Payload: []byte("foo")},
		Entry{Sequence: 2, Payload: []byte("bar")},
		Entry{Sequence: 3, Payload: []byte("baz")},
		Entry{Sequence: 4, Payload: []byte("qux")},
	)
	assert.NoError(t, err)

	index, found, err = ledger.Index(0)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, uint64(1), index)

	index, found, err = ledger.Index(2)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, uint64(3), index)

	index, found, err = ledger.Index(-2)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, uint64(3), index)

	index, found, err = ledger.Index(4)
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, uint64(4), index)

	index, found, err = ledger.Index(-5)
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, uint64(1), index)

	n, err := ledger.Delete(4)
	assert.NoError(t, err)
	assert.Equal(t, 4, n)

	index, found, err = ledger.Index(1)
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, uint64(4), index)

	err = db.Close()
	assert.NoError(t, err)
}

func TestLedgerClear(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	err = ledger.Write(
		Entry{Sequence: 1, Payload: []byte("foo")},
		Entry{Sequence: 2, Payload: []byte("bar")},
		Entry{Sequence: 3, Payload: []byte("baz")},
		Entry{Sequence: 4, Payload: []byte("qux")},
	)
	assert.NoError(t, err)

	length := ledger.Length()
	assert.Equal(t, 4, length)

	head := ledger.Head()
	assert.Equal(t, uint64(4), head)

	err = ledger.Clear()
	assert.NoError(t, err)

	length = ledger.Length()
	assert.Equal(t, 0, length)

	head = ledger.Head()
	assert.Equal(t, uint64(4), head)

	assert.Equal(t, map[string]string{
		"ledger!head": "4",
	}, dump(db))

	err = db.Close()
	assert.NoError(t, err)
}

func TestLedgerReset(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	err = ledger.Write(
		Entry{Sequence: 1, Payload: []byte("foo")},
		Entry{Sequence: 2, Payload: []byte("bar")},
		Entry{Sequence: 3, Payload: []byte("baz")},
		Entry{Sequence: 4, Payload: []byte("qux")},
	)
	assert.NoError(t, err)

	length := ledger.Length()
	assert.Equal(t, 4, length)

	head := ledger.Head()
	assert.Equal(t, uint64(4), head)

	err = ledger.Reset()
	assert.NoError(t, err)

	length = ledger.Length()
	assert.Equal(t, 0, length)

	head = ledger.Head()
	assert.Equal(t, uint64(0), head)

	assert.Equal(t, map[string]string{}, dump(db))

	err = db.Close()
	assert.NoError(t, err)
}

func TestLedgerIsolation(t *testing.T) {
	db := openDB(true)

	set(db, "00000000000000000001", "a")
	set(db, "e:00000000000000000002", "b")
	set(db, "ledger!head", "3")
	set(db, "ledger#00000000000000000003", "c")
	set(db, "ledger#00000000000000000003", "c")
	set(db, "z-ledger#00000000000000000004", "d")

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	entries, err := ledger.Read(0, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 3, Payload: []byte("c")},
	}, entries)

	length := ledger.Length()
	assert.Equal(t, 1, length)

	head := ledger.Head()
	assert.Equal(t, uint64(3), head)

	err = db.Close()
	assert.NoError(t, err)
}

func TestLedgerMonotonicity(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	err = ledger.Write(Entry{Sequence: 2, Payload: []byte("foo")})
	assert.NoError(t, err)

	err = ledger.Write(Entry{Sequence: 1, Payload: []byte("foo")})
	assert.Equal(t, ErrNotMonotonic, err)

	err = ledger.Write(Entry{Sequence: 2, Payload: []byte("foo")})
	assert.Equal(t, ErrNotMonotonic, err)

	err = ledger.Write(
		Entry{Sequence: 4, Payload: []byte("foo")},
		Entry{Sequence: 3, Payload: []byte("foo")},
	)
	assert.Equal(t, ErrNotMonotonic, err)

	err = db.Close()
	assert.NoError(t, err)
}

func TestLedgerReopen(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	err = ledger.Write(Entry{Sequence: 1, Payload: []byte("foo")})
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	db = openDB(false)

	ledger, err = CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	entries, err := ledger.Read(0, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 1, Payload: []byte("foo")},
	}, entries)

	length := ledger.Length()
	assert.Equal(t, 1, length)

	head := ledger.Head()
	assert.Equal(t, uint64(1), head)

	err = db.Close()
	assert.NoError(t, err)
}

func TestLedgerReopenCollapsed(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	err = ledger.Write(Entry{Sequence: 1, Payload: []byte("foo")})
	assert.NoError(t, err)

	err = ledger.Clear()
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	db = openDB(false)

	ledger, err = CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	length := ledger.Length()
	assert.Equal(t, 0, length)

	head := ledger.Head()
	assert.Equal(t, uint64(1), head)

	err = db.Close()
	assert.NoError(t, err)
}

func TestLedgerHugeDelete(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	batch := make([]Entry, 0, db.MaxBatchCount()+10)
	for i := int64(1); i <= (db.MaxBatchCount() + 10); i++ {
		batch = append(batch, Entry{
			Sequence: uint64(i),
			Payload:  []byte("foo"),
		})
	}

	err = ledger.Write(batch[0 : db.MaxBatchCount()-10]...)
	assert.NoError(t, err)

	err = ledger.Write(batch[db.MaxBatchCount()-10 : db.MaxBatchCount()+10]...)
	assert.NoError(t, err)

	n, err := ledger.Delete(uint64(db.MaxBatchCount() + 10))
	assert.NoError(t, err)
	assert.Equal(t, int(db.MaxBatchCount()+10), n)

	length := ledger.length
	assert.Equal(t, 0, length)

	err = db.Close()
	assert.NoError(t, err)
}

func TestLedgerCache(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger", Cache: 3})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	err = ledger.Write(Entry{Sequence: 1, Payload: []byte("foo")})
	assert.NoError(t, err)

	// cached read

	entries, err := ledger.Read(1, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 1, Payload: []byte("foo")},
	}, entries)

	err = ledger.Write(
		Entry{Sequence: 2, Payload: []byte("bar")},
		Entry{Sequence: 3, Payload: []byte("baz")},
		Entry{Sequence: 4, Payload: []byte("qux")},
	)
	assert.NoError(t, err)

	// not cached read

	entries, err = ledger.Read(1, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 1, Payload: []byte("foo")},
		{Sequence: 2, Payload: []byte("bar")},
		{Sequence: 3, Payload: []byte("baz")},
		{Sequence: 4, Payload: []byte("qux")},
	}, entries)

	// cached read

	entries, err = ledger.Read(2, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 2, Payload: []byte("bar")},
		{Sequence: 3, Payload: []byte("baz")},
		{Sequence: 4, Payload: []byte("qux")},
	}, entries)

	// cache invalidation

	n, err := ledger.Delete(2)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	entries, err = ledger.Read(2, 10)
	assert.NoError(t, err)
	assert.Equal(t, []Entry{
		{Sequence: 3, Payload: []byte("baz")},
		{Sequence: 4, Payload: []byte("qux")},
	}, entries)

	err = db.Close()
	assert.NoError(t, err)
}

func TestLedgerLimit(t *testing.T) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger", Limit: 3})
	assert.NoError(t, err)
	assert.NotNil(t, ledger)

	err = ledger.Write(
		Entry{Sequence: 1, Payload: []byte("bar")},
		Entry{Sequence: 2, Payload: []byte("baz")},
		Entry{Sequence: 3, Payload: []byte("qux")},
		Entry{Sequence: 4, Payload: []byte("qux")},
	)
	assert.Equal(t, ErrLimitReached, err)

	err = ledger.Write(
		Entry{Sequence: 1, Payload: []byte("bar")},
		Entry{Sequence: 2, Payload: []byte("baz")},
	)
	assert.NoError(t, err)

	err = ledger.Write(
		Entry{Sequence: 3, Payload: []byte("qux")},
		Entry{Sequence: 4, Payload: []byte("qux")},
	)
	assert.Equal(t, ErrLimitReached, err)

	err = ledger.Write(
		Entry{Sequence: 3, Payload: []byte("bar")},
	)
	assert.NoError(t, err)

	err = ledger.Write(
		Entry{Sequence: 4, Payload: []byte("qux")},
	)
	assert.Equal(t, ErrLimitReached, err)

	err = db.Close()
	assert.NoError(t, err)
}

func BenchmarkLedgerWrite(b *testing.B) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	size := 1000
	payload := []byte("hello world!")

	b.ResetTimer()
	b.ReportAllocs()

	batch := make([]Entry, 0, size)

	for i := 1; i <= b.N; i++ {
		batch = append(batch, Entry{Sequence: uint64(i), Payload: payload})

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

	err = db.Close()
	if err != nil {
		panic(err)
	}
}

func BenchmarkLedgerRead(b *testing.B) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	size := 1000
	payload := []byte("hello world!")

	batch := make([]Entry, 0, size)
	for i := 1; i <= size; i++ {
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

	err = db.Close()
	if err != nil {
		panic(err)
	}
}

func BenchmarkLedgerDelete(b *testing.B) {
	db := openDB(true)

	ledger, err := CreateLedger(db, LedgerConfig{Prefix: "ledger"})
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	size := 1000
	payload := []byte("hello world!")

	batch := make([]Entry, 0, size)
	for i := 1; i <= size; i++ {
		batch = append(batch, Entry{Sequence: uint64(i), Payload: payload})
	}

	err = ledger.Write(batch...)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := ledger.Delete(uint64(rand.Int63n(int64(size))))
		if err != nil {
			panic(err)
		}
	}

	b.StopTimer()

	err = db.Close()
	if err != nil {
		panic(err)
	}
}
