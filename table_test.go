package quasar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {
	tdb := openDB("table", true)

	// open

	table, err := CreateTable(tdb, "table")
	assert.NoError(t, err)
	assert.NotNil(t, table)

	count, err := table.Count()
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	// set

	err = table.Set("foo", 1)
	assert.NoError(t, err)

	position, ok, err := table.Get("foo")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(1), position)

	count, err = table.Count()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	assert.Equal(t, map[string]string{
		"table:foo": "00000000000000000001",
	}, dump(tdb))

	// delete

	err = table.Delete("foo")
	assert.NoError(t, err)

	position, ok, err = table.Get("foo")
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), position)

	count, err = table.Count()
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	assert.Equal(t, map[string]string{}, dump(tdb))

	// reset

	err = table.Set("foo", 1)
	assert.NoError(t, err)

	position, ok, err = table.Get("foo")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(1), position)

	count, err = table.Count()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// count

	count, err = table.Count()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// close

	err = tdb.Close()
	assert.NoError(t, err)
}

func TestTableIsolation(t *testing.T) {
	tdb := openDB("table", true)

	set(tdb, "foo", "00000000000000000001")
	set(tdb, "table:foo", "00000000000000000002")
	set(tdb, "z-table:foo", "00000000000000000003")

	table, err := CreateTable(tdb, "table")
	assert.NoError(t, err)
	assert.NotNil(t, table)

	count, err := table.Count()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	position, ok, err := table.Get("foo")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(2), position)
}

func TestTableReopen(t *testing.T) {
	tdb := openDB("table", true)

	table, err := CreateTable(tdb, "table")
	assert.NoError(t, err)
	assert.NotNil(t, table)

	err = table.Set("foo", 1)
	assert.NoError(t, err)

	err = tdb.Close()
	assert.NoError(t, err)

	tdb = openDB("table", false)

	table, err = CreateTable(tdb, "table")
	assert.NoError(t, err)
	assert.NotNil(t, table)

	position, ok, err := table.Get("foo")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(1), position)

	err = tdb.Close()
	assert.NoError(t, err)
}

func BenchmarkTableSet(b *testing.B) {
	tdb := openDB("table", true)

	table, err := CreateTable(tdb, "table")
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err = table.Set("foo", uint64(b.N))
		if err != nil {
			panic(err)
		}
	}

	b.StopTimer()

	err = tdb.Close()
	if err != nil {
		panic(err)
	}
}

func BenchmarkTableGet(b *testing.B) {
	tdb := openDB("table", true)

	l, err := CreateTable(tdb, "table")
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	err = l.Set("foo", 1)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := l.Get("foo")
		if err != nil {
			panic(err)
		}
	}

	b.StopTimer()

	err = tdb.Close()
	if err != nil {
		panic(err)
	}
}

func BenchmarkTableDelete(b *testing.B) {
	tdb := openDB("table", true)

	l, err := CreateTable(tdb, "table")
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	err = l.Set("foo", 1)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := l.Delete("foo")
		if err != nil {
			panic(err)
		}
	}

	b.StopTimer()

	err = tdb.Close()
	if err != nil {
		panic(err)
	}
}
