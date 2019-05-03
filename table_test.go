package quasar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {
	clear("table")

	// open

	table, err := OpenTable(dir("table"))
	assert.NoError(t, err)
	assert.NotNil(t, table)

	n, err := table.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), n)

	// set

	err = table.Set("foo", 1)
	assert.NoError(t, err)

	n, ok, err := table.Get("foo")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(1), n)

	n, err = table.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), n)

	// delete

	err = table.Delete("foo")
	assert.NoError(t, err)

	n, ok, err = table.Get("foo")
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), n)

	n, err = table.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), n)

	// reset

	err = table.Set("foo", 1)
	assert.NoError(t, err)

	n, ok, err = table.Get("foo")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(1), n)

	n, err = table.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), n)

	// clear

	err = table.Clear()
	assert.NoError(t, err)

	n, ok, err = table.Get("foo")
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), n)

	n, err = table.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), n)

	// close

	err = table.Close()
	assert.NoError(t, err)
}

func TestTableReopen(t *testing.T) {
	clear("table")

	l, err := OpenTable(dir("table"))
	assert.NoError(t, err)
	assert.NotNil(t, l)

	err = l.Set("foo", 1)
	assert.NoError(t, err)

	err = l.Close()
	assert.NoError(t, err)

	l, err = OpenTable(dir("table"))
	assert.NoError(t, err)
	assert.NotNil(t, l)

	n, ok, err := l.Get("foo")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(1), n)

	err = l.Close()
	assert.NoError(t, err)
}

func BenchmarkTableSet(b *testing.B) {
	clear("table")

	table, err := OpenTable(dir("table"))
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

	err = table.Close()
	if err != nil {
		panic(err)
	}
}

func BenchmarkTableGet(b *testing.B) {
	clear("table")

	l, err := OpenTable(dir("table"))
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

	err = l.Close()
	if err != nil {
		panic(err)
	}
}

func BenchmarkTableDelete(b *testing.B) {
	clear("table")

	l, err := OpenTable(dir("table"))
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

	err = l.Close()
	if err != nil {
		panic(err)
	}
}
