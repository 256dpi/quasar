package quasar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {
	db := openDB(true)

	// open

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
	assert.NoError(t, err)
	assert.NotNil(t, table)

	count, err := table.Count()
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	all, err := table.All()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]uint64{}, all)

	// set

	err = table.Set("foo", []uint64{1})
	assert.NoError(t, err)

	position, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1}, position)

	count, err = table.Count()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	all, err = table.All()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]uint64{
		"foo": {1},
	}, all)

	assert.Equal(t, map[string]string{
		"table!foo": "1",
	}, dump(db))

	// delete

	err = table.Delete("foo")
	assert.NoError(t, err)

	position, err = table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64(nil), position)

	count, err = table.Count()
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	all, err = table.All()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]uint64{}, all)

	assert.Equal(t, map[string]string{}, dump(db))

	// reset

	err = table.Set("foo", []uint64{1})
	assert.NoError(t, err)

	position, err = table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1}, position)

	all, err = table.All()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]uint64{
		"foo": {1},
	}, all)

	count, err = table.Count()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// clear

	err = table.Clear()
	assert.NoError(t, err)

	all, err = table.All()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]uint64{}, all)

	count, err = table.Count()
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	// close

	err = db.Close()
	assert.NoError(t, err)
}

func TestTableRange(t *testing.T) {
	db := openDB(true)

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
	assert.NoError(t, err)
	assert.NotNil(t, table)

	min, max, ok, err := table.Range()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), min)
	assert.Equal(t, uint64(0), max)
	assert.False(t, ok)

	err = table.Set("foo", []uint64{0})
	assert.NoError(t, err)

	min, max, ok, err = table.Range()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), min)
	assert.Equal(t, uint64(0), max)
	assert.True(t, ok)

	err = table.Set("foo", []uint64{7})
	assert.NoError(t, err)

	min, max, ok, err = table.Range()
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), min)
	assert.Equal(t, uint64(7), max)
	assert.True(t, ok)

	err = table.Set("bar", []uint64{5, 13})
	assert.NoError(t, err)

	min, max, ok, err = table.Range()
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), min)
	assert.Equal(t, uint64(13), max)
	assert.True(t, ok)

	err = db.Close()
	assert.NoError(t, err)
}

func TestTableIsolation(t *testing.T) {
	db := openDB(true)

	set(db, "foo", "1")
	set(db, "table!foo", "2")
	set(db, "z-table!foo", "3")

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
	assert.NoError(t, err)
	assert.NotNil(t, table)

	count, err := table.Count()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	all, err := table.All()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]uint64{
		"foo": {2},
	}, all)

	position, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{2}, position)
}

func TestTableReopen(t *testing.T) {
	db := openDB(true)

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
	assert.NoError(t, err)
	assert.NotNil(t, table)

	err = table.Set("foo", []uint64{1})
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	db = openDB(false)

	table, err = CreateTable(db, TableConfig{Prefix: "table"})
	assert.NoError(t, err)
	assert.NotNil(t, table)

	position, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1}, position)

	err = db.Close()
	assert.NoError(t, err)
}

func BenchmarkTableSet(b *testing.B) {
	db := openDB(true)

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err = table.Set("foo", []uint64{uint64(b.N)})
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

func BenchmarkTableGet(b *testing.B) {
	db := openDB(true)

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	err = table.Set("foo", []uint64{1})
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := table.Get("foo")
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

func BenchmarkTableDelete(b *testing.B) {
	db := openDB(true)

	table, err := CreateTable(db, TableConfig{Prefix: "table"})
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	err = table.Set("foo", []uint64{1})
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := table.Delete("foo")
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
