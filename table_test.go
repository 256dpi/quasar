package quasar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {
	m := startMachine()
	defer m.Stop()

	// open

	table, err := CreateTable(m, TableConfig{Prefix: "table"})
	assert.NoError(t, err)
	assert.NotNil(t, table)

	all, err := table.All()
	assert.NoError(t, err)
	assert.Empty(t, all)

	// set

	err = table.Set("foo", []uint64{1})
	assert.NoError(t, err)

	position, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1}, position)

	all, err = table.All()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]uint64{
		"foo": {1},
	}, all)

	assert.Equal(t, map[string]string{
		"table!foo": "1",
	}, dump(m))

	// delete

	err = table.Set("foo", nil)
	assert.NoError(t, err)

	position, err = table.Get("foo")
	assert.NoError(t, err)
	assert.Empty(t, position)

	all, err = table.All()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]uint64{}, all)

	assert.Equal(t, map[string]string{}, dump(m))

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
}

func TestTableCache(t *testing.T) {
	m := startMachine()
	defer m.Stop()

	// open

	table, err := CreateTable(m, TableConfig{Prefix: "table", Cache: true})
	assert.NoError(t, err)
	assert.NotNil(t, table)

	all, err := table.All()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]uint64{}, all)

	// set

	err = table.Set("foo", []uint64{1})
	assert.NoError(t, err)

	position, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1}, position)

	all, err = table.All()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]uint64{
		"foo": {1},
	}, all)

	assert.Equal(t, map[string]string{
		"table!foo": "1",
	}, dump(m))

	// delete

	err = table.Set("foo", nil)
	assert.NoError(t, err)

	position, err = table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64(nil), position)

	all, err = table.All()
	assert.NoError(t, err)
	assert.Empty(t, map[string][]uint64{}, all)

	assert.Equal(t, map[string]string{}, dump(m))

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
}

func TestTableRange(t *testing.T) {
	m := startMachine()
	defer m.Stop()

	table, err := CreateTable(m, TableConfig{Prefix: "table"})
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

}

func TestTableRangeCache(t *testing.T) {
	m := startMachine()
	defer m.Stop()

	table, err := CreateTable(m, TableConfig{Prefix: "table", Cache: true})
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

}

func TestTableIsolation(t *testing.T) {
	m := startMachine()
	defer m.Stop()

	set(m, "foo", "1")
	set(m, "table!foo", "2")
	set(m, "z-table!foo", "3")

	table, err := CreateTable(m, TableConfig{Prefix: "table"})
	assert.NoError(t, err)
	assert.NotNil(t, table)

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
	m := startMachine()
	defer m.Stop()

	table, err := CreateTable(m, TableConfig{Prefix: "table"})
	assert.NoError(t, err)
	assert.NotNil(t, table)

	err = table.Set("foo", []uint64{1})
	assert.NoError(t, err)

	table, err = CreateTable(m, TableConfig{Prefix: "table"})
	assert.NoError(t, err)
	assert.NotNil(t, table)

	position, err := table.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1}, position)
}

func BenchmarkTableSet(b *testing.B) {
	m := startMachine()
	defer m.Stop()

	table, err := CreateTable(m, TableConfig{Prefix: "table"})
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
}

func BenchmarkTableGet(b *testing.B) {
	m := startMachine()
	defer m.Stop()

	table, err := CreateTable(m, TableConfig{Prefix: "table"})
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
}

func BenchmarkTableDelete(b *testing.B) {
	m := startMachine()
	defer m.Stop()

	table, err := CreateTable(m, TableConfig{Prefix: "table"})
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
		err := table.Set("foo", nil)
		if err != nil {
			panic(err)
		}
	}
}
