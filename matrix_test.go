package quasar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMatrix(t *testing.T) {
	db := openDB(true)

	// open

	matrix, err := CreateMatrix(db, MatrixConfig{Prefix: "matrix"})
	assert.NoError(t, err)
	assert.NotNil(t, matrix)

	count, err := matrix.Count()
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	// set

	err = matrix.Set("foo", []uint64{1})
	assert.NoError(t, err)

	position, err := matrix.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1}, position)

	count, err = matrix.Count()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	assert.Equal(t, map[string]string{
		"matrix:foo": "1",
	}, dump(db))

	// delete

	err = matrix.Delete("foo")
	assert.NoError(t, err)

	position, err = matrix.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64(nil), position)

	count, err = matrix.Count()
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	assert.Equal(t, map[string]string{}, dump(db))

	// reset

	err = matrix.Set("foo", []uint64{1})
	assert.NoError(t, err)

	position, err = matrix.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1}, position)

	count, err = matrix.Count()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// clear

	err = matrix.Clear()
	assert.NoError(t, err)

	count, err = matrix.Count()
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	// close

	err = db.Close()
	assert.NoError(t, err)
}

func TestMatrixRange(t *testing.T) {
	db := openDB(true)

	matrix, err := CreateMatrix(db, MatrixConfig{Prefix: "matrix"})
	assert.NoError(t, err)
	assert.NotNil(t, matrix)

	min, max, err := matrix.Range()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), min)
	assert.Equal(t, uint64(0), max)

	err = matrix.Set("foo", []uint64{7})
	assert.NoError(t, err)

	min, max, err = matrix.Range()
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), min)
	assert.Equal(t, uint64(7), max)

	err = matrix.Set("bar", []uint64{5, 13})
	assert.NoError(t, err)

	min, max, err = matrix.Range()
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), min)
	assert.Equal(t, uint64(13), max)

	err = db.Close()
	assert.NoError(t, err)
}

func TestMatrixIsolation(t *testing.T) {
	db := openDB(true)

	set(db, "foo", "1")
	set(db, "matrix:foo", "2")
	set(db, "z-matrix:foo", "3")

	matrix, err := CreateMatrix(db, MatrixConfig{Prefix: "matrix"})
	assert.NoError(t, err)
	assert.NotNil(t, matrix)

	count, err := matrix.Count()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	position, err := matrix.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{2}, position)
}

func TestMatrixReopen(t *testing.T) {
	db := openDB(true)

	matrix, err := CreateMatrix(db, MatrixConfig{Prefix: "matrix"})
	assert.NoError(t, err)
	assert.NotNil(t, matrix)

	err = matrix.Set("foo", []uint64{1})
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	db = openDB(false)

	matrix, err = CreateMatrix(db, MatrixConfig{Prefix: "matrix"})
	assert.NoError(t, err)
	assert.NotNil(t, matrix)

	position, err := matrix.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1}, position)

	err = db.Close()
	assert.NoError(t, err)
}

func BenchmarkMatrixSet(b *testing.B) {
	db := openDB(true)

	matrix, err := CreateMatrix(db, MatrixConfig{Prefix: "matrix"})
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err = matrix.Set("foo", []uint64{uint64(b.N)})
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

func BenchmarkMatrixGet(b *testing.B) {
	db := openDB(true)

	l, err := CreateMatrix(db, MatrixConfig{Prefix: "matrix"})
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	err = l.Set("foo", []uint64{1})
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := l.Get("foo")
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

func BenchmarkMatrixDelete(b *testing.B) {
	db := openDB(true)

	l, err := CreateMatrix(db, MatrixConfig{Prefix: "matrix"})
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	err = l.Set("foo", []uint64{1})
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

	err = db.Close()
	if err != nil {
		panic(err)
	}
}
