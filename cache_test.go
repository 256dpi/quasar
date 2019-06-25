package quasar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	entry1 := Entry{Sequence: 1, Payload: []byte("foo")}
	entry2 := Entry{Sequence: 2, Payload: []byte("bar")}
	entry3 := Entry{Sequence: 3, Payload: []byte("baz")}

	cache := NewCache(2)
	assert.Equal(t, 0, cache.Length())

	var list []Entry
	cache.Scan(func(_ int, msg Entry) bool {
		list = append(list, msg)
		return true
	})
	assert.Equal(t, []Entry(nil), list)

	cache.Add(entry1)
	assert.Equal(t, 1, cache.Length())

	list = nil
	cache.Scan(func(_ int, msg Entry) bool {
		list = append(list, msg)
		return true
	})
	assert.Equal(t, []Entry{entry1}, list)

	cache.Add(entry2)
	assert.Equal(t, 2, cache.Length())

	list = nil
	cache.Scan(func(_ int, msg Entry) bool {
		list = append(list, msg)
		return true
	})
	assert.Equal(t, []Entry{entry1, entry2}, list)

	cache.Add(entry3)
	assert.Equal(t, 2, cache.Length())

	list = nil
	cache.Scan(func(_ int, msg Entry) bool {
		list = append(list, msg)
		return true
	})
	assert.Equal(t, []Entry{entry2, entry3}, list)

	cache.Trim(func(entry Entry) bool {
		return entry.Sequence <= 2
	})

	list = nil
	cache.Scan(func(_ int, msg Entry) bool {
		list = append(list, msg)
		return true
	})
	assert.Equal(t, []Entry{entry3}, list)

	cache.Reset()

	list = nil
	cache.Scan(func(_ int, msg Entry) bool {
		list = append(list, msg)
		return true
	})
	assert.Equal(t, []Entry(nil), list)
}

func BenchmarkCacheAdd(b *testing.B) {
	cache := NewCache(1000)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cache.Add(Entry{Sequence: uint64(i), Payload: []byte("foo")})
	}
}

func BenchmarkCacheScan(b *testing.B) {
	cache := NewCache(1000)

	for i := 0; i < 1000; i++ {
		cache.Add(Entry{Sequence: uint64(i), Payload: []byte("foo")})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		counter := 0
		cache.Scan(func(i int, entry Entry) bool {
			counter++
			return true
		})
	}
}
