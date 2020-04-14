package seq

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGenerateSequence(t *testing.T) {
	start1 := Generate(100)
	assert.True(t, start1 > 0)

	start2 := Generate(100)
	assert.Equal(t, start1+100, start2)
}

func TestSplitAndJoinSequence(t *testing.T) {
	now, err := time.Parse(time.RFC3339, "2019-05-02T12:05:42+03:00")
	assert.NoError(t, err)

	seq := Join(now, 42)
	assert.Equal(t, uint64(6686353297697144874), seq)

	ts, n := Split(seq)

	assert.Equal(t, now.UTC(), ts.UTC())
	assert.Equal(t, uint32(42), n)
}

func TestSequenceProperties(t *testing.T) {
	ts, n := Split(0)
	assert.Equal(t, time.Unix(0, 0), ts)
	assert.Equal(t, uint32(0), n)

	ts, n = Split(math.MaxUint64)
	end, _ := time.Parse(time.RFC3339, "2106-02-07T06:28:15+00:00")
	assert.True(t, end.UTC().Equal(ts.UTC()))
	assert.Equal(t, uint32(math.MaxUint32), n)
}

func TestEncodeAndDecodeSequence(t *testing.T) {
	key := Encode(0, true)
	assert.Equal(t, []byte("0"), key)

	key = Encode(0, false)
	assert.Equal(t, []byte("00000000000000000000"), key)

	n, err := Decode(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), n)

	key = Encode(1, true)
	assert.Equal(t, []byte("1"), key)

	key = Encode(1, false)
	assert.Equal(t, []byte("00000000000000000001"), key)

	n, err = Decode(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), n)

	key = Encode(math.MaxUint64, false)
	assert.Equal(t, []byte("18446744073709551615"), key)

	n, err = Decode(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(math.MaxUint64), n)
}

func TestEncodeDecodeSequences(t *testing.T) {
	value := EncodeList([]uint64{1, 2, 3})
	assert.Equal(t, []byte("1,2,3"), value)

	list, err := DecodeList(value)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3}, list)

	value = EncodeList([]uint64{7})
	assert.Equal(t, []byte("7"), value)

	list, err = DecodeList(value)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{7}, list)

	value = EncodeList([]uint64{})
	assert.Equal(t, []byte(nil), value)

	list, err = DecodeList(value)
	assert.NoError(t, err)
	assert.Equal(t, []uint64{}, list)
}

func TestCompileSequences(t *testing.T) {
	table := []struct {
		m map[uint64]bool
		l []uint64
	}{
		// basic
		{
			m: map[uint64]bool{
				1: true,
				2: false,
				3: true,
				4: false,
				5: true,
				6: false,
				7: true,
			},
			l: []uint64{1, 3, 5, 7},
		},
		// compress tail
		{
			m: map[uint64]bool{
				1: true,
				2: true,
				3: false,
				4: true,
				5: true,
				6: false,
				7: true,
			},
			l: []uint64{2, 4, 5, 7},
		},
		// inject start
		{
			m: map[uint64]bool{
				2: false,
				3: true,
				4: false,
				5: true,
			},
			l: []uint64{1, 3, 5},
		},
		// inject start
		{
			m: map[uint64]bool{
				1: false,
				2: true,
				3: false,
				4: true,
			},
			l: []uint64{0, 2, 4},
		},
	}

	for i, item := range table {
		l := Compile(item.m)
		assert.Equal(t, item.l, l, "test %d", i)
	}
}

func BenchmarkGenerateSequence(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		Generate(100)
	}
}

func BenchmarkEncodeSequence(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		Encode(uint64(i), false)
	}
}

func BenchmarkEncodeSequenceCompact(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		Encode(uint64(i), true)
	}
}

func BenchmarkDecodeSequence(b *testing.B) {
	m := map[int][]byte{}
	for i := 0; i < b.N; i++ {
		m[i] = Encode(uint64(i), false)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := Decode(m[i])
		if err != nil {
			panic(err)
		}
	}
}
