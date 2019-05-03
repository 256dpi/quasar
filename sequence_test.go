package quasar

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGenerateSequence(t *testing.T) {
	start1 := GenerateSequence(100)
	assert.True(t, start1 > 0)

	start2 := GenerateSequence(100)
	assert.Equal(t, start1+100, start2)
}

func TestSplitAndJoinSequence(t *testing.T) {
	now, err := time.Parse(time.RFC3339, "2019-05-02T12:05:42+03:00")
	assert.NoError(t, err)

	seq := JoinSequence(now, 42)
	assert.Equal(t, uint64(6686353297697144874), seq)

	ts, n := SplitSequence(seq)

	assert.Equal(t, now.UTC(), ts.UTC())
	assert.Equal(t, uint32(42), n)
}

func TestSequenceProperties(t *testing.T) {
	ts, n := SplitSequence(0)
	assert.Equal(t, time.Unix(0, 0), ts)
	assert.Equal(t, uint32(0), n)

	ts, n = SplitSequence(math.MaxUint64)
	end, _ := time.Parse(time.RFC3339, "2106-02-07T06:28:15+00:00")
	assert.True(t, end.UTC().Equal(ts.UTC()))
	assert.Equal(t, uint32(math.MaxUint32), n)
}

func TestEncodeAndDecodeKey(t *testing.T) {
	key := EncodeSequence(0)
	assert.Equal(t, []byte("00000000000000000000"), key)

	n, err := DecodeSequence(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), n)

	key = EncodeSequence(1)
	assert.Equal(t, []byte("00000000000000000001"), key)

	n, err = DecodeSequence(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), n)

	key = EncodeSequence(math.MaxUint64)
	assert.Equal(t, []byte("18446744073709551615"), key)

	n, err = DecodeSequence(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(math.MaxUint64), n)
}

func BenchmarkGenerateSequence(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		GenerateSequence(100)
	}
}

func BenchmarkEncodeSequence(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		EncodeSequence(uint64(i))
	}
}

func BenchmarkDecodeSequence(b *testing.B) {
	m := map[int][]byte{}
	for i := 0; i < b.N; i++ {
		m[i] = EncodeSequence(uint64(i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DecodeSequence(m[i])
		if err != nil {
			panic(err)
		}
	}
}
