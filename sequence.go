package quasar

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

var seconds int64
var counter uint32
var mutex sync.Mutex

// SequenceLength defines the encoded length of a sequence.
const SequenceLength = 20

// GenerateSequence will generate a locally monotonic sequence that consists of
// the current time and an ordinal number. The returned sequence is the first of
// n consecutive numbers and will either overflow in 2106 or if generated more
// than ca. 4 billion times a second.
func GenerateSequence(n uint32) uint64 {
	// acquire mutex
	mutex.Lock()

	// get current time
	now := time.Now().Unix()

	// check if reset is needed
	if seconds != now {
		seconds = now
		counter = 1
	}

	// increment counter
	counter += n

	// release mutex
	mutex.Unlock()

	// compute first number
	first := JoinSequence(time.Unix(seconds, 0), counter-n)

	return first
}

// JoinSequence constructs a sequence from a 32 bit timestamp and 32 bit ordinal
// number.
func JoinSequence(ts time.Time, n uint32) uint64 {
	return uint64(ts.Unix())<<32 | uint64(n)
}

// SplitSequence explodes the sequence in its timestamp and ordinal number.
func SplitSequence(s uint64) (time.Time, uint32) {
	ts := time.Unix(int64(s>>32), 0)
	return ts, uint32(s & 0xFFFFFFFF)
}

// EncodeSequence will encode a sequence.
func EncodeSequence(s uint64, compact bool) []byte {
	// check compact
	if compact {
		return []byte(fmt.Sprintf("%d", s))
	}

	return []byte(fmt.Sprintf("%020d", s))
}

// DecodeSequence will decode a sequence.
func DecodeSequence(key []byte) (uint64, error) {
	return strconv.ParseUint(string(key), 10, 64)
}
