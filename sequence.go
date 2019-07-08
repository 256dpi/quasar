package quasar

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"time"
)

var seconds int64
var counter uint32
var mutex sync.Mutex

// EncodedSequenceLength defines the expected length of an encoded sequence.
const EncodedSequenceLength = 20

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

// EncodeSequence will encode a sequence to the specified writer.
func EncodeSequenceTo(w io.Writer, s uint64, compact bool) error {
	// check compact
	if compact {
		_, err := fmt.Fprintf(w, "%d", s)
		return err
	}

	_, err := fmt.Fprintf(w, "%020d", s)
	return err
}

// DecodeSequence will decode a sequence.
func DecodeSequence(key []byte) (uint64, error) {
	return strconv.ParseUint(string(key), 10, 64)
}

// EncodeSequences will encode a list of compacted sequences.
func EncodeSequences(list []uint64) []byte {
	// check length
	if len(list) == 0 {
		return nil
	}

	// prepare buffer
	buf := make([]byte, 0, len(list)*(EncodedSequenceLength+1))

	// add sequences
	for _, item := range list {
		buf = strconv.AppendUint(buf, item, 10)
		buf = append(buf, ',')
	}

	return buf[:len(buf)-1]
}

// DecodeSequences will decode a list of compacted sequences.
func DecodeSequences(value []byte) ([]uint64, error) {
	// prepare list
	list := make([]uint64, 0, bytes.Count(value, []byte(",")))

	// parse all items
	for i := 0; i < len(value); {
		// get index of next separator
		index := bytes.Index(value[i:], []byte(","))
		if index <= 0 {
			index = len(value) - i
		}

		// parse item
		item, err := strconv.ParseUint(string(value[i:i+index]), 10, 64)
		if err != nil {
			return nil, err
		}

		// add item
		list = append(list, item)

		// advance counter
		i += index + 1
	}

	return list, nil
}

// CompileSequences will compile a list of positive sequences from the provided
// mark table. It will compress positive adjacent tail sequences and inject a
// fake positive sequence at the beginning if the first entry in the table is
// negative.
func CompileSequences(table map[uint64]bool) []uint64 {
	// compile lists
	var all = make([]uint64, 0, len(table))
	var set = make([]uint64, 0, len(table))
	for seq, ok := range table {
		all = append(all, seq)

		if ok {
			set = append(set, seq)
		}
	}

	// sort lists
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	sort.Slice(set, func(i, j int) bool { return set[i] < set[j] })

	// compact adjacent tails sequences
	for {
		// stop if less than two marked sequences remaining
		if len(set) < 2 {
			break
		}

		// stop if no adjacent positives at front
		if set[0] != all[0] || set[1] != all[1] {
			break
		}

		// remove first positive
		all = all[1:]
		set = set[1:]
	}

	// inject fake true start if required
	if len(set) > 0 && set[0] != all[0] {
		set = append([]uint64{all[0] - 1}, set...)
	}

	return set
}
