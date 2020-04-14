package seq

import (
	"bytes"
	"sort"
	"strconv"
	"sync"
	"time"
)

var seconds int64
var counter uint32
var mutex sync.Mutex

// EncodedLength defines the expected length of an encoded sequence.
const EncodedLength = 20

// Generate will generate a locally monotonic sequence that consists of
// the current time and an ordinal number. The returned sequence is the first of
// n consecutive numbers and will either overflow in 2106 or if generated more
// than ca. 4 billion times a second.
func Generate(n uint32) uint64 {
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
	first := Join(time.Unix(seconds, 0), counter-n)

	return first
}

// Join constructs a sequence from a 32 bit timestamp and 32 bit ordinal
// number.
func Join(ts time.Time, n uint32) uint64 {
	return uint64(ts.Unix())<<32 | uint64(n)
}

// Split explodes the sequence in its timestamp and ordinal number.
func Split(s uint64) (time.Time, uint32) {
	ts := time.Unix(int64(s>>32), 0)
	return ts, uint32(s & 0xFFFFFFFF)
}

// Encode will encode a sequence.
func Encode(s uint64, compact bool) []byte {
	// prepare buffer
	buf := make([]byte, EncodedLength*2)

	// encode number
	res := strconv.AppendUint(buf[EncodedLength:EncodedLength], s, 10)

	// return directly if compact or full
	if compact || len(res) >= EncodedLength {
		return res
	}

	// determine start
	start := len(res)

	// writes zeroes
	for i := start; i < start+EncodedLength-len(res); i++ {
		buf[i] = '0'
	}

	// slice number
	res = buf[start : start+EncodedLength]

	return res
}

// Decode will decode a sequence.
func Decode(key []byte) (uint64, error) {
	return strconv.ParseUint(toString(key), 10, 64)
}

// EncodeList will encode a list of compacted sequences.
func EncodeList(list []uint64) []byte {
	// check length
	if len(list) == 0 {
		return nil
	}

	// prepare buffer
	buf := make([]byte, 0, len(list)*(EncodedLength+1))

	// add sequences
	for _, item := range list {
		buf = strconv.AppendUint(buf, item, 10)
		buf = append(buf, ',')
	}

	return buf[:len(buf)-1]
}

// DecodeList will decode a list of compacted sequences.
func DecodeList(value []byte) ([]uint64, error) {
	// prepare list
	list := make([]uint64, 0, bytes.Count(value, []byte(","))+1)

	// parse all items
	for i := 0; i < len(value); {
		// get index of next separator
		index := bytes.Index(value[i:], []byte(","))
		if index <= 0 {
			index = len(value) - i
		}

		// parse item
		item, err := strconv.ParseUint(toString(value[i:i+index]), 10, 64)
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

// Compile will compile a list of positive sequences from the provided
// mark table. It will compress positive adjacent tail sequences and inject a
// fake positive sequence at the beginning if the first entry in the table is
// negative.
func Compile(table map[uint64]bool) []uint64 {
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
