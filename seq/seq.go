package seq

import (
	"bytes"
	"sort"
	"strconv"
	"unsafe"
)

// EncodedLength defines the expected length of an encoded sequence.
const EncodedLength = 20

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
	// get string without copying
	str := *(*string)(unsafe.Pointer(&key))

	return strconv.ParseUint(str, 10, 64)
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
		item, err := Decode(value[i : i+index])
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
