package quasar

import (
	"unsafe"

	"github.com/cockroachdb/pebble"
)

func prefixRange(prefix []byte) ([]byte, []byte) {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return prefix, limit
}

func prefixIterator(prefix []byte) *pebble.IterOptions {
	low, up := prefixRange(prefix)

	return &pebble.IterOptions{
		LowerBound: low,
		UpperBound: up,
	}
}

func toString(b []byte) string {
	// this is not safe, but unlikely to break
	return *(*string)(unsafe.Pointer(&b))
}
