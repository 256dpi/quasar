package qis

import (
	"encoding/binary"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/coding"
)

var headSuffix = []byte("!head")
var tailSuffix = []byte("!tail")
var entrySuffix = []byte("#")

func readSeq(mem turing.Memory, prefix, suffix []byte) (uint64, error) {
	// get key
	key, ref := coding.Concat(prefix, suffix)
	defer ref.Release()

	// get sequence
	var seq uint64
	err := mem.Use(key, func(value []byte) error {
		seq = binary.BigEndian.Uint64(value)
		return nil
	})
	if err != nil {
		return 0, err
	}

	return seq, nil
}

func writeSeq(mem turing.Memory, prefix, suffix []byte, seq uint64) error {
	// get key
	key, ref := coding.Concat(prefix, suffix)
	defer ref.Release()

	// encode sequence
	value := make([]byte, 8) // faster than sync.Pool
	binary.BigEndian.PutUint64(value, seq)

	// set sequence
	err := mem.Set(key, value)
	if err != nil {
		return err
	}

	return nil
}

func joinEntryKey(prefix []byte, seq uint64) ([]byte, *coding.Ref) {
	// borrow buffer
	buf, ref := coding.Borrow(len(prefix) + 1 + 8)

	// write prefix
	copy(buf, prefix)

	// write sequence
	buf[len(prefix)] = '#'
	binary.BigEndian.PutUint64(buf[len(prefix)+1:], seq)

	return buf, ref
}

func splitEntryKey(key []byte) ([]byte, uint64) {
	// get prefix
	prefix := key[:len(key)-8]

	// get sequence
	seq := binary.BigEndian.Uint64(key[len(key)-8:])

	return prefix, seq
}

func makeTableKey(prefix, name []byte) ([]byte, *coding.Ref) {
	// borrow buffer
	buf, ref := coding.Borrow(len(prefix) + 1 + len(name))

	// write prefix
	copy(buf, prefix)

	// write name
	buf[len(prefix)] = '!'
	copy(buf[len(prefix)+1:], name)

	return buf, ref
}
