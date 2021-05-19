package qis

import (
	"fmt"

	"github.com/256dpi/fpack"
	"github.com/256dpi/turing"
)

// Read is used to read entries from a ledger.
type Read struct {
	// The ledger prefix.
	Prefix []byte

	// The start sequence.
	Start uint64

	// The maximum number number of entries to read.
	Limit uint16

	// The read entries.
	Entries [][]byte
}

var readDesc = &turing.Description{
	Name: "quasar/Read",
}

func (r *Read) Describe() *turing.Description {
	return readDesc
}

func (r *Read) Effect() int {
	return 0
}

func (r *Read) Execute(mem turing.Memory, _ turing.Cache) error {
	// prepare prefix
	prefix, prefixRef := fpack.Concat(r.Prefix, entrySuffix)
	defer prefixRef.Release()

	// create iterator
	iter := mem.Iterate(prefix)
	defer iter.Close()

	// prepare start
	start, startRef := joinEntryKey(r.Prefix, r.Start)
	defer startRef.Release()

	// prepare list
	r.Entries = make([][]byte, 0, r.Limit)

	// read entries
	for iter.SeekGE(start); iter.Valid() && len(r.Entries) < int(r.Limit); iter.Next() {
		// read first sequence
		if len(r.Entries) == 0 {
			_, r.Start = splitEntryKey(iter.TempKey())
		}

		// get value
		value, ref, err := iter.Value()
		if err != nil {
			return err
		}

		// add entry
		r.Entries = append(r.Entries, turing.Clone(value))

		// release
		ref.Release()
	}

	// check error
	err := iter.Error()
	if err != nil {
		return err
	}

	return nil
}

func (r *Read) Encode() ([]byte, turing.Ref, error) {
	return fpack.Encode(true, func(enc *fpack.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode start and limit
		enc.Uint64(r.Start)
		enc.Uint16(r.Limit)

		// encode entries
		for _, entry := range r.Entries {
			enc.Bytes(entry, 4)
		}

		return nil
	})
}

func (r *Read) Decode(bytes []byte) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("invalid version")
		}

		// decode start and limit
		dec.Uint64(&r.Start)
		dec.Uint16(&r.Limit)

		// decode entries
		for i := 0; i < int(r.Limit); i++ {
			dec.Bytes(&r.Entries[i], 4, true)
		}

		return nil
	})
}
