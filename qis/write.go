package qis

import (
	"fmt"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/coding"
)

// Write is used to write entries to a ledger.
type Write struct {
	// The ledger prefix.
	Prefix []byte

	// The entries.
	Entries [][]byte

	// The new head of the ledger.
	Head uint64
}

var pushDesc = &turing.Description{
	Name: "quasar/Write",
}

func (w *Write) Describe() *turing.Description {
	return pushDesc
}

func (w *Write) Effect() int {
	return len(w.Entries) + 1
}

func (w *Write) Execute(mem turing.Memory, _ turing.Cache) error {
	// get head
	head, err := readSeq(mem, w.Prefix, headSuffix)
	if err != nil {
		return err
	}

	// prepare head
	w.Head = head

	// write entries
	for _, entry := range w.Entries {
		// increment head
		w.Head++

		// prepare entry key
		entryKey, entryRef := joinEntryKey(w.Prefix, w.Head)

		// set entry
		err = mem.Set(entryKey, entry)
		if err != nil {
			entryRef.Release()
			return err
		}

		// release
		entryRef.Release()
	}

	// write head
	err = writeSeq(mem, w.Prefix, headSuffix, w.Head)
	if err != nil {
		return err
	}

	return nil
}

func (w *Write) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode prefix and head
		enc.Bytes(w.Prefix, 1) // 256
		enc.Uint64(w.Head)

		// encode length
		enc.Uint16(uint16(len(w.Entries)))

		// encode entries
		for _, entry := range w.Entries {
			enc.Bytes(entry, 4) // ~4.3GB
		}

		return nil
	})
}

func (w *Write) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("invalid version")
		}

		// decode prefix and head
		dec.Bytes(&w.Prefix, 1, true)
		dec.Uint64(&w.Head)

		// decode length
		var length uint16
		dec.Uint16(&length)

		// decode entries
		w.Entries = make([][]byte, length)
		for i := 0; i < int(length); i++ {
			dec.Bytes(&w.Entries[i], 4, true)
		}

		return nil
	})
}
