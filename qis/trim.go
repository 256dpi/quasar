package qis

import (
	"fmt"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/coding"
)

// Trim is used to delete entries from a ledger
type Trim struct {
	Prefix   []byte
	Sequence uint64
	Deleted  uint16
}

var trimDesc = &turing.Description{
	Name: "quasar/Trim",
}

func (t *Trim) Describe() *turing.Description {
	return trimDesc
}

func (t *Trim) Effect() int {
	return 1
}

func (t *Trim) Execute(mem turing.Memory) error {
	// get head
	head, err := readSeq(mem, t.Prefix, headSuffix)
	if err != nil {
		return err
	}

	// get tail
	tail, err := readSeq(mem, t.Prefix, tailSuffix)
	if err != nil {
		return err
	}

	// correct sequence if beyond head
	if t.Sequence > head {
		t.Sequence = head
	}

	// skip if sequence is at or below tail
	if t.Sequence <= tail {
		return nil
	}

	// seq <= head && seq > tail

	// prepare start entry
	start, startRef := joinEntryKey(t.Prefix, tail+1)
	defer startRef.Release()

	// prepare end entry
	end, endRef := joinEntryKey(t.Prefix, t.Sequence+1)
	defer endRef.Release()

	// delete entries
	err = mem.Delete(start, end)
	if err != nil {
		return err
	}

	// set tail
	err = writeSeq(mem, t.Prefix, tailSuffix, t.Sequence)
	if err != nil {
		return err
	}

	// set deleted
	t.Deleted = uint16(t.Sequence - tail)

	return nil
}

func (t *Trim) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode body
		enc.Bytes(t.Prefix, 1) // 256
		enc.Uint64(t.Sequence)
		enc.Uint16(t.Deleted)

		return nil
	})
}

func (t *Trim) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("invalid version")
		}

		// decode body
		dec.Bytes(&t.Prefix, 1, true)
		dec.Uint64(&t.Sequence)
		dec.Uint16(&t.Deleted)

		return nil
	})
}
