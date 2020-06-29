package qis

import (
	"fmt"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/coding"

	"github.com/256dpi/quasar/seq"
)

// Store is used to store table positions.
type Store struct {
	Prefix    []byte
	Name      []byte
	Positions []uint64
}

var storeDesc = &turing.Description{
	Name: "quasar/Store",
}

func (s *Store) Describe() *turing.Description {
	return storeDesc
}

func (s *Store) Effect() int {
	return 1
}

func (s *Store) Execute(mem turing.Memory, _ turing.Cache) error {
	// get key
	key, ref := makeTableKey(s.Prefix, s.Name)
	defer ref.Release()

	// unset if empty
	if len(s.Positions) == 0 {
		return mem.Unset(key)
	}

	// set positions
	err := mem.Set(key, seq.EncodeList(s.Positions))
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode prefix and name
		enc.Bytes(s.Prefix, 1)
		enc.Bytes(s.Name, 1)

		// encode length
		enc.Uint16(uint16(len(s.Positions)))

		// encode positions
		for _, pos := range s.Positions {
			enc.Uint64(pos)
		}

		return nil
	})
}

func (s *Store) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("invalid version")
		}

		// decode prefix and name
		dec.Bytes(&s.Prefix, 1, true)
		dec.Bytes(&s.Name, 1, true)

		// decode length
		var length uint16
		dec.Uint16(&length)

		// decode entries
		s.Positions = make([]uint64, length)
		for i := 0; i < int(length); i++ {
			dec.Uint64(&s.Positions[i])
		}

		return nil
	})
}
