package qis

import (
	"fmt"

	"github.com/256dpi/fpack"
	"github.com/256dpi/turing"

	"github.com/256dpi/quasar/seq"
)

// List is used to list table positions.
type List struct {
	Prefix    []byte
	Positions map[string][]uint64
}

var listDesc = &turing.Description{
	Name: "quasar/List",
}

func (s *List) Describe() *turing.Description {
	return listDesc
}

func (s *List) Effect() int {
	return 0
}

func (s *List) Execute(mem turing.Memory, _ turing.Cache) error {
	// get key
	key, ref := makeTableKey(s.Prefix, nil)
	defer ref.Release()

	// prepare map
	s.Positions = map[string][]uint64{}

	// create iterator
	iter := mem.Iterate(key)
	defer iter.Close()

	// read all keys
	for iter.First(); iter.Valid(); iter.Next() {
		// get value
		value, ref, err := iter.Value()
		if err != nil {
			return err
		}
		defer ref.Release()

		// parse positions
		positions, err := seq.DecodeList(value)
		if err != nil {
			return err
		}

		// get name
		name := iter.TempKey()[len(s.Prefix)+1:]

		// set positions
		s.Positions[string(name)] = positions
	}

	// check errors
	err := iter.Error()
	if err != nil {
		return err
	}

	return nil
}

func (s *List) Encode() ([]byte, turing.Ref, error) {
	return fpack.Encode(true, func(enc *fpack.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode prefix
		enc.Bytes(s.Prefix, 1)

		// encode length
		enc.Uint16(uint16(len(s.Positions)))

		// encode pairs
		for name, positions := range s.Positions {
			// encode name
			enc.String(name, 1)

			// encode name
			enc.Uint16(uint16(len(positions)))

			// encode positions
			for _, pos := range positions {
				enc.Uint64(pos)
			}
		}

		return nil
	})
}

func (s *List) Decode(bytes []byte) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("invalid version")
		}

		// decode prefix
		dec.Bytes(&s.Prefix, 1, true)

		// decode length
		var length uint16
		dec.Uint16(&length)

		// decode pairs
		s.Positions = make(map[string][]uint64, length)
		for i := 0; i < int(length); i++ {
			// decode name
			var name string
			dec.String(&name, 1, true)

			// decode size
			var size uint16
			dec.Uint16(&size)

			// decode positions
			positions := make([]uint64, size)
			for j := 0; j < int(size); j++ {
				dec.Uint64(&positions[j])
			}

			// set pair
			s.Positions[name] = positions
		}

		return nil
	})
}
