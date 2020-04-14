package qis

import (
	"fmt"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/coding"
)

type Stat struct {
	Prefix []byte
	Head   uint64
	Tail   uint64
}

var statDesc = &turing.Description{
	Name: "quasar/Stat",
}

func (s *Stat) Describe() *turing.Description {
	return statDesc
}

func (s *Stat) Effect() int {
	return 0
}

func (s *Stat) Execute(mem turing.Memory) error {
	// get head
	head, err := readSeq(mem, s.Prefix, headSuffix)
	if err != nil {
		return err
	}

	// get tail
	tail, err := readSeq(mem, s.Prefix, tailSuffix)
	if err != nil {
		return err
	}

	// set head and tail
	s.Head = head
	s.Tail = tail

	return nil
}

func (s *Stat) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode values
		enc.Uint64(s.Head)
		enc.Uint64(s.Tail)

		return nil
	})
}

func (s *Stat) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("invalid version")
		}

		// decode values
		dec.Uint64(&s.Head)
		dec.Uint64(&s.Tail)

		return nil
	})
}