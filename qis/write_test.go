package qis

import (
	"testing"

	"github.com/256dpi/turing"
)

func BenchmarkPush(b *testing.B) {
	machine := turing.Test(&Write{})

	push := &Write{
		Prefix: []byte("foo"),
		Entry:  []byte("bar"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := machine.Execute(push)
		if err != nil {
			panic(err)
		}
	}
}
