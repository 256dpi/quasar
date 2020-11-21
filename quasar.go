package quasar

import (
	"github.com/256dpi/turing"

	"github.com/256dpi/quasar/qis"
)

// Instructions is the list of turing instructions used by quasar.
var Instructions = []turing.Instruction{
	&qis.Write{}, &qis.Read{}, &qis.Stat{}, &qis.Trim{},
	&qis.List{}, &qis.Store{}, &qis.Load{},
}
