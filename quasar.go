package quasar

import (
	"github.com/256dpi/turing"

	"github.com/256dpi/quasar/qis"
)

var Instructions = []turing.Instruction{
	&qis.Write{}, &qis.Read{}, &qis.Stat{}, &qis.Trim{},
	&qis.List{}, &qis.Store{}, &qis.Load{},
}
