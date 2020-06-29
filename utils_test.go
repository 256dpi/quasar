package quasar

import (
	"github.com/256dpi/turing"
	"github.com/256dpi/turing/stdset"

	"github.com/256dpi/quasar/qis"
)

func init() {
	turing.SetLogger(nil)
}

func startMachine() *turing.Machine {
	return turing.Test(
		&qis.Write{}, &qis.Read{}, &qis.Stat{}, &qis.Trim{},
		&qis.List{}, &qis.Store{}, &qis.Load{},
		&stdset.Set{}, &stdset.Dump{},
	)
}

func set(m *turing.Machine, key, value string) {
	// set entry
	err := m.Execute(&stdset.Set{
		Key:   []byte(key),
		Value: []byte(value),
	})
	if err != nil {
		panic(err)
	}
}

func dump(m *turing.Machine) map[string]string {
	// dump keys
	mp := &stdset.Dump{}
	err := m.Execute(mp)
	if err != nil {
		panic(err)
	}

	return mp.Map
}
