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
		&stdset.Set{}, &stdset.Map{},
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
	// map keys
	mp := &stdset.Map{}
	err := m.Execute(mp)
	if err != nil {
		panic(err)
	}

	// build map
	data := map[string]string{}
	for key, value := range mp.Pairs {
		data[key] = string(value)
	}

	return data
}
