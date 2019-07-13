package quasar

import (
	"github.com/tecbot/gorocksdb"
)

var defaultReadOptions = gorocksdb.NewDefaultReadOptions()
var defaultWriteOptions = gorocksdb.NewDefaultWriteOptions()

func init() {
	defaultReadOptions.SetFillCache(false)
	defaultWriteOptions.SetSync(true)
}
