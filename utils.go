package quasar

import (
	"github.com/tecbot/gorocksdb"
)

var defaultReadOptions = gorocksdb.NewDefaultReadOptions()
var defaultWriteOptions = gorocksdb.NewDefaultWriteOptions()
