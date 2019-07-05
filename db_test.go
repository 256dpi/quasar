package quasar

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOpenDB(t *testing.T) {
	dir, err := filepath.Abs(filepath.Join("test"))
	if err != nil {
		panic(err)
	}

	err = os.RemoveAll(dir)
	if err != nil {
		panic(err)
	}

	db, closer, err := OpenDB(dir, DBConfig{
		GCInterval: 2 * time.Millisecond,
		GCErrors: func(err error) {
			panic(err)
		},
	})
	if err != nil {
		panic(err)
	}

	time.Sleep(5 * time.Millisecond)

	closer()

	err = db.Close()
	assert.NoError(t, err)
}
