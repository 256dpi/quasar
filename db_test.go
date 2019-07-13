package quasar

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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

	db, err := OpenDB(dir)
	if err != nil {
		panic(err)
	}

	time.Sleep(5 * time.Millisecond)

	db.Close()
}
