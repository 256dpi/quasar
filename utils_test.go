package quasar

import (
	"os"
	"path/filepath"
)

func dir(name string) string {
	// make dir absolute
	dir, err := filepath.Abs(filepath.Join("test", name))
	if err != nil {
		panic(err)
	}

	return dir
}

func clear(name string) {
	// clear directory
	err := os.RemoveAll(dir(name))
	if err != nil {
		panic(err)
	}
}
