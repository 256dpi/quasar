package seq

import "unsafe"

func toString(b []byte) string {
	// this is not safe, but unlikely to break
	return *(*string)(unsafe.Pointer(&b))
}
