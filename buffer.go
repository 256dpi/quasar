package quasar

import "sync"

// Buffer is a circular buffer to store entries.
type Buffer struct {
	size   int
	nodes  []Entry
	head   int
	tail   int
	length int

	mutex sync.RWMutex
}

// NewBuffer creates and returns a new cache.
func NewBuffer(size int) *Buffer {
	return &Buffer{
		size:  size,
		nodes: make([]Entry, size),
	}
}

// Push will add entries to the buffer.
func (b *Buffer) Push(entries ...Entry) {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// add all entries
	for _, entry := range entries {
		// remove last entry if buffer is full
		if b.length == b.size {
			b.tail = b.wrap(b.tail + 1)
			b.length--
		}

		// save element to head
		b.nodes[b.head] = entry
		b.head = b.wrap(b.head + 1)
		b.length++
	}
}

// Scan will iterate over the buffered entries until false is returned.
func (b *Buffer) Scan(fn func(Entry) bool) {
	// acquire mutex
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	// iterate through from tail to head
	for i := 0; i < b.length; i++ {
		// return if false is returned
		if !fn(b.nodes[b.wrap(b.tail+i)]) {
			return
		}
	}
}

// Trim will remove entries from the cache until false is returned.
func (b *Buffer) Trim(fn func(Entry) bool) {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// iterate through from tail to head
	for i := 0; i < b.length; i++ {
		// return if false is returned
		if !fn(b.nodes[b.wrap(b.tail+i)]) {
			return
		}

		// remove entry
		b.nodes[b.tail] = Entry{}
		b.tail = b.wrap(b.tail + 1)
		b.length--
	}
}

// Length will return the length of the buffer.
func (b *Buffer) Length() int {
	// acquire mutex
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.length
}

// Reset will reset the buffer.
func (b *Buffer) Reset() {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// unset all entries
	for i := range b.nodes {
		b.nodes[i] = Entry{}
	}

	// reset counters
	b.head = 0
	b.tail = 0
	b.length = 0
}

func (b *Buffer) wrap(i int) int {
	// subtract size if is greater than size
	if i >= b.size {
		return i - b.size
	}

	return i
}
