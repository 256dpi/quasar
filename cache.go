package quasar

import "sync"

// Cache is a circular list to cache entries.
type Cache struct {
	size  int
	nodes []Entry
	head  int
	tail  int
	count int

	mutex sync.RWMutex
}

// NewCache creates and returns a new cache.
func NewCache(size int) *Cache {
	return &Cache{
		size:  size,
		nodes: make([]Entry, size),
	}
}

// Add will add an entry to the cache.
func (q *Cache) Add(entries ...Entry) {
	// acquire mutex
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// add all entries
	for _, entry := range entries {
		// remove entry if cache is full
		if q.count == q.size {
			q.tail = q.wrap(q.tail + 1)
			q.count--
		}

		// save element to head
		q.nodes[q.head] = entry
		q.count++

		// increment head
		q.head = q.wrap(q.head + 1)
	}
}

// Scan will iterate over the cache entries.
func (q *Cache) Scan(fn func(int, Entry) bool) {
	// acquire mutex
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	// iterate through from tail to head
	for i := 0; i < q.count; i++ {
		if !fn(i, q.nodes[q.wrap(q.tail+i)]) {
			return
		}
	}
}

// Trim will remove entries from the cache with a sequence up to and including
// the provided sequence.
func (q *Cache) Trim(sequence uint64) {
	// acquire mutex
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	// iterate through from tail to head
	for i := 0; i < q.count; i++ {
		// return if entry is after provided sequence
		if q.nodes[q.wrap(q.tail+i)].Sequence > sequence {
			return
		}

		// remove entry
		q.nodes[q.tail] = Entry{}
		q.tail = q.wrap(q.tail + 1)
		q.count--
	}
}

// Length will return the length of the cache.
func (q *Cache) Length() int {
	// acquire mutex
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.count
}

// Reset will reset the cache.
func (q *Cache) Reset() {
	// acquire mutex
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// allocate a new list and reset counters
	q.nodes = make([]Entry, q.size)
	q.head = 0
	q.tail = 0
	q.count = 0
}

func (q *Cache) wrap(i int) int {
	// subtract size if is greater than size
	if i >= q.size {
		return i - q.size
	}

	return i
}
