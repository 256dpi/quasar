package quasar

import (
	"errors"
	"sync"

	"github.com/256dpi/turing"

	"github.com/256dpi/quasar/qis"
)

// TODO: We use head and tail the wrong way around. Let's correct it.

// ErrLimitReached is returned for write attempts that go beyond the allowed
// ledger length.
var ErrLimitReached = errors.New("limit reached")

// Entry is a single entry in the ledger.
type Entry struct {
	// The entries sequence.
	Sequence uint64

	// The entries payload that is written to disk.
	Payload []byte

	// The reference to a shared object. This can be used with cached ledgers
	// to retain a reference to a decoded object of the entry.
	Object interface{}
}

// LedgerConfig is used to configure a ledger.
type LedgerConfig struct {
	// The prefix for all ledger keys.
	Prefix string

	// The amount of entries to cache in memory.
	Cache int

	// The maximum length of the ledger. Write() will return ErrLimitReached if
	// the ledger is longer than this value.
	Limit int
}

// TODO: Allow concurrent access.

// Ledger manages the storage of sequential entries.
type Ledger struct {
	machine *turing.Machine
	config  LedgerConfig

	cache     *Buffer
	observer  *ledgerObserver
	receivers map[chan<- uint64]struct{}

	head  uint64
	tail  uint64
	mutex sync.Mutex
}

// CreateLedger will create a ledger that stores entries in the provided db.
// Read, write and delete requests can be issued concurrently to maximize
// performance.
func CreateLedger(machine *turing.Machine, config LedgerConfig) (*Ledger, error) {
	// check prefix
	if config.Prefix == "" {
		panic("quasar: missing prefix")
	}

	// prepare cache
	var cache *Buffer
	if config.Cache > 0 {
		cache = NewBuffer(config.Cache)
	}

	// create ledger
	l := &Ledger{
		machine:   machine,
		config:    config,
		cache:     cache,
		receivers: make(map[chan<- uint64]struct{}),
	}

	// init ledger
	err := l.init()
	if err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Ledger) init() error {
	// prepare stat
	stat := qis.Stat{
		Prefix: []byte(l.config.Prefix),
	}

	// execute stat
	err := l.machine.Execute(&stat)
	if err != nil {
		return err
	}

	// set values
	l.head = stat.Head
	l.tail = stat.Tail

	// fill cache
	if l.cache != nil {
		// prepare read
		read := qis.Read{
			Prefix: []byte(l.config.Prefix),
			Limit:  100,
		}

		// prepare start
		read.Start = l.tail
		if int(l.head-read.Start) > l.config.Cache {
			read.Start = l.head - uint64(l.config.Cache)
		}

		// read entries
		for read.Start < l.head {
			// execute read
			err = l.machine.Execute(&read)
			if err != nil {
				return err
			}

			// add entries
			for i, entry := range read.Entries {
				l.cache.Push(Entry{
					Sequence: read.Start + uint64(i),
					Payload:  entry,
				})
			}

			// increment
			read.Start += uint64(len(read.Entries))
		}
	}

	// create observer
	l.observer = &ledgerObserver{ledger: l}

	// subscribe ledger observer
	l.machine.Subscribe(l.observer)

	return nil
}

// Write will write the specified entries to the ledger. No entries have been
// written if an error has been returned.
func (l *Ledger) Write(entries ...Entry) error {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// check limit if available
	if l.config.Limit > 0 && int(l.head-l.tail)+len(entries) > l.config.Limit {
		return ErrLimitReached
	}

	// prepare write
	write := qis.Write{
		Prefix:  []byte(l.config.Prefix),
		Entries: make([][]byte, 0, len(entries)),
	}

	// append entries
	for _, entry := range entries {
		write.Entries = append(write.Entries, entry.Payload)
	}

	// execute write
	err := l.machine.Execute(&write)
	if err != nil {
		return err
	}

	// TODO: Write sequence to entries?

	return nil
}

// Read will read entries from and including the specified sequence up to the
// requested limit of entries.
func (l *Ledger) Read(sequence uint64, limit int) ([]Entry, error) {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// prepare list
	list := make([]Entry, 0, limit)

	// attempt to read from cache if available
	if l.cache != nil {
		// prepare counter
		i := 0

		// iterate through buffer
		l.cache.Scan(func(entry Entry) bool {
			// stop if start sequence is not in cache
			if i == 0 && sequence < entry.Sequence {
				return false
			}

			// otherwise add item if in range and stop if list is full
			if entry.Sequence >= sequence {
				list = append(list, entry)
				if len(list) >= limit {
					return false
				}
			}

			// increment counter
			i++

			return true
		})
	}

	// return cached entries immediately
	if len(list) > 0 {
		return list, nil
	}

	// prepare read
	read := qis.Read{
		Prefix: []byte(l.config.Prefix),
		Start:  sequence,
		Limit:  uint16(limit),
	}

	// execute read
	err := l.machine.Execute(&read)
	if err != nil {
		return nil, err
	}

	// append entries
	for i, entry := range read.Entries {
		list = append(list, Entry{
			Sequence: read.Start + uint64(i),
			Payload:  entry,
		})
	}

	return list, nil
}

// Index will return the sequence of the specified index in the ledger. Negative
// indexes are counted backwards from the head. If the index exceeds the current
// length, the sequence of the last entry and false is returned. If the ledger
// is empty zero and false will be returned.
func (l *Ledger) Index(index int) (uint64, bool, error) {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// compute direction
	backward := index < 0

	// make absolute if backward
	if backward {
		index *= -1
		index--
	}

	// check empty
	if l.head-l.tail == 0 {
		return 0, false, nil
	}

	// check length
	if index >= int(l.head-l.tail) {
		if backward {
			return l.tail + 1, false, nil
		}
		return l.head, false, nil
	}

	// compute sequence
	seq := l.tail + 1 + uint64(index)
	if backward {
		seq = l.head - uint64(index)
	}

	return seq, true, nil
}

// Delete will remove all entries up to and including the specified sequence
// from the ledger. It will return the number of deleted entries.
func (l *Ledger) Delete(sequence uint64) (int, error) {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// never delete beyond the current head
	if sequence > l.head {
		sequence = l.head
	}

	// skip if ledger is empty or sequence is at or behind tail
	if l.head-l.tail == 0 || sequence <= l.tail {
		return 0, nil
	}

	// prepare trim
	trim := qis.Trim{
		Prefix:   []byte(l.config.Prefix),
		Sequence: sequence,
	}

	// execute trim
	err := l.machine.Execute(&trim)
	if err != nil {
		return 0, err
	}

	return int(trim.Deleted), nil
}

// Length will return the number of stored entries.
func (l *Ledger) Length() int {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return int(l.head - l.tail)
}

// Head will return the last committed sequence. This value can be checked
// periodically to asses whether new entries have been added.
func (l *Ledger) Head() uint64 {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.head
}

// Tail will return the last deleted sequence. This value can be checked
// periodically to asses whether entries haven been deleted.
func (l *Ledger) Tail() uint64 {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.tail
}

// Subscribe will subscribe the specified channel to changes to the last
// sequence stored in the ledger. Notifications will be skipped if the specified
// channel is not writable for some reason.
func (l *Ledger) Subscribe(receiver chan<- uint64) {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// store receiver
	l.receivers[receiver] = struct{}{}
}

// Unsubscribe will remove a previously subscribed receiver.
func (l *Ledger) Unsubscribe(receiver chan<- uint64) {
	// acquire mutex
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// delete receiver
	delete(l.receivers, receiver)
}

func (l *Ledger) onPush(write *qis.Write) {
	// cache entries if available
	if l.cache != nil {
		for i, entry := range write.Entries {
			l.cache.Push(Entry{
				Sequence: write.Head - uint64(len(write.Entries)-(i+1)),
				Payload:  entry,
			})
		}
	}

	// update head
	l.head = write.Head

	// send notifications to all receivers and skip full receivers
	for receiver := range l.receivers {
		select {
		case receiver <- l.head:
		default:
		}
	}
}

func (l *Ledger) onDelete(trim *qis.Trim) {
	// remove deleted entries from cache
	if l.cache != nil {
		l.cache.Trim(func(entry Entry) bool {
			return entry.Sequence <= trim.Sequence
		})
	}

	// set tail
	l.tail = trim.Sequence
}

type ledgerObserver struct {
	ledger *Ledger
}

func (l *ledgerObserver) Init() {
	// TODO: Reset?
}

func (l *ledgerObserver) Process(ins turing.Instruction) bool {
	// process instruction
	switch ins := ins.(type) {
	case *qis.Write:
		l.ledger.onPush(ins)
	case *qis.Trim:
		l.ledger.onDelete(ins)
	}

	return true
}
