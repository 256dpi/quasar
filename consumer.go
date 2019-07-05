package quasar

import (
	"errors"
	"sync"
	"time"

	"gopkg.in/tomb.v2"
)

// ErrInvalidSequence is returned if Mark() is called with a sequences that has
// not yet been processed by the consumer.
var ErrInvalidSequence = errors.New("invalid sequence")

// ErrConsumerClosed is yielded to callbacks if the consumer has been closed.
var ErrConsumerClosed = errors.New("consumer closed")

// ErrConsumerTimeout is returned by the consumer if the specified timeout has
// been reached.
var ErrConsumerTimeout = errors.New("consumer timeout")

type markTuple struct {
	seq uint64
	cum bool
	ack func(error)
}

// ConsumerConfig is used to configure a consumer.
type ConsumerConfig struct {
	// The name of the persistent consumer.
	Name string

	// The start position of the consumer if not recovered from the table.
	Start uint64

	// The channel on which entries are sent.
	Entries chan<- Entry

	// The callback that is called with errors before the consumer dies.
	Errors func(error)

	// The amount of entries to fetch from the ledger at once.
	Batch int

	// The maximal size of the unprocessed sequence range.
	Window int

	// The number of acks to skip before sequences are written to the table.
	Skip int

	// The timeout after which the consumer crashes if it cannot make progress.
	// This can be used to protect the consumer from deadlocks if an ack has
	// been missed.
	Timeout time.Duration
}

// Consumer manages consuming messages of a ledger using a sequence map.
type Consumer struct {
	ledger *Ledger
	table  *Table
	config ConsumerConfig

	marks chan markTuple
	mutex sync.RWMutex
	once  sync.Once

	start uint64
	pipe  chan Entry
	tomb  tomb.Tomb
}

// NewConsumer will create and return a new consumer. If table is given, the
// consumer will persists processed sequences according to the provided
// configuration.
func NewConsumer(ledger *Ledger, table *Table, config ConsumerConfig) *Consumer {
	// check table
	if config.Name != "" && table == nil {
		panic("quasar: missing table")
	}

	// check entries channel
	if config.Entries == nil {
		panic("quasar: missing entries channel")
	}

	// set default batch
	if config.Batch <= 0 {
		config.Batch = 1
	}

	// set default window
	if config.Window <= 0 {
		config.Window = 1
	}

	// check skip
	if config.Skip >= config.Window {
		panic("quasar: skip bigger or equal as window")
	}

	// prepare consumer
	c := &Consumer{
		ledger: ledger,
		table:  table,
		config: config,
		start:  config.Start,
		pipe:   make(chan Entry, config.Batch),
		marks:  make(chan markTuple, config.Window),
	}

	// unset table if name is missing
	if config.Name == "" {
		c.table = nil
	}

	// run worker
	c.tomb.Go(c.worker)

	return c
}

// Mark will acknowledge and mark the consumption of the specified sequence. The
// specified callback is called with the result of the processed mark. If Skip
// is configured the callback might called later once the mark will be persisted.
// The method returns whether the mark has been successfully queued and its
// callback will be called with the result or an error if the consumer is closed.
func (c *Consumer) Mark(sequence uint64, cumulative bool, ack func(error)) bool {
	// check if closed
	if !c.tomb.Alive() {
		return false
	}

	// create tuple
	tpl := markTuple{
		seq: sequence,
		cum: cumulative,
		ack: ack,
	}

	// acquire mutex
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	select {
	case c.marks <- tpl:
		return true
	case <-c.tomb.Dying():
		return false
	}
}

// Close will close the consumer.
func (c *Consumer) Close() {
	// kill tomb
	c.tomb.Kill(nil)

	// close marks
	c.once.Do(func() {
		c.mutex.Lock()
		close(c.marks)
		c.mutex.Unlock()
	})

	// wait for exit
	_ = c.tomb.Wait()
}

func (c *Consumer) reader() error {
	// subscribe to notifications
	notifications := make(chan uint64, 1)
	c.ledger.Subscribe(notifications)
	defer c.ledger.Unsubscribe(notifications)

	// get initial position
	position := c.start

	for {
		// check if closed
		if !c.tomb.Alive() {
			return tomb.ErrDying
		}

		// wait for notification if no new data in ledger
		if c.ledger.Head() < position {
			select {
			case <-notifications:
			case <-c.tomb.Dying():
				return tomb.ErrDying
			}

			continue
		}

		// read entries
		entries, err := c.ledger.Read(position, c.config.Batch)
		if err != nil {
			return c.die(err)
		}

		// put entries on pipe
		for _, entry := range entries {
			select {
			case c.pipe <- entry:
				position = entry.Sequence + 1
			case <-c.tomb.Dying():
				return tomb.ErrDying
			}
		}
	}
}

func (c *Consumer) worker() error {
	// cancel queued marks on exit
	defer func() {
		for tpl := range c.marks {
			if tpl.ack != nil {
				tpl.ack(ErrConsumerClosed)
			}
		}
	}()

	// prepare stored markers
	var storedMarkers []uint64

	// check if persistent
	if c.table != nil {
		// fetch stored markers
		markers, err := c.table.Get(c.config.Name)
		if err != nil {
			return c.die(err)
		}

		// check if markers haven been recovered
		if len(markers) > 0 {
			// set start to first sequence
			c.start = markers[0] + 1
		} else {
			// store provided initial start sequence in table
			err := c.table.Set(c.config.Name, []uint64{c.start})
			if err != nil {
				return c.die(err)
			}
		}

		// set stored markers
		storedMarkers = markers
	}

	// run reader
	c.tomb.Go(c.reader)

	// prepare markers
	markers := map[uint64]bool{}

	// prepare buffer
	buffer := NewBuffer(c.config.Batch)

	// prepare skipped
	var skipped []func(error)

	// prepare first flag
	first := true

	defer func() {
		// store potentially uncommitted markers if skip is enabled
		if c.table != nil && len(skipped) > 0 {
			// compile markers
			list := CompileSequences(markers)

			// store markers in table
			err := c.table.Set(c.config.Name, list)

			// call acks with result
			for _, ack := range skipped {
				if ack != nil {
					ack(err)
				}
			}
		}
	}()

	for {
		// prepare dynamic pipe
		var dynPipe <-chan Entry

		// only receive entry if buffer is not full yet
		if buffer.Length() < c.config.Batch {
			dynPipe = c.pipe
		}

		// prepare dynamic queue and entry
		var dynQueue chan<- Entry
		var dynEntry Entry

		// only queue an entry if one is available and there is space
		if buffer.Length() > 0 && len(markers) < c.config.Window+1 {
			dynQueue = c.config.Entries
			buffer.Scan(func(entry Entry) bool {
				dynEntry = entry
				return false
			})
		}

		// prepare dynamic timeout
		var dynTimeout <-chan time.Time

		// set timeout if window is full and enabled
		if len(markers) >= c.config.Window+1 && c.config.Timeout > 0 {
			dynTimeout = time.After(c.config.Timeout)
		}

		// buffer entry, queue entry or handle mark
		select {
		case entry := <-dynPipe:
			// restore stored markers that are newer or equal to first entry
			if first {
				for _, seq := range storedMarkers {
					if seq >= dynEntry.Sequence {
						markers[seq] = true
					}
				}

				// reset flag
				first = false
			}

			// skip already processed entry
			if ok, _ := markers[dynEntry.Sequence]; ok {
				continue
			}

			// add entry
			buffer.Push(entry)
		case dynQueue <- dynEntry:
			// remove entry from buffer
			buffer.Trim(func(entry Entry) bool {
				return entry.Sequence <= dynEntry.Sequence
			})

			// set marker if not temporary
			if c.table != nil {
				markers[dynEntry.Sequence] = false
			}
		case tuple := <-c.marks:
			// ignore if temporary
			if c.table == nil {
				// call ack
				if tuple.ack != nil {
					tuple.ack(nil)
				}

				continue
			}

			// check sequence
			_, ok := markers[tuple.seq]
			if !ok {
				// call ack
				if tuple.ack != nil {
					tuple.ack(ErrInvalidSequence)
				}

				continue
			}

			// mark sequence
			markers[tuple.seq] = true

			// mark all lower sequences if cumulative
			if tuple.cum {
				for seq := range markers {
					if seq < tuple.seq {
						markers[seq] = true
					}
				}
			}

			// cache ack if skipping is enabled and there is space
			if c.config.Skip > 0 && len(skipped) < c.config.Skip {
				skipped = append(skipped, tuple.ack)
				continue
			}

			// compile markers
			list := CompileSequences(markers)

			// store markers in table
			err := c.table.Set(c.config.Name, list)
			if err != nil {
				// call ack
				if tuple.ack != nil {
					tuple.ack(err)
				}

				return c.die(err)
			}

			// compress markers
			for seq := range markers {
				if seq < list[0] {
					delete(markers, seq)
				}
			}

			// call cached acks
			for _, ack := range skipped {
				if ack != nil {
					ack(nil)
				}
			}

			// call acks
			if tuple.ack != nil {
				tuple.ack(nil)
			}

			// reset list
			skipped = nil
		case <-dynTimeout:
			return c.die(ErrConsumerTimeout)
		case <-c.tomb.Dying():
			return tomb.ErrDying
		}
	}
}

func (c *Consumer) die(err error) error {
	// call error callback if present and error given
	if err != nil && c.config.Errors != nil {
		c.config.Errors(err)
	}

	return err
}
