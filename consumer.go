package quasar

import (
	"errors"

	"gopkg.in/tomb.v2"
)

// ErrInvalidSequence is returned if Mark() is called with a sequences that has
// not yet been processed by the consumer.
var ErrInvalidSequence = errors.New("invalid sequence")

type markTuple struct {
	seq uint64
	cum bool
	ack func(error)
}

// ConsumerConfig are used to configure a consumer.
type ConsumerConfig struct {
	// The name of the consumer.
	Name string

	// The start position of the consumer if not recovered from the table.
	Start uint64

	// The channel on which entries are sent.
	Entries chan<- Entry

	// The channel on which errors are sent.
	Errors chan<- error

	// The amount of entries to fetch from the ledger at once.
	Batch int

	// The maximal size of the unprocessed sequence range.
	Window int

	// The number of acks to skip before sequences are written to the table.
	Skip int
}

// Consumer manages consuming messages of a ledger using a sequence map.
type Consumer struct {
	ledger *Ledger
	table  *Table
	config ConsumerConfig
	start  uint64
	pipe   chan Entry
	marks  chan markTuple
	tomb   tomb.Tomb
}

// NewConsumer will create and return a new consumer. If table is given, the
// consumer will persists processed sequences according to the provided
// configuration.
func NewConsumer(ledger *Ledger, table *Table, config ConsumerConfig) *Consumer {
	// check name
	if table != nil && config.Name == "" {
		panic("quasar: missing name")
	}

	// check entries channel
	if config.Entries == nil {
		panic("quasar: missing entries channel")
	}

	// check errors channel
	if config.Errors == nil {
		panic("quasar: missing errors channel")
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

	// run worker
	c.tomb.Go(c.worker)

	return c
}

// Mark will acknowledge and mark the consumption of the specified sequence. The
// specified callback is called with the result of the processed mark. Skipped
// marks will have their callback called right away.
func (c *Consumer) Mark(sequence uint64, cumulative bool, ack func(error)) {
	select {
	case c.marks <- markTuple{
		seq: sequence,
		cum: cumulative,
		ack: ack,
	}:
	case <-c.tomb.Dying():
	}
}

// Close will close the consumer.
func (c *Consumer) Close() {
	c.tomb.Kill(nil)
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
			select {
			case c.config.Errors <- err:
			default:
			}

			return err
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
	// prepare stored markers
	var storedMarkers []uint64

	// check if persistent
	if c.table != nil {
		// fetch stored markers
		markers, err := c.table.Get(c.config.Name)
		if err != nil {
			select {
			case c.config.Errors <- err:
			default:
			}

			return err
		}

		// check if markers haven been recovered
		if len(markers) > 0 {
			// set start to first sequence
			c.start = markers[0] + 1
		} else {
			// store provided initial start sequence in table
			err := c.table.Set(c.config.Name, []uint64{c.start})
			if err != nil {
				select {
				case c.config.Errors <- err:
				default:
				}

				return err
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
	var skipped int

	// prepare first flag
	first := true

	for {
		// check if closed
		if !c.tomb.Alive() {
			// store potentially uncommitted markers if skip is enabled
			if c.table != nil && c.config.Skip > 0 {
				// compile markers
				list := CompileSequences(markers)

				// store markers in table
				err := c.table.Set(c.config.Name, list)
				if err != nil {
					select {
					case c.config.Errors <- err:
					default:
					}

					return err
				}
			}

			return tomb.ErrDying
		}

		// prepare dynamic pipe
		var dynPipe <-chan Entry

		// only receive entry if buffer is not full yet
		if buffer.Length() < c.config.Batch {
			dynPipe = c.pipe
		}

		// prepare dynamic queue and entry
		var dynQueue chan<- Entry
		var dynEntry Entry

		// only queue an entry if one is available
		if buffer.Length() > 0 {
			dynQueue = c.config.Entries
			buffer.Scan(func(entry Entry) bool {
				dynEntry = entry
				return false
			})
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
				// push error
				select {
				case c.config.Errors <- ErrInvalidSequence:
				default:
				}

				// call ack
				if tuple.ack != nil {
					tuple.ack(ErrInvalidSequence)
				}

				return ErrInvalidSequence
			}

			// check if mark is cumulative
			if tuple.cum {
				// mark all sequences
				for seq := range markers {
					if seq <= tuple.seq {
						markers[seq] = true
					}
				}
			} else {
				// mark single sequence
				markers[tuple.seq] = true
			}

			// handle skipping
			if c.config.Skip > 0 {
				// increment counter
				skipped++

				// return immediately when skipped
				if skipped <= c.config.Skip {
					// call ack
					if tuple.ack != nil {
						tuple.ack(nil)
					}

					continue
				}

				// otherwise reset counter
				skipped = 0
			}

			// compile markers
			list := CompileSequences(markers)

			// store markers in table
			err := c.table.Set(c.config.Name, list)
			if err != nil {
				// push error
				select {
				case c.config.Errors <- err:
				default:
				}

				// call ack
				if tuple.ack != nil {
					tuple.ack(err)
				}

				return err
			}

			// compress markers
			for seq := range markers {
				if seq < list[0] {
					delete(markers, seq)
				}
			}

			// call ack
			if tuple.ack != nil {
				tuple.ack(nil)
			}
		case <-c.tomb.Dying():
		}
	}
}
