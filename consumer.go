package quasar

import (
	"errors"
	"sort"

	"gopkg.in/tomb.v2"
)

// ErrInvalidSequence is returned if Ack() is called with a sequences that has
// not yet been processed by the consumer.
var ErrInvalidSequence = errors.New("invalid sequence")

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
	marks  chan uint64
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
		marks:  make(chan uint64, config.Window),
	}

	// run worker
	c.tomb.Go(c.worker)

	return c
}

// Ack will acknowledge the consumption of the specified sequence.
func (c *Consumer) Ack(sequence uint64) {
	select {
	case c.marks <- sequence:
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
	storedMarkers := map[uint64]bool{}

	// check if persistent
	if c.table != nil {
		// fetch stored sequences
		storedSequences, err := c.table.Get(c.config.Name)
		if err != nil {
			select {
			case c.config.Errors <- err:
			default:
			}

			return err
		}

		// set start to first sequence if available
		if len(storedSequences) > 0 {
			c.start = storedSequences[0]
		}

		// set stored markers
		for _, seq := range storedSequences {
			storedMarkers[seq] = true
		}

		// store initial start sequence in table
		if storedSequences == nil {
			err := c.table.Set(c.config.Name, []uint64{c.start})
			if err != nil {
				return err
			}
		}
	}

	// run reader
	c.tomb.Go(c.reader)

	// prepare markers
	markers := map[uint64]bool{}

	// prepare buffer
	var buffer []Entry

	// prepare skipped
	var skipped int

	for {
		// check if closed
		if !c.tomb.Alive() {
			// store potentially uncommitted sequences if skip is enabled
			if c.table != nil && c.config.Skip > 0 {
				// compile markers
				list := compileAndCompressMarkers(markers)

				// store sequences in table
				err := c.table.Set(c.config.Name, list)
				if err != nil {
					return err
				}
			}

			return tomb.ErrDying
		}

		// prepare dynamic pipe
		var dynPipe <-chan Entry

		// check if window is not full yet and
		if len(markers) <= c.config.Window {
			dynPipe = c.pipe
		}

		// prepare dynamic queue and entry
		var dynQueue chan<- Entry
		var dynEntry Entry

		// only queue an entry if one is available
		if len(buffer) > 0 {
			dynQueue = c.config.Entries
			dynEntry = buffer[0]
		}

		// receive entry, queue entry or receive mark
		select {
		case entry := <-dynPipe:
			// skip already processed entry
			if ok, _ := markers[entry.Sequence]; ok {
				continue
			}

			// skip entry processed in old session
			if ok, _ := storedMarkers[entry.Sequence]; ok {
				continue
			}

			// set marker if not temporary
			if c.table != nil {
				markers[entry.Sequence] = false
			}

			// add entry
			buffer = append(buffer, entry)
		case dynQueue <- dynEntry:
			// remove entry from queue
			buffer = buffer[1:]
		case sequence := <-c.marks:
			// ignore if temporary
			if c.table == nil {
				continue
			}

			// check sequence
			_, ok := markers[sequence]
			if !ok {
				select {
				case c.config.Errors <- ErrInvalidSequence:
				default:
				}

				return ErrInvalidSequence
			}

			// mark sequence
			markers[sequence] = true

			// handle skipping
			if c.config.Skip > 0 {
				// increment counter
				skipped++

				// return immediately when skipped
				if skipped <= c.config.Skip {
					break
				}

				// otherwise reset counter
				skipped = 0
			}

			// compile markers
			list := compileAndCompressMarkers(markers)

			// store sequences in table
			err := c.table.Set(c.config.Name, list)
			if err != nil {
				select {
				case c.config.Errors <- err:
				default:
				}

				return err
			}
		case <-c.tomb.Dying():
		}
	}
}

func compileAndCompressMarkers(markers map[uint64]bool) []uint64 {
	// compile list
	var list []uint64
	for seq, ok := range markers {
		if ok {
			list = append(list, seq)
		}
	}

	// sort sequences
	sort.Slice(list, func(i, j int) bool {
		return list[i] < list[j]
	})

	// compact list and markers
	for {
		// stop if less than 2
		if len(list) < 2 {
			break
		}

		// stop if no positives at front
		if !markers[list[0]] || !markers[list[1]] {
			break
		}

		// remove first positive
		delete(markers, list[0])
		list = list[1:]
	}

	return list
}
