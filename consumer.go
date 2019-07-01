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
	var storedSequences []uint64

	// check if persistent
	if c.table != nil {
		// fetch stored sequences
		sequences, err := c.table.Get(c.config.Name)
		if err != nil {
			select {
			case c.config.Errors <- err:
			default:
			}

			return err
		}

		// check sequences haven been recovered
		if len(sequences) > 0 {
			// set start to first sequence
			c.start = sequences[0]
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

		// set sequences
		storedSequences = sequences
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
			// store potentially uncommitted sequences if skip is enabled
			if c.table != nil && c.config.Skip > 0 {
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

		// receive entry, queue entry or receive mark
		select {
		case entry := <-dynPipe:
			// restore stored sequences that are newer or equal to first entry
			if first {
				for _, seq := range storedSequences {
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
