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

	// The channel on which entries are sent.
	Entries chan<- Entry

	// The channel on which errors are sent.
	Errors chan<- error

	// The amount of entries to fetch from the ledger at once.
	Batch int

	// The maximal size of the unprocessed sequence range.
	Window int

	// The number of acks to skip before a new one is written.
	Skip int
}

// Consumer manages consuming messages of a ledger using a sequence map.
type Consumer struct {
	ledger *Ledger
	matrix *Matrix
	config ConsumerConfig
	start  uint64
	pipe   chan Entry
	marks  chan uint64
	tomb   tomb.Tomb
}

// NewConsumer will create and return a new consumer.
func NewConsumer(ledger *Ledger, matrix *Matrix, config ConsumerConfig) *Consumer {
	// check name
	if config.Name == "" {
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

	// prepare consumer
	c := &Consumer{
		ledger: ledger,
		matrix: matrix,
		config: config,
		pipe:   make(chan Entry, config.Batch),
		marks:  make(chan uint64, config.Window),
	}

	// run worker
	c.tomb.Go(c.worker)

	return c
}

// Ack will acknowledge the consumption of the specified sequence.
func (w *Consumer) Ack(sequence uint64) {
	select {
	case w.marks <- sequence:
	case <-w.tomb.Dying():
	}
}

// Close will close the consumer.
func (w *Consumer) Close() {
	w.tomb.Kill(nil)
	_ = w.tomb.Wait()
}

func (w *Consumer) reader() error {
	// subscribe to notifications
	notifications := make(chan uint64, 1)
	w.ledger.Subscribe(notifications)
	defer w.ledger.Unsubscribe(notifications)

	// get initial position
	position := w.start

	for {
		// check if closed
		if !w.tomb.Alive() {
			return tomb.ErrDying
		}

		// wait for notification if no new data in ledger
		if w.ledger.Head() < position {
			select {
			case <-notifications:
			case <-w.tomb.Dying():
				return tomb.ErrDying
			}

			continue
		}

		// read entries
		entries, err := w.ledger.Read(position, w.config.Batch)
		if err != nil {
			select {
			case w.config.Errors <- err:
			default:
			}

			return err
		}

		// put entries on pipe
		for _, entry := range entries {
			select {
			case w.pipe <- entry:
				position = entry.Sequence + 1
			case <-w.tomb.Dying():
				return tomb.ErrDying
			}
		}
	}
}

func (w *Consumer) worker() error {
	// fetch stored sequences
	storedSequences, err := w.matrix.Get(w.config.Name)
	if err != nil {
		select {
		case w.config.Errors <- err:
		default:
		}

		return err
	}

	// set start to first sequence if available
	if len(storedSequences) > 0 {
		w.start = storedSequences[0]
	}

	// run reader
	w.tomb.Go(w.reader)

	// compute stored markers
	storedMarkers := map[uint64]bool{}
	for _, seq := range storedSequences {
		storedMarkers[seq] = true
	}

	// unset stored sequences
	storedSequences = nil

	// prepare markers
	markers := map[uint64]bool{}

	// prepare buffer
	var buffer []Entry

	// prepare skipped
	var skipped int

	for {
		// check if closed
		if !w.tomb.Alive() {
			// store potentially uncommitted sequences if skip is enabled
			if w.config.Skip > 0 {
				// compile markers
				list := compileAndCompressMarkers(markers)

				// store sequences in matrix
				err := w.matrix.Set(w.config.Name, list)
				if err != nil {
					return err
				}
			}

			return tomb.ErrDying
		}

		// prepare dynamic pipe
		var dynPipe <-chan Entry

		// check if window is not full yet and
		if len(markers) <= w.config.Window {
			dynPipe = w.pipe
		}

		// prepare dynamic queue and entry
		var dynQueue chan<- Entry
		var dynEntry Entry

		// only queue an entry if one is available
		if len(buffer) > 0 {
			dynQueue = w.config.Entries
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

			// add entry
			markers[entry.Sequence] = false
			buffer = append(buffer, entry)
		case dynQueue <- dynEntry:
			// remove entry from queue
			buffer = buffer[1:]

			// TODO: Use circular buffer?
		case sequence := <-w.marks:
			// check sequence
			_, ok := markers[sequence]
			if !ok {
				select {
				case w.config.Errors <- ErrInvalidSequence:
				default:
				}

				return ErrInvalidSequence
			}

			// mark sequence
			markers[sequence] = true

			// handle skipping
			if w.config.Skip > 0 {
				// increment counter
				skipped++

				// return immediately when skipped
				if skipped <= w.config.Skip {
					break
				}

				// otherwise reset counter
				skipped = 0
			}

			// compile markers
			list := compileAndCompressMarkers(markers)

			// store sequences in matrix
			err := w.matrix.Set(w.config.Name, list)
			if err != nil {
				select {
				case w.config.Errors <- err:
				default:
				}

				return err
			}
		case <-w.tomb.Dying():
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
