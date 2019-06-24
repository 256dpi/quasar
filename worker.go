package quasar

import (
	"sort"

	"gopkg.in/tomb.v2"
)

// WorkerConfig are used to configure a worker.
type WorkerConfig struct {
	// The name of the worker.
	Name string

	// The amount of entries to fetch from the ledger at once.
	Batch int

	// The maximal size of the unprocessed sequence range.
	Window int

	// The number of acks to skip before a new one is written.
	Skip int

	// The channel on which entries are sent.
	Entries chan<- Entry

	// The channel on which errors are sent.
	Errors chan<- error
}

// Worker manages consuming messages of a ledger using a sequence map.
type Worker struct {
	ledger *Ledger
	matrix *Matrix
	config WorkerConfig
	marks  chan uint64
	tomb   tomb.Tomb
}

// NewWorker will create and return a new worker.
func NewWorker(ledger *Ledger, matrix *Matrix, config WorkerConfig) *Worker {
	// prepare workers
	c := &Worker{
		ledger: ledger,
		matrix: matrix,
		config: config,
		marks:  make(chan uint64, config.Window),
	}

	// run worker
	c.tomb.Go(c.worker)

	return c
}

// Ack will acknowledge the consumption of the specified sequence.
func (w *Worker) Ack(sequence uint64) {
	select {
	case w.marks <- sequence:
	case <-w.tomb.Dying():
	}
}

// Close will close the worker.
func (w *Worker) Close() {
	w.tomb.Kill(nil)
	_ = w.tomb.Wait()
}

func (w *Worker) worker() error {
	// subscribe to notifications
	notifications := make(chan uint64, 1)
	w.ledger.Subscribe(notifications)
	defer w.ledger.Unsubscribe(notifications)

	// fetch stored sequences
	sequences, err := w.matrix.Get(w.config.Name)
	if err != nil {
		select {
		case w.config.Errors <- err:
		default:
		}

		return err
	}

	// prepare markers
	markers := map[uint64]bool{}

	// prepare head
	var head uint64

	// prepare queue
	var queue []Entry

	// check loaded sequences
	if len(sequences) > 0 {
		// set initial head
		head = sequences[0]

		// apply processed entries
		for _, seq := range sequences {
			markers[seq] = true
		}

		// load all unprocessed entries
		for {
			// load a batch of unprocessed entries
			entries, err := w.ledger.Read(head, w.config.Batch)
			if err != nil {
				select {
				case w.config.Errors <- err:
				default:
				}

				return err
			}

			// add unprocessed entries
			for _, entry := range entries {
				// set head
				head = entry.Sequence

				// add if not already processed
				if !markers[entry.Sequence] {
					markers[entry.Sequence] = false
					queue = append(queue, entry)
				}
			}

			// stop if all entries have been loaded
			if head >= sequences[len(sequences)-1] {
				break
			}
		}
	}

	// prepare skipped
	var skipped int

	for {
		// check if closed
		select {
		case <-w.tomb.Dying():
			return tomb.ErrDying
		default:
		}

		// check if there is space for another batch
		if len(markers) < w.config.Window-w.config.Batch {
			// load more entries
			entries, err := w.ledger.Read(head+1, w.config.Batch)
			if err != nil {
				select {
				case w.config.Errors <- err:
				default:
				}

				return err
			}

			// add entries
			for _, entry := range entries {
				head = entry.Sequence
				markers[entry.Sequence] = false
				queue = append(queue, entry)
			}
		}

		// prepare dynamic queue and entry
		var dynQueue chan<- Entry
		var dynEntry Entry

		// only queue an entry if one is available
		if len(queue) > 0 {
			dynQueue = w.config.Entries
			dynEntry = queue[0]
		}

		// queue entry, receive mark or receive notification
		select {
		case dynQueue <- dynEntry:
			// remove entry from queue
			queue = queue[1:]
		case sequence := <-w.marks:
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

			// store sequences in matrix
			err := w.matrix.Set(w.config.Name, list)
			if err != nil {
				select {
				case w.config.Errors <- err:
				default:
				}

				return err
			}
		case <-notifications:
		case <-w.tomb.Dying():
			return tomb.ErrDying
		}
	}
}
