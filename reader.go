package quasar

import "sync"

// ReaderOptions are used to configure a reader.
type ReaderOptions struct {
	// The start position of the reader.
	Start uint64

	// The channel on which entries are sent.
	Entries chan<- Entry

	// The channel on which errors are sent.
	Errors chan<- error

	// The amount of entries to fetch from the ledger at once.
	Batch int
}

// Reader manages consuming messages of a ledger.
type Reader struct {
	ledger *Ledger
	opts   ReaderOptions

	once   sync.Once
	closed chan struct{}
}

// NewReader will create and return a new reader.
func NewReader(ledger *Ledger, opts ReaderOptions) *Reader {
	// prepare consumers
	c := &Reader{
		ledger: ledger,
		opts:   opts,

		closed: make(chan struct{}),
	}

	// run worker
	go c.worker()

	return c
}

// Close will close the consumer.
func (r *Reader) Close() {
	r.once.Do(func() {
		close(r.closed)
	})
}

func (r *Reader) worker() {
	// subscribe to notifications
	notifications := make(chan uint64, 1)
	r.ledger.Subscribe(notifications)
	defer r.ledger.Unsubscribe(notifications)

	// set initial position
	position := r.opts.Start

	for {
		// check if closed
		select {
		case <-r.closed:
			return
		default:
		}

		// wait for notification if no new data in ledger
		if r.ledger.Head() <= position {
			select {
			case <-notifications:
			case <-r.closed:
				return
			}

			continue
		}

		// read entries
		entries, err := r.ledger.Read(position, r.opts.Batch)
		if err != nil {
			select {
			case r.opts.Errors <- err:
			default:
			}

			return
		}

		// put entries on pipe
		for _, entry := range entries {
			select {
			case r.opts.Entries <- entry:
				position = entry.Sequence + 1
			case <-r.closed:
				return
			}
		}
	}
}
