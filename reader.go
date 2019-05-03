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
func (c *Reader) Close() {
	c.once.Do(func() {
		close(c.closed)
	})
}

func (c *Reader) worker() {
	// subscribe to notifications
	notifications := make(chan uint64, 1)
	c.ledger.Subscribe(notifications)
	defer c.ledger.Unsubscribe(notifications)

	// set initial position
	position := c.opts.Start

	for {
		// check if closed
		select {
		case <-c.closed:
			return
		default:
		}

		// wait for notification if no new data in ledger
		if c.ledger.Head() <= position {
			select {
			case <-notifications:
			case <-c.closed:
				return
			}

			continue
		}

		// read entries
		entries, err := c.ledger.Read(position, c.opts.Batch)
		if err != nil {
			select {
			case c.opts.Errors <- err:
			default:
			}

			return
		}

		// put entries on pipe
		for _, entry := range entries {
			select {
			case c.opts.Entries <- entry:
				position = entry.Sequence + 1
			case <-c.closed:
				return
			}
		}
	}
}
