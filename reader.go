package quasar

import (
	"gopkg.in/tomb.v2"
)

// ReaderConfig are used to configure a reader.
type ReaderConfig struct {
	// The start position of the reader.
	Start uint64

	// The amount of entries to fetch from the ledger at once.
	Batch int

	// The channel on which entries are sent.
	Entries chan<- Entry

	// The channel on which errors are sent.
	Errors chan<- error
}

// Reader manages consuming messages of a ledger.
type Reader struct {
	ledger *Ledger
	config ReaderConfig
	tomb   tomb.Tomb
}

// NewReader will create and return a new reader.
func NewReader(ledger *Ledger, config ReaderConfig) *Reader {
	// prepare reader
	r := &Reader{
		ledger: ledger,
		config: config,
	}

	// run worker
	r.tomb.Go(r.worker)

	return r
}

// Close will close the consumer.
func (r *Reader) Close() {
	r.tomb.Kill(nil)
	_ = r.tomb.Wait()
}

func (r *Reader) worker() error {
	// subscribe to notifications
	notifications := make(chan uint64, 1)
	r.ledger.Subscribe(notifications)
	defer r.ledger.Unsubscribe(notifications)

	// set initial position
	position := r.config.Start

	for {
		// check if closed
		select {
		case <-r.tomb.Dying():
			return tomb.ErrDying
		default:
		}

		// wait for notification if no new data in ledger
		if r.ledger.Head() <= position {
			select {
			case <-notifications:
			case <-r.tomb.Dying():
				return tomb.ErrDying
			}

			continue
		}

		// read entries
		entries, err := r.ledger.Read(position, r.config.Batch)
		if err != nil {
			select {
			case r.config.Errors <- err:
			default:
			}

			return err
		}

		// put entries on pipe
		for _, entry := range entries {
			select {
			case r.config.Entries <- entry:
				position = entry.Sequence + 1
			case <-r.tomb.Dying():
				return tomb.ErrDying
			}
		}
	}
}
