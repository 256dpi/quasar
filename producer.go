package quasar

import (
	"sync"
	"time"

	"gopkg.in/tomb.v2"
)

var noop = func(error) {}

type tuple struct {
	entry Entry
	ack   func(error)
}

// ProducerOptions are used to configure a producer.
type ProducerOptions struct {
	// The size of the sent entry batches.
	Batch int

	// The timeout after a batch is sent in any case.
	Timeout time.Duration
}

// Producer provides an interface to efficiently batch entries and write them
// to a ledger.
type Producer struct {
	ledger *Ledger
	opts   ProducerOptions
	pipe   chan tuple
	mutex  sync.RWMutex
	once   sync.Once
	tomb   tomb.Tomb
}

// NewProducer will create and return a producer.
func NewProducer(ledger *Ledger, opts ProducerOptions) *Producer {
	// prepare producer
	p := &Producer{
		ledger: ledger,
		opts:   opts,
		pipe:   make(chan tuple, opts.Batch),
	}

	// run publisher
	p.tomb.Go(p.publisher)

	return p
}

// Write will asynchronously write the specified message and call the provided
// callback with the result. If no error is present the operation was
// successful.
func (p *Producer) Write(entry Entry, ack func(error)) bool {
	// check if closed
	select {
	case <-p.tomb.Dying():
		return false
	default:
	}

	// set noop ack if missing
	if ack == nil {
		ack = noop
	}

	// create tuple
	tpl := tuple{
		entry: entry,
		ack:   ack,
	}

	// acquire mutex
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// queue entry
	select {
	case p.pipe <- tpl:
		return true
	case <-p.tomb.Dying():
		return false
	}
}

// Close will close the producer.
func (p *Producer) Close() {
	// kill tomb
	p.tomb.Kill(nil)

	// close pipe
	p.once.Do(func() {
		p.mutex.Lock()
		close(p.pipe)
		p.mutex.Unlock()
	})

	// wait for exit
	_ = p.tomb.Wait()
}

func (p *Producer) publisher() error {
	for {
		// prepare entries and acks
		entries := make([]Entry, 0, p.opts.Batch)
		acks := make([]func(error), 0, p.opts.Batch)

		// wait for first tuple
		select {
		case tpl, ok := <-p.pipe:
			// return if pipe has been closed
			if !ok {
				return tomb.ErrDying
			}

			// add entry and ack
			entries = append(entries, tpl.entry)
			acks = append(acks, tpl.ack)
		}

		// prepare timeout
		tmt := time.After(p.opts.Timeout)

		// await next tuple or timeout
		for {
			select {
			case tpl, ok := <-p.pipe:
				// stop if pipe has been closed
				if !ok {
					break
				}

				// add entry and ack
				entries = append(entries, tpl.entry)
				acks = append(acks, tpl.ack)

				// continue if there is still space
				if len(entries) < p.opts.Batch {
					continue
				}
			case <-tmt:
			}

			// exit loop
			break
		}

		// write entries
		err := p.ledger.Write(entries...)

		// call acks
		for _, ack := range acks {
			ack(err)
		}
	}
}
