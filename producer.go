package quasar

import (
	"errors"
	"sync"
	"time"

	"gopkg.in/tomb.v2"
)

// ErrProducerClosed is yielded to callbacks if the producer has been closed.
var ErrProducerClosed = errors.New("producer closed")

type producerTuple struct {
	entry Entry
	ack   func(error)
}

// ProducerConfig is used to configure a producer.
type ProducerConfig struct {
	// The maximum size of the written entry batches.
	Batch int

	// The timeout after an unfinished batch is written in any case.
	Timeout time.Duration

	// The number of times a failed write due to ErrLimitReached is retried.
	Retry int

	// The time after which a failed write due to ErrLimitReached is retried.
	Delay time.Duration

	// If enabled, the producer will filter out entries that have a lower
	// sequence than the current ledger head.
	Filter bool
}

// Producer provides an interface to efficiently batch entries and write them
// to a ledger.
type Producer struct {
	ledger *Ledger
	config ProducerConfig
	pipe   chan producerTuple
	mutex  sync.RWMutex
	once   sync.Once
	tomb   tomb.Tomb
}

// NewProducer will create and return a producer.
func NewProducer(ledger *Ledger, config ProducerConfig) *Producer {
	// set default
	if config.Batch <= 0 {
		config.Batch = 1
	}

	// check interval
	if config.Retry > 0 && config.Delay <= 0 {
		panic("quasar: missing retry interval")
	}

	// prepare producer
	p := &Producer{
		ledger: ledger,
		config: config,
		pipe:   make(chan producerTuple, config.Batch),
	}

	// run worker
	p.tomb.Go(p.worker)

	return p
}

// Write will asynchronously write the specified message and call the provided
// callback with the result. The method returns whether the entry has been
// accepted and that the ack, if present, will be called with the result of the
// operation.
func (p *Producer) Write(entry Entry, ack func(error)) bool {
	// check if closed
	if !p.tomb.Alive() {
		return false
	}

	// create tuple
	tpl := producerTuple{
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

// Close will close the producer. Unprocessed entries will be canceled and the
// callbacks receive ErrProducerClosed if available.
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

func (p *Producer) worker() error {
	// prepare entries and acks
	entries := make([]Entry, 0, p.config.Batch)
	acks := make([]func(error), 0, p.config.Batch)

	// set cleanup handler
	defer func() {
		// cancel batched tuples
		for _, ack := range acks {
			ack(ErrProducerClosed)
		}

		// cancel queued tuples
		for tpl := range p.pipe {
			tpl.ack(ErrProducerClosed)
		}
	}()

	for {
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
		case <-p.tomb.Dying():
			return tomb.ErrDying
		}

		// prepare timeout
		timeout := time.After(p.config.Timeout)

		// get more tuples or timeout
		for {
			select {
			case tpl, ok := <-p.pipe:
				// stop if pipe has been closed
				if !ok {
					return tomb.ErrDying
				}

				// add entry and ack
				entries = append(entries, tpl.entry)
				acks = append(acks, tpl.ack)

				// continue if there is still space
				if len(entries) < p.config.Batch {
					continue
				}
			case <-timeout:
			case <-p.tomb.Dying():
				return tomb.ErrDying
			}

			// exit loop
			break
		}

		// filter out old entries if requested
		if p.config.Filter {
			// get head
			head := p.ledger.Head()

			// collect new entries
			newEntries := make([]Entry, 0, len(entries))
			for _, entry := range entries {
				if entry.Sequence > head {
					newEntries = append(newEntries, entry)
				}
			}

			// update list
			entries = newEntries
		}

		// write entries
		err := p.ledger.Write(entries...)

		// handle retries
		if err == ErrLimitReached && p.config.Retry > 0 {
			// retry the specified number of times
			for i := 0; i < p.config.Retry; i++ {
				// wait for next interval or close
				select {
				case <-time.After(p.config.Delay):
				case <-p.tomb.Dying():
					return tomb.ErrDying
				}

				// attempt again to write entries
				err = p.ledger.Write(entries...)

				// break if write succeeded or failed otherwise
				if err != ErrLimitReached {
					break
				}
			}
		}

		// call acks with result
		for _, ack := range acks {
			if ack != nil {
				ack(err)
			}
		}

		// reset lists
		entries = make([]Entry, 0, p.config.Batch)
		acks = make([]func(error), 0, p.config.Batch)
	}
}
