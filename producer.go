package quasar

import (
	"sync"
	"time"

	"gopkg.in/tomb.v2"
)

type tuple struct {
	entry Entry
	ack   func(error)
}

// ProducerConfig are used to configure a producer.
type ProducerConfig struct {
	// The maximum size of the written entry batches.
	BatchSize int

	// The timeout after an unfinished batch is written in any case.
	BatchTimeout time.Duration

	// The time for which a failed write due to ErrLimitReached is retried.
	RetryTimeout time.Duration

	// The interval at which a failed write due to ErrLimitReached is retried.
	RetryInterval time.Duration
}

// Producer provides an interface to efficiently batch entries and write them
// to a ledger.
type Producer struct {
	ledger *Ledger
	config ProducerConfig
	pipe   chan tuple
	mutex  sync.RWMutex
	once   sync.Once
	tomb   tomb.Tomb
}

// NewProducer will create and return a producer.
func NewProducer(ledger *Ledger, config ProducerConfig) *Producer {
	// set default
	if config.BatchSize <= 0 {
		config.BatchSize = 1
	}

	// check interval
	if config.RetryTimeout > 0 && config.RetryInterval <= 0 {
		panic("quasar: missing retry interval")
	}

	// prepare producer
	p := &Producer{
		ledger: ledger,
		config: config,
		pipe:   make(chan tuple, config.BatchSize),
	}

	// run worker
	p.tomb.Go(p.worker)

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

func (p *Producer) worker() error {
	for {
		// prepare entries and acks
		entries := make([]Entry, 0, p.config.BatchSize)
		acks := make([]func(error), 0, p.config.BatchSize)

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
		tmt := time.After(p.config.BatchTimeout)

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
				if len(entries) < p.config.BatchSize {
					continue
				}
			case <-tmt:
			}

			// exit loop
			break
		}

		// write entries
		err := p.ledger.Write(entries...)

		// handle retries
		if err == ErrLimitReached && p.config.RetryTimeout > 0 {
			// calculate deadline
			deadline := time.Now().Add(p.config.RetryTimeout)

			// retry as long as limit has been reached
			for err == ErrLimitReached {
				// wait for next interval or close
				select {
				case <-p.tomb.Dying():
					return tomb.ErrDying
				case <-time.After(p.config.RetryInterval):
				}

				// attempt again to write entries
				err = p.ledger.Write(entries...)

				// break if write succeeded
				if err == nil {
					break
				}

				// break if deadline has been reached
				if time.Now().After(deadline) {
					break
				}
			}
		}

		// call acks
		for _, ack := range acks {
			if ack != nil {
				ack(err)
			}
		}
	}
}
