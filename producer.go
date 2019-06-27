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
	Batch int

	// The timeout after an unfinished batch is written in any case.
	Timeout time.Duration

	// The number of times a failed write due to ErrLimitReached is retried.
	Retry int

	// The time after which a failed write due to ErrLimitReached is retried.
	Delay time.Duration
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
		pipe:   make(chan tuple, config.Batch),
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
	if !p.tomb.Alive() {
		return false
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
		entries := make([]Entry, 0, p.config.Batch)
		acks := make([]func(error), 0, p.config.Batch)

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
		tmt := time.After(p.config.Timeout)

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
				if len(entries) < p.config.Batch {
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
		if err == ErrLimitReached && p.config.Retry > 0 {
			// retry the specified number of times
			for i := 0; i < p.config.Retry; i++ {
				// wait for next interval or close
				select {
				case <-p.tomb.Dying():
					return tomb.ErrDying
				case <-time.After(p.config.Delay):
				}

				// attempt again to write entries
				err = p.ledger.Write(entries...)

				// break if write succeeded
				if err == nil {
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
