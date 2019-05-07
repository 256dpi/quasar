package quasar

import (
	"gopkg.in/tomb.v2"
)

// ConsumerOptions are used to configure a consumer.
type ConsumerOptions struct {
	// The name of the consumer.
	Name string

	// The amount of entries to fetch from the ledger at once.
	Batch int

	// The channel on which entries are sent.
	Entries chan<- Entry

	// The channel on which errors are sent.
	Errors chan<- error
}

// Consumer manages consuming messages of a ledger using a persistent position.
type Consumer struct {
	ledger *Ledger
	table  *Table
	opts   ConsumerOptions
	tomb   tomb.Tomb
}

// NewConsumer will create and return a new consumer.
func NewConsumer(ledger *Ledger, table *Table, opts ConsumerOptions) *Consumer {
	// prepare consumer
	c := &Consumer{
		ledger: ledger,
		table:  table,
		opts:   opts,
	}

	// run worker
	c.tomb.Go(c.worker)

	return c
}

// Ack will acknowledge the consumption of message up to the specified position.
func (c *Consumer) Ack(position uint64) error {
	// save position in table
	err := c.table.Set(c.opts.Name, position)
	if err != nil {
		return err
	}

	return nil
}

// Close will close the consumer.
func (c *Consumer) Close() {
	c.tomb.Kill(nil)
	_ = c.tomb.Wait()
}

func (c *Consumer) worker() error {
	// subscribe to notifications
	notifications := make(chan uint64, 1)
	c.ledger.Subscribe(notifications)
	defer c.ledger.Unsubscribe(notifications)

	// fetch stored position
	position, err := c.table.Get(c.opts.Name)
	if err != nil {
		select {
		case c.opts.Errors <- err:
		default:
		}

		return err
	}

	// advanced position
	position++

	for {
		// check if closed
		select {
		case <-c.tomb.Dying():
			return tomb.ErrDying
		default:
		}

		// wait for notification if no new data in ledger
		if c.ledger.Head() <= position {
			select {
			case <-notifications:
			case <-c.tomb.Dying():
				return tomb.ErrDying
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

			return err
		}

		// put entries on pipe
		for _, entry := range entries {
			select {
			case c.opts.Entries <- entry:
				position = entry.Sequence + 1
			case <-c.tomb.Dying():
				return tomb.ErrDying
			}
		}
	}
}
