package quasar

import (
	"sync"

	"gopkg.in/tomb.v2"
)

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

	// The number of acks to skip before a new one is written.
	Skip int
}

// Consumer manages consuming messages of a ledger using a persistent position.
type Consumer struct {
	ledger *Ledger
	table  *Table
	config ConsumerConfig

	head    uint64
	skipped int
	mutex   sync.Mutex

	tomb tomb.Tomb
}

// NewConsumer will create and return a new consumer.
func NewConsumer(ledger *Ledger, table *Table, config ConsumerConfig) *Consumer {
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

	// prepare consumer
	c := &Consumer{
		ledger: ledger,
		table:  table,
		config: config,
	}

	// run worker
	c.tomb.Go(c.worker)

	return c
}

// Ack will acknowledge the consumption of message up to the specified position.
// Positions that are lower than previously acknowledged positions are ignored.
func (c *Consumer) Ack(position uint64) error {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check if closed
	if !c.tomb.Alive() {
		return tomb.ErrDying
	}

	// return immediately if lower or equal
	if position <= c.head {
		return nil
	}

	// set new head
	c.head = position

	// handle skipping
	if c.config.Skip > 0 {
		// increment counter
		c.skipped++

		// return immediately when skipped
		if c.skipped <= c.config.Skip {
			return nil
		}

		// otherwise reset counter
		c.skipped = 0
	}

	// save position in table
	err := c.table.Set(c.config.Name, position)
	if err != nil {
		return err
	}

	return nil
}

// Close will close the consumer.
func (c *Consumer) Close() {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// close worker
	c.tomb.Kill(nil)
	_ = c.tomb.Wait()

	// try to save potentially uncommitted head if skip is enabled
	if c.config.Skip > 0 {
		_ = c.table.Set(c.config.Name, c.head)
	}
}

func (c *Consumer) worker() error {
	// subscribe to notifications
	notifications := make(chan uint64, 1)
	c.ledger.Subscribe(notifications)
	defer c.ledger.Unsubscribe(notifications)

	// fetch stored position
	position, err := c.table.Get(c.config.Name)
	if err != nil {
		select {
		case c.config.Errors <- err:
		default:
		}

		return err
	}

	// advance position
	position++

	for {
		// check if closed
		select {
		case <-c.tomb.Dying():
			return tomb.ErrDying
		default:
		}

		// wait for notification if no new data in ledger
		if c.ledger.Head() < position {
			select {
			case <-notifications:
			case <-c.tomb.Dying():
				return tomb.ErrDying
			}

			continue
		}

		// read entries
		entries, err := c.ledger.Read(position, c.config.Batch)
		if err != nil {
			select {
			case c.config.Errors <- err:
			default:
			}

			return err
		}

		// put entries on pipe
		for _, entry := range entries {
			select {
			case c.config.Entries <- entry:
				position = entry.Sequence + 1
			case <-c.tomb.Dying():
				return tomb.ErrDying
			}
		}
	}
}
