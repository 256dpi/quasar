package quasar

import (
	"time"

	"gopkg.in/tomb.v2"
)

// CleanerConfig are used to configure a cleaner.
type CleanerConfig struct {
	// The point to which entries are kept in any case.
	Retention int

	// The point after which entries are dropped no matter what.
	Threshold int

	// The interval of cleanings.
	Interval time.Duration

	// The matrices to check for minimal positions.
	Tables []*Table

	// The channel on which errors are sent.
	Errors chan<- error
}

// Cleaner will periodically delete entries from a ledger honoring the configured
// retention and threshold as well as positions from the specified tables. Failed
// cleanings are retried and the errors are sent on the supplied channel.
type Cleaner struct {
	ledger *Ledger
	config CleanerConfig

	tomb tomb.Tomb
}

// NewCleaner will create and return a new cleaner.
func NewCleaner(ledger *Ledger, config CleanerConfig) *Cleaner {
	// check interval
	if config.Interval <= 0 {
		panic("quasar: missing interval")
	}

	// prepare consumers
	c := &Cleaner{
		ledger: ledger,
		config: config,
	}

	// run worker
	c.tomb.Go(c.worker)

	return c
}

// Close will close the cleaner.
func (c *Cleaner) Close() {
	c.tomb.Kill(nil)
	_ = c.tomb.Wait()
}

func (c *Cleaner) worker() error {
	for {
		// wait for trigger or close
		select {
		case <-time.After(c.config.Interval):
		case <-c.tomb.Dying():
			return tomb.ErrDying
		}

		// perform clean
		err := c.clean()
		if err != nil && c.config.Errors != nil {
			select {
			case c.config.Errors <- err:
			default:
			}
		}
	}
}

func (c *Cleaner) clean() error {
	// skip if ledger if empty or smaller than the retention
	if c.ledger.Length() <= c.config.Retention {
		return nil
	}

	// get initial position honoring the retention
	position, ok, err := c.ledger.Index(-(c.config.Retention + 1))
	if err != nil {
		return err
	}

	// abort clean if initial position has not been found
	if !ok {
		return nil
	}

	// prefetch threshold position. this will make sure that we properly honor
	// the positions of tables if a lot of entries are written during the cleaning
	var threshold uint64
	if c.config.Threshold > 0 {
		// get threshold position
		threshold, ok, err = c.ledger.Index(-(c.config.Threshold + 1))
		if err != nil {
			return err
		}

		// unset threshold if not found
		if !ok {
			threshold = 0
		}
	}

	// honor lowest table sequence
	for _, table := range c.config.Tables {
		// get lowest sequence in table
		tablePosition, _, err := table.Range()
		if err != nil {
			return err
		}

		// set to lowest position if valid
		if tablePosition > 0 && position > tablePosition {
			position = tablePosition
		}
	}

	// honor threshold if configured and available
	if c.config.Threshold > 0 && threshold > 0 {
		if position < threshold {
			position = threshold
		}
	}

	// delete entries up to the calculated position
	_, err = c.ledger.Delete(position)
	if err != nil {
		return err
	}

	return nil
}
