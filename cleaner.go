package quasar

import (
	"time"

	"gopkg.in/tomb.v2"
)

// CleanerConfig are used to configure a cleaner.
type CleanerConfig struct {
	// The minimal amount of entries to keep.
	MinRetention int

	// The maximal amount of entries to keep.
	MaxRetention int

	// The interval of cleanings.
	Interval time.Duration

	// The tables to check for minimal positions.
	Tables []*Table

	// The matrices to check for minimal positions.
	Matrices []*Matrix

	// The channel on which errors are sent.
	Errors chan<- error
}

// Cleaner will delete entries from a ledger using different strategies.
type Cleaner struct {
	ledger *Ledger
	config CleanerConfig

	tomb tomb.Tomb
}

// NewCleaner will create and return a new cleaner.
func NewCleaner(ledger *Ledger, config CleanerConfig) *Cleaner {
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

		// skip if ledger is empty or smaller than the minimal retention
		if c.ledger.Length() <= c.config.MinRetention {
			continue
		}

		// get initial position honoring the minimal retention
		position, _, err := c.ledger.Index(-(c.config.MinRetention + 1))
		if err != nil {
			select {
			case c.config.Errors <- err:
			default:
			}

			return err
		}

		// prefetch max retention position. this will make sure that we properly
		// honor the low positions of tables and matrices if a lot of entries are
		// written in between
		var maxPosition uint64
		if c.config.MaxRetention > 0 {
			// get maximal retention position
			maxPosition, _, err = c.ledger.Index(-(c.config.MaxRetention + 1))
			if err != nil {
				select {
				case c.config.Errors <- err:
				default:
				}

				return err
			}
		}

		// honor lowest table positions
		for _, table := range c.config.Tables {
			// get lowest position in table
			lowestPosition, _, err := table.Range()
			if err != nil {
				select {
				case c.config.Errors <- err:
				default:
				}

				return err
			}

			// set to lowest position if valid
			if lowestPosition > 0 && position > lowestPosition {
				position = lowestPosition
			}
		}

		// honor lowest matrix positions
		for _, matrix := range c.config.Matrices {
			// get lowest position in matrix
			lowestPosition, _, err := matrix.Range()
			if err != nil {
				select {
				case c.config.Errors <- err:
				default:
				}

				return err
			}

			// set to lowest position if valid
			if lowestPosition > 0 && position > lowestPosition {
				position = lowestPosition
			}
		}

		// honor max retention if configured
		if c.config.MaxRetention > 0 {
			if position < maxPosition {
				position = maxPosition
			}
		}

		// delete entries
		err = c.ledger.Delete(position)
		if err != nil {
			select {
			case c.config.Errors <- err:
			default:
			}

			return err
		}
	}
}
