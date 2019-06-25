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

	// The delay between cleanings.
	Delay time.Duration

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
		case <-time.After(c.config.Delay):
		case <-c.tomb.Dying():
			return tomb.ErrDying
		}

		// skip if ledger is too small
		if c.config.MinRetention > 0 && c.ledger.Length() <= c.config.MinRetention {
			continue
		}

		// set current head as delete position
		deletePosition := c.ledger.Head()

		// honor minimal retention position if configured
		if c.config.MinRetention > 0 {
			// get minimal retention position
			minPosition, err := c.ledger.Index(-(c.config.MinRetention + 1))
			if err != nil {
				select {
				case c.config.Errors <- err:
				default:
				}

				return err
			}

			// set to minimal position if valid
			if minPosition > 0 {
				deletePosition = minPosition
			}
		}

		// honor lowest table positions
		for _, table := range c.config.Tables {
			// get lowest position
			lowestPosition, _, err := table.Range()
			if err != nil {
				select {
				case c.config.Errors <- err:
				default:
				}

				return err
			}

			// set to lowest position if valid
			if lowestPosition > 0 && deletePosition > lowestPosition {
				deletePosition = lowestPosition
			}
		}

		// honor lowest matrix positions
		for _, matrix := range c.config.Matrices {
			// get lowest position
			lowestPosition, _, err := matrix.Range()
			if err != nil {
				select {
				case c.config.Errors <- err:
				default:
				}

				return err
			}

			// set to lowest position if valid
			if lowestPosition > 0 && deletePosition > lowestPosition {
				deletePosition = lowestPosition
			}
		}

		// honor max retention if configured and position has been changed
		if c.config.MaxRetention > 0 {
			// get maximal retention position
			maxPosition, err := c.ledger.Index(-(c.config.MaxRetention + 1))
			if err != nil {
				select {
				case c.config.Errors <- err:
				default:
				}

				return err
			}

			// set to highest position if valid
			if maxPosition > 0 && deletePosition < maxPosition {
				deletePosition = maxPosition
			}
		}

		// delete entries
		err := c.ledger.Delete(deletePosition)
		if err != nil {
			select {
			case c.config.Errors <- err:
			default:
			}

			return err
		}
	}
}
