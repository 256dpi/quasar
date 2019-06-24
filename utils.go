package quasar

import "github.com/dgraph-io/badger/v2"

const maxTransactionRetries = 10

func retryUpdate(db *badger.DB, updater func(*badger.Txn) error) error {
	for i := 0; i < maxTransactionRetries; i++ {
		err := db.Update(updater)
		if err == badger.ErrConflict {
			continue
		} else if err != nil {
			return err
		}

		return nil
	}

	return badger.ErrConflict
}
