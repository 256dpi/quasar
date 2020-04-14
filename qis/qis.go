// Package qis implements the instruction set used by the quasar package.
package qis

// TODO: Add table instructions: Range.

/*
Ledger Architecture

Every ledger starts out to be empty. Its head and tail are zero and it has
therefore a zero length.

A written entry receives the sequence head+1 and is stored using its sequence
in the ledger. The head is incremented as well.

After deleting records, the tail is moved up until it matches the head, at which
point the ledger is empty again.
*/
