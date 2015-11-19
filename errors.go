package agro

import "errors"

var (
	// ErrBlockUnavailable is returned when a function fails to retrieve a known
	// block.
	ErrBlockUnavailable = errors.New("agro: block cannot be retrieved")

	// ErrBlockNotExist is returned when a function attempts to manipulate a
	// non-existant block.
	ErrBlockNotExist = errors.New("agro: block doesn't exist")

	// ErrClosed is returned when a function attempts to manipulate a storage
	// that is not currently available.
	ErrClosed = errors.New("agro: store is closed")
)
