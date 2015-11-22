package agro

import "errors"

var (
	// ErrBlockUnavailable is returned when a function fails to retrieve a known
	// block.
	ErrBlockUnavailable = errors.New("agro: block cannot be retrieved")

	// ErrBlockNotExist is returned when a function attempts to manipulate a
	// non-existant block.
	ErrBlockNotExist = errors.New("agro: block doesn't exist")

	// ErrClosed is returned when a function attempts to manipulate a Store
	// that is not currently open.
	ErrClosed = errors.New("agro: store is closed")

	// ErrInvalid is a locally invalid operation (such as Close()ing a nil file pointer)
	ErrInvalid = errors.New("agro: invalid operation")
)
