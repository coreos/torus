package agro

import "errors"

var (
	// ErrBlockUnavailable is returned when a function fails to retrieve a known
	// block.
	ErrBlockUnavailable = errors.New("agro: block cannot be retrieved")

	// ErrINodeUnavailable is returned when a function fails to retrieve a known
	// INode.
	ErrINodeUnavailable = errors.New("agro: inode cannot be retrieved")

	// ErrBlockNotExist is returned when a function attempts to manipulate a
	// non-existant block.
	ErrBlockNotExist = errors.New("agro: block doesn't exist")

	// ErrClosed is returned when a function attempts to manipulate a Store
	// that is not currently open.
	ErrClosed = errors.New("agro: store is closed")

	// ErrInvalid is a locally invalid operation (such as Close()ing a nil file pointer)
	ErrInvalid = errors.New("agro: invalid operation")

	// ErrOutOfSpace is returned when the block storage is out of space.
	ErrOutOfSpace = errors.New("agro: out of space on block store")

	// ErrExists is returned if the entity already exists
	ErrExists = errors.New("agro: already exists")

	// ErrAgain is returned if the operation was interrupted. The call was valid, and
	// may be tried again.
	ErrAgain = errors.New("agro: interrupted, try again")

	// ErrNoGlobalMetadata is returned if the metadata service hasn't been formatted.
	ErrNoGlobalMetadata = errors.New("agro: no global metadata available at mds")

	// ErrNonSequentialRing is returned if the ring's internal version number appears to jump.
	ErrNonSequentialRing = errors.New("agro: non-sequential ring")
)
