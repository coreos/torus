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

	// ErrNotExist is returned if the entity doesn't already exist
	ErrNotExist = errors.New("agro: doesn't exist")

	// ErrAgain is returned if the operation was interrupted. The call was valid, and
	// may be tried again.
	ErrAgain = errors.New("agro: interrupted, try again")

	// ErrNoGlobalMetadata is returned if the metadata service hasn't been formatted.
	ErrNoGlobalMetadata = errors.New("agro: no global metadata available at mds")

	// ErrNonSequentialRing is returned if the ring's internal version number appears to jump.
	ErrNonSequentialRing = errors.New("agro: non-sequential ring")

	// ErrNoPeer is returned if the peer can't be found.
	ErrNoPeer = errors.New("agro: no such peer")

	// ErrCompareFailed is returned if the CAS operation failed to compare.
	ErrCompareFailed = errors.New("agro: compare failed")

	// ErrIsSymlink is returned if we're trying to modify a symlink incorrectly.
	ErrIsSymlink = errors.New("agro: is symlink")

	// ErrNotDir is returned if we're trying a directory operation on a non-directory path.
	ErrNotDir = errors.New("agro: not a directory")

	// ErrWrongVolumeType is returned if the operation cannot be performed on this type of volume.
	ErrWrongVolumeType = errors.New("agro: wrong volume type")

	// ErrNotSupported is returned if the interface doesn't implement the
	// requested subfuntionality.
	ErrNotSupported = errors.New("agro: not supported")
)
