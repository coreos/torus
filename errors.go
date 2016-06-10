package torus

import "errors"

var (
	// ErrBlockUnavailable is returned when a function fails to retrieve a known
	// block.
	ErrBlockUnavailable = errors.New("torus: block cannot be retrieved")

	// ErrINodeUnavailable is returned when a function fails to retrieve a known
	// INode.
	ErrINodeUnavailable = errors.New("torus: inode cannot be retrieved")

	// ErrBlockNotExist is returned when a function attempts to manipulate a
	// non-existent block.
	ErrBlockNotExist = errors.New("torus: block doesn't exist")

	// ErrClosed is returned when a function attempts to manipulate a Store
	// that is not currently open.
	ErrClosed = errors.New("torus: store is closed")

	// ErrInvalid is a locally invalid operation (such as Close()ing a nil file pointer)
	ErrInvalid = errors.New("torus: invalid operation")

	// ErrOutOfSpace is returned when the block storage is out of space.
	ErrOutOfSpace = errors.New("torus: out of space on block store")

	// ErrExists is returned if the entity already exists
	ErrExists = errors.New("torus: already exists")

	// ErrNotExist is returned if the entity doesn't already exist
	ErrNotExist = errors.New("torus: doesn't exist")

	// ErrAgain is returned if the operation was interrupted. The call was valid, and
	// may be tried again.
	ErrAgain = errors.New("torus: interrupted, try again")

	// ErrNoGlobalMetadata is returned if the metadata service hasn't been formatted.
	ErrNoGlobalMetadata = errors.New("torus: no global metadata available at mds")

	// ErrNonSequentialRing is returned if the ring's internal version number appears to jump.
	ErrNonSequentialRing = errors.New("torus: non-sequential ring")

	// ErrNoPeer is returned if the peer can't be found.
	ErrNoPeer = errors.New("torus: no such peer")

	// ErrCompareFailed is returned if the CAS operation failed to compare.
	ErrCompareFailed = errors.New("torus: compare failed")

	// ErrIsSymlink is returned if we're trying to modify a symlink incorrectly.
	ErrIsSymlink = errors.New("torus: is symlink")

	// ErrNotDir is returned if we're trying a directory operation on a non-directory path.
	ErrNotDir = errors.New("torus: not a directory")

	// ErrWrongVolumeType is returned if the operation cannot be performed on this type of volume.
	ErrWrongVolumeType = errors.New("torus: wrong volume type")

	// ErrNotSupported is returned if the interface doesn't implement the
	// requested subfunctionality.
	ErrNotSupported = errors.New("torus: not supported")

	// ErrLocked is returned if the resource is locked.
	ErrLocked = errors.New("torus: locked")
)
