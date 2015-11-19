package agro

import "io"

// File is the interface that represents the standardized methods to interact
// with a file in the filesystem.
type File interface {
	io.ReadWriter
}
