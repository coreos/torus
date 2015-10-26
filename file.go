package agro

import "io"

type File interface {
	io.ReadWriter
}
