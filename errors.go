package agro

import "errors"

var (
	ErrBlockUnavailable = errors.New("agro: block cannot be retrieved")
	ErrBlockNotExist    = errors.New("agro: block doesn't exist")
)
