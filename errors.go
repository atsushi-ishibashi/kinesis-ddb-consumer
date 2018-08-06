package consumer

import "errors"

var (
	ErrInvalidMaxWait    = errors.New("invalid max wait")
	ErrInvalidChannelCap = errors.New("invalid channel cap")
)
