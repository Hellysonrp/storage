package storage

import "errors"

var (
	ErrPrefixIsAnObject = errors.New("prefix is an object")
	ErrNotImplemented   = errors.New("not implemented")
	ErrNewPathNotEmpty  = errors.New("new path is not empty")
)
