package ingest

import (
	"errors"
	"fmt"
)

var (
	ErrForbiddenField = errors.New("field is not allowed")
	ErrValueTooLarge  = errors.New("value exceeds sanitizer limit")
	ErrTraceTooLarge  = errors.New("trace exceeds sanitizer limit")
	ErrInvalidTrace   = errors.New("trace is invalid")
)

type SanitizationError struct {
	Reason   error
	Location string
}

func (e *SanitizationError) Error() string {
	return fmt.Sprintf("sanitize %s: %v", e.Location, e.Reason)
}

func (e *SanitizationError) Unwrap() error {
	return e.Reason
}
