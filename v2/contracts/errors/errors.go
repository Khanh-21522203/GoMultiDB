package errors

import (
	"errors"
	"fmt"
)

// ErrNotImplemented is returned by scaffold stubs whose behavior is not yet
// implemented, across every GoMultiDB v2 module.
var ErrNotImplemented = errors.New("GoMultiDB/v2: not implemented")

// ErrorCode is a stable, machine-readable classification for a DBError.
// Callers should switch on Code, never on Message, which is free-form and
// may change.
type ErrorCode string

// Error code constants shared across GoMultiDB v2.
const (
	ErrNotPrimary           ErrorCode = "ERR_NOT_PRIMARY"
	ErrPrimaryOwnerChanged  ErrorCode = "ERR_PRIMARY_OWNER_CHANGED"
	ErrTimeout              ErrorCode = "ERR_TIMEOUT"
	ErrConflict             ErrorCode = "ERR_CONFLICT"
	ErrRetryableUnavailable ErrorCode = "ERR_RETRYABLE_UNAVAILABLE"
	ErrInvalidArgument      ErrorCode = "ERR_INVALID_ARGUMENT"
	ErrIdempotencyConflict  ErrorCode = "ERR_IDEMPOTENCY_CONFLICT"
	ErrInternal             ErrorCode = "ERR_INTERNAL"
	ErrInternalUnmapped     ErrorCode = "ERR_INTERNAL_UNMAPPED"
	ErrInvalidConfig        ErrorCode = "ERR_INVALID_CONFIG"
	ErrBootstrapRequired    ErrorCode = "ERR_BOOTSTRAP_REQUIRED"
	ErrTxnRestartRequired   ErrorCode = "ERR_TXN_RESTART_REQUIRED"
	ErrOOMKill              ErrorCode = "ERR_OOM_KILL"
)

// DBError is the canonical error shape returned by GoMultiDB v2 subsystems:
// a stable Code, a human-readable Message, a Retryable hint for callers
// deciding whether to retry, and an optional wrapped Cause.
type DBError struct {
	Code      ErrorCode
	Message   string
	Retryable bool
	Cause     error
}

// Error renders the DBError as "<code>: <message>", appending ": <cause>"
// when Cause is set.
func (e DBError) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
}

// Unwrap returns the wrapped Cause, allowing errors.Is and errors.As to see
// through a DBError to the underlying error, if any.
func (e DBError) Unwrap() error {
	return e.Cause
}

// New constructs a DBError from its constituent fields.
func New(code ErrorCode, msg string, retryable bool, cause error) DBError {
	return DBError{Code: code, Message: msg, Retryable: retryable, Cause: cause}
}

// IsRetryable reports whether err is a DBError (directly or wrapped)
// marked as retryable.
//
// Not yet implemented in this scaffold.
func IsRetryable(err error) bool {
	panic("not implemented")
}

// NormalizeError converts an arbitrary error into a DBError, passing an
// existing DBError through unchanged and wrapping any other error under
// ErrInternalUnmapped.
//
// Not yet implemented in this scaffold.
func NormalizeError(err error) DBError {
	return DBError{}
}
