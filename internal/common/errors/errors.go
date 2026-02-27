package dberrors

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrNotLeader            ErrorCode = "ERR_NOT_LEADER"
	ErrStaleTerm            ErrorCode = "ERR_STALE_TERM"
	ErrTimeout              ErrorCode = "ERR_TIMEOUT"
	ErrConflict             ErrorCode = "ERR_CONFLICT"
	ErrRetryableUnavailable ErrorCode = "ERR_RETRYABLE_UNAVAILABLE"
	ErrInvalidArgument      ErrorCode = "ERR_INVALID_ARGUMENT"
	ErrIdempotencyConflict  ErrorCode = "ERR_IDEMPOTENCY_CONFLICT"
	ErrInternal             ErrorCode = "ERR_INTERNAL"
	ErrInternalUnmapped     ErrorCode = "ERR_INTERNAL_UNMAPPED"
)

type DBError struct {
	Code      ErrorCode
	Message   string
	Retryable bool
	Cause     error
}

func (e DBError) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
}

func (e DBError) Unwrap() error {
	return e.Cause
}

func New(code ErrorCode, msg string, retryable bool, cause error) DBError {
	return DBError{Code: code, Message: msg, Retryable: retryable, Cause: cause}
}

func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var dbErr DBError
	if errors.As(err, &dbErr) {
		return dbErr.Retryable
	}
	return false
}

func NormalizeError(err error) DBError {
	if err == nil {
		return DBError{Code: ErrInternal, Message: "nil error", Retryable: false}
	}
	var dbErr DBError
	if errors.As(err, &dbErr) {
		return dbErr
	}
	return DBError{
		Code:      ErrInternalUnmapped,
		Message:   "unmapped internal error",
		Retryable: false,
		Cause:     err,
	}
}
