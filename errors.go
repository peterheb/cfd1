package cfd1

import (
	"errors"
	"fmt"
	"strings"
)

// ErrSQLite is returned within a wrapped error if a query fails with an API
// error 7500, typically indicating an error from the SQLite engine.
var ErrSQLite = errors.New("SQLite error")

// ErrNotFound is returned from GetHandle if the requested database does not
// exist.
var ErrNotFound = errors.New("database not found")

// D1Error represents an error returned by the D1 API other than an [ErrSQLite].
type D1Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func newD1Error(code int, message string) *D1Error {
	return &D1Error{
		Code:    code,
		Message: message,
	}
}

func (e *D1Error) Error() string {
	return fmt.Sprintf("D1 API error %d: %s", e.Code, e.Message)
}

func (e *D1Error) Is(target error) bool {
	t, ok := target.(*D1Error)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// SQLiteError represents a syntax error returned when executing a query. It
// contains the error message, the query that caused the error, the query
// bindings, and the SQLite error code such as SQLITE_AUTH or SQLITE_ERROR.
type SQLiteError struct {
	Message    string
	Query      string
	Bindings   []any
	SQLiteCode string
}

func newSQLiteError(message, query string, bindings []any, sqliteCode string) *SQLiteError {
	return &SQLiteError{
		Message:    message,
		Query:      query,
		Bindings:   bindings,
		SQLiteCode: sqliteCode,
	}
}

func (e *SQLiteError) Error() string {
	return fmt.Sprintf("%s: %s", e.Message, e.SQLiteCode)
}

func (e *SQLiteError) Is(target error) bool {
	return target == ErrSQLite
}

// convertSQLiteError converts a [D1Error] to a more-specific [SQLiteError] if
// it is appropriate. Otherwise, it returns the original error.
func convertSQLiteError(err error, query string, bindings []any) error {
	var d1Err *D1Error
	if errors.As(err, &d1Err) && d1Err.Code == 7500 {
		parts := strings.SplitN(d1Err.Message, ": SQLITE_", 2)
		message := parts[0]
		sqliteCode := "SQLITE_ERROR" // default if not specified
		if len(parts) == 2 {
			sqliteCode = "SQLITE_" + parts[1]
		}
		return newSQLiteError(message, query, bindings, sqliteCode)
	}
	return err
}
