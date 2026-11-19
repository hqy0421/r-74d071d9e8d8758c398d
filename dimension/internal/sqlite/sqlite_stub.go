//go:build !(cgo && linux)
// +build !cgo !linux

// Package sqlite is unavailable on this platform/build configuration.
//
// The dimension loader needs the bundled SQLite binding, which requires cgo and
// the system libsqlite3 shared library on Linux. This stub keeps the rest of
// the module buildable elsewhere while making the missing capability explicit
// at the call site.
package sqlite

import "errors"

// ErrUnsupported is reported by every entry point when the real binding is not
// compiled in.
var ErrUnsupported = errors.New("sqlite: bundled binding requires cgo and libsqlite3 on linux")

// Column data types, mirrored for API parity with the supported build.
const (
	ColumnInteger = 1
	ColumnFloat   = 2
	ColumnText    = 3
	ColumnBlob    = 4
	ColumnNull    = 5
)

// DB is a placeholder for the unsupported build.
type DB struct{}

// Open always fails on the unsupported build.
func Open(string) (*DB, error) { return nil, ErrUnsupported }

// Close always fails on the unsupported build.
func (d *DB) Close() error { return ErrUnsupported }

// Exec always fails on the unsupported build.
func (d *DB) Exec(string) error { return ErrUnsupported }

// Prepare always fails on the unsupported build.
func (d *DB) Prepare(string) (*Stmt, error) { return nil, ErrUnsupported }

// Changes always reports zero on the unsupported build.
func (d *DB) Changes() int { return 0 }

// LastInsertRowID always reports zero on the unsupported build.
func (d *DB) LastInsertRowID() int64 { return 0 }

// Stmt is a placeholder for the unsupported build.
type Stmt struct{}

// Close always fails on the unsupported build.
func (s *Stmt) Close() error { return ErrUnsupported }

// BindInt64 always fails on the unsupported build.
func (s *Stmt) BindInt64(int, int64) error { return ErrUnsupported }

// BindFloat always fails on the unsupported build.
func (s *Stmt) BindFloat(int, float64) error { return ErrUnsupported }

// BindText always fails on the unsupported build.
func (s *Stmt) BindText(int, string) error { return ErrUnsupported }

// BindBlob always fails on the unsupported build.
func (s *Stmt) BindBlob(int, []byte) error { return ErrUnsupported }

// BindNull always fails on the unsupported build.
func (s *Stmt) BindNull(int) error { return ErrUnsupported }

// Step always fails on the unsupported build.
func (s *Stmt) Step() (bool, error) { return false, ErrUnsupported }

// Reset always fails on the unsupported build.
func (s *Stmt) Reset() {}

// ColumnType always reports NULL on the unsupported build.
func (s *Stmt) ColumnType(int) int { return ColumnNull }

// Int64 always reports zero on the unsupported build.
func (s *Stmt) Int64(int) int64 { return 0 }

// Float64 always reports zero on the unsupported build.
func (s *Stmt) Float64(int) float64 { return 0 }

// Text always reports the empty string on the unsupported build.
func (s *Stmt) Text(int) string { return "" }

// Blob always reports nil on the unsupported build.
func (s *Stmt) Blob(int) []byte { return nil }
