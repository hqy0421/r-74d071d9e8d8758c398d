//go:build cgo && linux
// +build cgo,linux

// Package sqlite is a deliberately small SQLite binding used by the dimension
// loader. It embeds just the handful of APIs the loader needs so that the
// goavro module keeps its existing, dependency-free module graph.
//
// The binding links against the system libsqlite3 shared object using its
// versioned file name, so it does not require the sqlite3 development package
// (headers or the unversioned libsqlite3.so symlink) to be installed.
package sqlite

/*
#cgo LDFLAGS: -l:libsqlite3.so.0
#include <stdlib.h>

typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;

extern int sqlite3_open_v2(const char*, sqlite3**, int, const char*);
extern int sqlite3_close_v2(sqlite3*);
extern int sqlite3_busy_timeout(sqlite3*, int);
extern int sqlite3_extended_result_codes(sqlite3*, int);
extern const char *sqlite3_errmsg(sqlite3*);
extern int sqlite3_prepare_v2(sqlite3*, const char*, int, sqlite3_stmt**, const char**);
extern int sqlite3_step(sqlite3_stmt*);
extern int sqlite3_finalize(sqlite3_stmt*);
extern int sqlite3_reset(sqlite3_stmt*);
extern int sqlite3_clear_bindings(sqlite3_stmt*);
extern int sqlite3_bind_int64(sqlite3_stmt*, int, long long);
extern int sqlite3_bind_double(sqlite3_stmt*, int, double);
extern int sqlite3_bind_null(sqlite3_stmt*, int);
extern int sqlite3_bind_blob(sqlite3_stmt*, int, const void*, int, void (*)(void*));
extern int sqlite3_bind_text(sqlite3_stmt*, int, const char*, int, void (*)(void*));
extern int sqlite3_column_type(sqlite3_stmt*, int);
extern long long sqlite3_column_int64(sqlite3_stmt*, int);
extern double sqlite3_column_double(sqlite3_stmt*, int);
extern const unsigned char *sqlite3_column_text(sqlite3_stmt*, int);
extern const void *sqlite3_column_blob(sqlite3_stmt*, int);
extern int sqlite3_column_bytes(sqlite3_stmt*, int);
extern int sqlite3_changes(sqlite3*);
extern long long sqlite3_last_insert_rowid(sqlite3*);

// bind_text_copy binds a string and asks SQLite to copy it (SQLITE_TRANSIENT),
// so the caller may release the buffer as soon as the call returns.
static int bind_text_copy(sqlite3_stmt *st, int index, const char *value, int n) {
	return sqlite3_bind_text(st, index, value, n, (void (*)(void *))-1);
}

static int bind_blob_copy(sqlite3_stmt *st, int index, const void *value, int n) {
	return sqlite3_bind_blob(st, index, value, n, (void (*)(void *))-1);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

// Result codes from the SQLite C API.
const (
	row    = 100
	done   = 101
	okCode = 0
)

// Open flags: read-write, create, full mutex.
const (
	openReadWrite = 0x00000002
	openCreate    = 0x00000004
	openFullMutex = 0x00010000
)

// Column data types reported by sqlite3_column_type.
const (
	ColumnInteger = 1
	ColumnFloat   = 2
	ColumnText    = 3
	ColumnBlob    = 4
	ColumnNull    = 5
)

// DB is a SQLite database connection. It is safe for concurrent use by
// multiple goroutines; a single connection is serialised with a mutex.
type DB struct {
	mu sync.Mutex
	db *C.sqlite3
}

// Open opens (creating if needed) the SQLite database at path.
func Open(path string) (*DB, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	var handle *C.sqlite3
	rc := C.sqlite3_open_v2(cpath, &handle, C.int(openReadWrite|openCreate|openFullMutex), nil) //nolint:gocritic
	if rc != okCode {
		if handle != nil {
			_ = C.sqlite3_close_v2(handle)
		}
		return nil, fmt.Errorf("sqlite: cannot open %q: code %d", path, int(rc))
	}
	C.sqlite3_extended_result_codes(handle, 1)
	C.sqlite3_busy_timeout(handle, 5000)
	return &DB{db: handle}, nil
}

// Close releases the database connection.
func (d *DB) Close() error {
	if d == nil || d.db == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if rc := C.sqlite3_close_v2(d.db); rc != okCode {
		return fmt.Errorf("sqlite: close: %s", d.errmsg())
	}
	d.db = nil
	return nil
}

// errmsg must be called while holding d.mu.
func (d *DB) errmsg() string {
	if d.db == nil {
		return "database is closed"
	}
	return C.GoString(C.sqlite3_errmsg(d.db))
}

// Exec runs one or more SQL statements that return no rows. Use for DDL and
// for statements that need no bound parameters.
func (d *DB) Exec(query string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	remaining := query
	for remaining != "" {
		st, consumed, err := d.preparePrefix(remaining)
		if err != nil {
			return err
		}
		if st == nil {
			return nil // only whitespace or comments left
		}
		err = stepAll(d, st)
		C.sqlite3_finalize(st)
		if err != nil {
			return err
		}
		if consumed <= 0 {
			return nil
		}
		remaining = remaining[consumed:]
	}
	return nil
}

// stepAll drains a statement, expecting it to produce no rows.
func stepAll(d *DB, st *C.sqlite3_stmt) error {
	for {
		switch rc := C.sqlite3_step(st); rc {
		case done:
			return nil
		case row:
			continue
		default:
			return fmt.Errorf("sqlite: exec: %s", d.errmsg())
		}
	}
}

// Stmt is a prepared statement. It must not be used after the DB is closed.
type Stmt struct {
	db *DB
	st *C.sqlite3_stmt
}

// Prepare compiles query into a reusable statement.
func (d *DB) Prepare(query string) (*Stmt, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	st, _, err := d.preparePrefix(query)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errors.New("sqlite: cannot prepare empty statement")
	}
	return &Stmt{db: d, st: st}, nil
}

// preparePrefix compiles the first statement in query, returning the number of
// bytes consumed so callers can iterate over a multi-statement script. Callers
// must hold d.mu.
func (d *DB) preparePrefix(query string) (*C.sqlite3_stmt, int, error) {
	if d.db == nil {
		return nil, 0, errors.New("sqlite: database is closed")
	}
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))

	var st *C.sqlite3_stmt
	var tail *C.char
	rc := C.sqlite3_prepare_v2(d.db, cquery, C.int(len(query)), &st, &tail) //nolint:gocritic
	if rc != okCode {
		return nil, 0, fmt.Errorf("sqlite: cannot prepare: %s", d.errmsg())
	}
	consumed := len(query)
	if tail != nil {
		offset := uintptr(unsafe.Pointer(tail)) - uintptr(unsafe.Pointer(cquery)) //nolint:gosec
		if offset <= uintptr(len(query)) {
			consumed = int(offset)
		}
	}
	return st, consumed, nil
}

// Close finalizes the prepared statement.
func (s *Stmt) Close() error {
	if s == nil || s.st == nil {
		return nil
	}
	C.sqlite3_finalize(s.st)
	s.st = nil
	return nil
}

// BindInt64 binds an integer to the 1-based parameter index.
func (s *Stmt) BindInt64(index int, value int64) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if rc := C.sqlite3_bind_int64(s.st, C.int(index), C.longlong(value)); rc != okCode {
		return fmt.Errorf("sqlite: bind int64: %s", s.db.errmsg())
	}
	return nil
}

// BindFloat binds a float to the 1-based parameter index.
func (s *Stmt) BindFloat(index int, value float64) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if rc := C.sqlite3_bind_double(s.st, C.int(index), C.double(value)); rc != okCode {
		return fmt.Errorf("sqlite: bind float: %s", s.db.errmsg())
	}
	return nil
}

// BindText binds a string to the 1-based parameter index. An empty string is
// stored as an empty string, not as SQL NULL; use BindNull for NULL.
func (s *Stmt) BindText(index int, value string) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	cvalue := C.CString(value)
	defer C.free(unsafe.Pointer(cvalue))
	if rc := C.bind_text_copy(s.st, C.int(index), cvalue, C.int(len(value))); rc != okCode {
		return fmt.Errorf("sqlite: bind text: %s", s.db.errmsg())
	}
	return nil
}

// BindBlob binds a byte slice to the 1-based parameter index. A zero-length
// slice is stored as an empty blob, not as SQL NULL; use BindNull for NULL.
func (s *Stmt) BindBlob(index int, value []byte) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if len(value) == 0 {
		// Bind a non-NULL pointer with a zero length: SQLite distinguishes a
		// NULL pointer (NULL) from a zero-length value (empty blob).
		empty := C.malloc(1)
		defer C.free(empty)
		if rc := C.bind_blob_copy(s.st, C.int(index), empty, 0); rc != okCode {
			return fmt.Errorf("sqlite: bind blob: %s", s.db.errmsg())
		}
		return nil
	}
	if rc := C.bind_blob_copy(s.st, C.int(index), unsafe.Pointer(&value[0]), C.int(len(value))); rc != okCode {
		return fmt.Errorf("sqlite: bind blob: %s", s.db.errmsg())
	}
	return nil
}

// BindNull binds SQL NULL to the 1-based parameter index.
func (s *Stmt) BindNull(index int) error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if rc := C.sqlite3_bind_null(s.st, C.int(index)); rc != okCode {
		return fmt.Errorf("sqlite: bind null: %s", s.db.errmsg())
	}
	return nil
}

// Step advances the statement. It reports whether a row is available.
func (s *Stmt) Step() (bool, error) {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	switch rc := C.sqlite3_step(s.st); rc {
	case row:
		return true, nil
	case done:
		return false, nil
	default:
		return false, fmt.Errorf("sqlite: step: %s", s.db.errmsg())
	}
}

// Reset prepares the statement to be executed again.
func (s *Stmt) Reset() {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	C.sqlite3_reset(s.st)
	C.sqlite3_clear_bindings(s.st)
}

// ColumnType reports the SQLite storage class of the 0-based column.
func (s *Stmt) ColumnType(index int) int {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	return int(C.sqlite3_column_type(s.st, C.int(index)))
}

// Int64 returns the 0-based column as an integer.
func (s *Stmt) Int64(index int) int64 {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	return int64(C.sqlite3_column_int64(s.st, C.int(index)))
}

// Float64 returns the 0-based column as a float.
func (s *Stmt) Float64(index int) float64 {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	return float64(C.sqlite3_column_double(s.st, C.int(index)))
}

// Text returns the 0-based column as a string.
func (s *Stmt) Text(index int) string {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	ptr := C.sqlite3_column_text(s.st, C.int(index))
	if ptr == nil {
		return ""
	}
	n := C.sqlite3_column_bytes(s.st, C.int(index))
	return C.GoStringN((*C.char)(unsafe.Pointer(ptr)), n)
}

// Blob returns a copy of the 0-based column as bytes.
func (s *Stmt) Blob(index int) []byte {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	ptr := C.sqlite3_column_blob(s.st, C.int(index))
	if ptr == nil {
		return nil
	}
	n := C.sqlite3_column_bytes(s.st, C.int(index))
	return C.GoBytes(ptr, n)
}

// Changes returns the number of rows modified by the most recent write.
func (d *DB) Changes() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int(C.sqlite3_changes(d.db))
}

// LastInsertRowID returns the rowid of the most recent successful insert.
func (d *DB) LastInsertRowID() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int64(C.sqlite3_last_insert_rowid(d.db))
}
