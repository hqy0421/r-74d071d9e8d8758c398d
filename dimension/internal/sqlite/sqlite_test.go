//go:build cgo && linux
// +build cgo,linux

package sqlite

import (
	"path/filepath"
	"testing"
)

func TestOpenExecPrepareRoundTrip(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Exec(`CREATE TABLE t (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		score REAL,
		note TEXT,
		payload BLOB
	);
	CREATE INDEX t_name ON t(name);`); err != nil {
		t.Fatalf("Exec multi-statement: %v", err)
	}

	ins, err := db.Prepare(`INSERT INTO t(name, score, note, payload) VALUES (?, ?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ins.Close() }()
	if err := ins.BindText(1, "alpha"); err != nil {
		t.Fatal(err)
	}
	if err := ins.BindFloat(2, 1.5); err != nil {
		t.Fatal(err)
	}
	if err := ins.BindNull(3); err != nil {
		t.Fatal(err)
	}
	if err := ins.BindBlob(4, []byte{0, 1, 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := ins.Step(); err != nil {
		t.Fatalf("Step insert: %v", err)
	}
	if id := db.LastInsertRowID(); id != 1 {
		t.Fatalf("rowid: %d", id)
	}
	if n := db.Changes(); n != 1 {
		t.Fatalf("changes: %d", n)
	}

	// Reuse the statement with Reset.
	ins.Reset()
	if err := ins.BindText(1, "beta"); err != nil {
		t.Fatal(err)
	}
	if err := ins.BindFloat(2, 2.5); err != nil {
		t.Fatal(err)
	}
	if err := ins.BindText(3, ""); err != nil {
		t.Fatal(err)
	}
	if err := ins.BindBlob(4, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := ins.Step(); err != nil {
		t.Fatal(err)
	}

	sel, err := db.Prepare(`SELECT id, name, score, note, payload FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sel.Close() }()

	ok, err := sel.Step()
	if err != nil || !ok {
		t.Fatalf("first row: ok=%v err=%v", ok, err)
	}
	if sel.Int64(0) != 1 || sel.Text(1) != "alpha" || sel.Float64(2) != 1.5 {
		t.Fatalf("row 1: %d %q %v", sel.Int64(0), sel.Text(1), sel.Float64(2))
	}
	if sel.ColumnType(3) != ColumnNull {
		t.Fatalf("note should be NULL, type=%d", sel.ColumnType(3))
	}
	if b := sel.Blob(4); len(b) != 3 || b[2] != 2 {
		t.Fatalf("blob: %v", b)
	}

	ok, err = sel.Step()
	if err != nil || !ok {
		t.Fatalf("second row: ok=%v err=%v", ok, err)
	}
	if sel.Text(1) != "beta" || sel.Text(3) != "" {
		t.Fatalf("row 2: %q %q", sel.Text(1), sel.Text(3))
	}
	if sel.ColumnType(4) != ColumnBlob || len(sel.Blob(4)) != 0 {
		t.Fatalf("empty blob should stay an empty blob, type=%d", sel.ColumnType(4))
	}

	ok, err = sel.Step()
	if err != nil || ok {
		t.Fatalf("end of rows: ok=%v err=%v", ok, err)
	}
}

func TestConstraintsSurfaceAsErrors(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Exec(`CREATE TABLE t (a INTEGER PRIMARY KEY); INSERT INTO t VALUES (1);`); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO t VALUES (1);`); err == nil {
		t.Fatal("expected unique violation")
	}
}

func TestOpenBadPath(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "missing", "dir", "t.db")); err == nil {
		t.Fatal("expected open error for missing directory")
	}
}

func TestEmptyTextAndBlobAreNotEmptyValues(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Exec(`CREATE TABLE t(a TEXT, b BLOB)`); err != nil {
		t.Fatal(err)
	}
	ins, err := db.Prepare(`INSERT INTO t VALUES (?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ins.Close() }()
	if err := ins.BindText(1, ""); err != nil {
		t.Fatal(err)
	}
	if err := ins.BindBlob(2, []byte{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ins.Step(); err != nil {
		t.Fatal(err)
	}

	q, err := db.Prepare(`SELECT a IS NULL, length(a), b IS NULL, length(b) FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	if ok, err := q.Step(); err != nil || !ok {
		t.Fatalf("step: ok=%v err=%v", ok, err)
	}
	// Empty string and zero-length blob are stored as empty values, not NULL.
	if q.Int64(0) != 0 || q.Int64(1) != 0 {
		t.Fatalf("empty text: isNull=%d len=%d", q.Int64(0), q.Int64(1))
	}
	if q.Int64(2) != 0 || q.Int64(3) != 0 {
		t.Fatalf("empty blob: isNull=%d len=%d", q.Int64(2), q.Int64(3))
	}
}
