// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package dimension

import (
	"fmt"
	"strings"
	"sync"

	"github.com/linkedin/goavro/v2/dimension/internal/sqlite"
)

// schemaVersion is stored in dim_meta so that a future format change can be
// detected rather than silently misread.
const schemaVersion = "1"

// schemaStatements creates the storage layout. Version rows are immutable:
// the triggers below reject any UPDATE or DELETE that would change a published
// version in place.
//
// Timeline conventions
//
//   - dim_version stores a half-open interval [effective_time, end_time).
//     end_time is NULL for the version still open at the timeline's end.
//   - entity_key is stored as TEXT, effective_time as INTEGER milliseconds.
//   - attrs_blob is the canonical attribute encoding; attrs_hash is its
//     SHA-256, and is also the content identity used for version reuse.
var schemaStatements = []string{
	`PRAGMA foreign_keys = ON`,
	`CREATE TABLE IF NOT EXISTS dim_meta (
		key   TEXT PRIMARY KEY,
		value TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS dim_entity (
		entity_key    TEXT PRIMARY KEY,
		current_dim_id INTEGER,
		FOREIGN KEY (current_dim_id) REFERENCES dim_version(dim_id)
	)`,
	`CREATE TABLE IF NOT EXISTS dim_version (
		dim_id         INTEGER PRIMARY KEY AUTOINCREMENT,
		entity_key     TEXT NOT NULL,
		effective_time INTEGER NOT NULL,
		end_time       INTEGER,
		attrs_blob     BLOB NOT NULL,
		attrs_hash     TEXT NOT NULL,
		deleted        INTEGER NOT NULL DEFAULT 0 CHECK (deleted IN (0, 1)),
		CHECK (end_time IS NULL OR end_time >= effective_time),
		FOREIGN KEY (entity_key) REFERENCES dim_entity(entity_key)
	)`,
	`CREATE INDEX IF NOT EXISTS dim_version_entity_effective ON dim_version(entity_key, effective_time)`,

	`CREATE TABLE IF NOT EXISTS dim_batch (
		batch_id       TEXT PRIMARY KEY,
		content_hash   TEXT NOT NULL,
		checkpoint_seq INTEGER NOT NULL,
		status         TEXT NOT NULL CHECK (status IN ('published', 'skipped')),
		point_count    INTEGER NOT NULL,
		fact_count     INTEGER NOT NULL,
		created_at     INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS dim_fact (
		fact_id         TEXT PRIMARY KEY,
		entity_key      TEXT NOT NULL,
		effective_time  INTEGER NOT NULL,
		payload         BLOB,
		content_hash    TEXT NOT NULL,
		frozen_dim_id   INTEGER,
		current_dim_id  INTEGER,
		batch_id        TEXT NOT NULL,
		frozen_at       INTEGER NOT NULL,
		FOREIGN KEY (frozen_dim_id)  REFERENCES dim_version(dim_id),
		FOREIGN KEY (current_dim_id) REFERENCES dim_version(dim_id),
		FOREIGN KEY (batch_id)       REFERENCES dim_batch(batch_id)
	)`,
	`CREATE INDEX IF NOT EXISTS dim_fact_entity ON dim_fact(entity_key)`,

	// A version that any fact froze must survive forever: the fact's
	// frozen_dim_id is a promise about the attributes it was loaded against.
	`CREATE TRIGGER IF NOT EXISTS dim_version_no_delete_frozen
		BEFORE DELETE ON dim_version
		WHEN EXISTS (SELECT 1 FROM dim_fact WHERE frozen_dim_id = OLD.dim_id)
	BEGIN
		SELECT RAISE(ABORT, 'dimension: cannot delete a version referenced by frozen_dim_id');
	END`,

	// A version's attributes are immutable. The only columns a loader may
	// change are its half-open interval end time, which is bookkeeping about
	// where the version sits on the timeline, never what the version says.
	`CREATE TRIGGER IF NOT EXISTS dim_version_no_attr_update
		BEFORE UPDATE OF dim_id, entity_key, effective_time, attrs_blob, attrs_hash, deleted ON dim_version
	BEGIN
		SELECT RAISE(ABORT, 'dimension: dimension version attributes are immutable');
	END`,
}

// Store owns a SQLite connection and the dimension schema. It is safe for
// concurrent use; Loads are serialised per Store.
type Store struct {
	mu sync.Mutex
	db *sqlite.DB
}

// Open opens (creating if necessary) the dimension database at path and applies
// the schema. It is safe to call repeatedly on an existing database.
func Open(path string) (*Store, error) {
	db, err := sqlite.Open(path)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init() error {
	if err := s.db.Exec(strings.Join(schemaStatements, ";\n") + ";"); err != nil {
		return fmt.Errorf("dimension: cannot create schema: %w", err)
	}
	return s.exec(
		`INSERT INTO dim_meta(key, value) VALUES ('schema_version', ?)
		 ON CONFLICT(key) DO NOTHING`,
		schemaVersion,
	)
}

// Close releases the underlying database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// exec runs a single statement with text parameters.
func (s *Store) exec(query string, args ...interface{}) error {
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	if err := bindAll(stmt, args); err != nil {
		return err
	}
	if _, err := stmt.Step(); err != nil {
		return err
	}
	return nil
}

// bindAll binds heterogeneous arguments by position. Supported kinds are nil,
// strings, ints, and floats.
func bindAll(stmt *sqlite.Stmt, args []interface{}) error {
	for i, arg := range args {
		index := i + 1
		var err error
		switch v := arg.(type) {
		case nil:
			err = stmt.BindNull(index)
		case string:
			err = stmt.BindText(index, v)
		case int:
			err = stmt.BindInt64(index, int64(v))
		case int64:
			err = stmt.BindInt64(index, v)
		case float64:
			err = stmt.BindFloat(index, v)
		case []byte:
			err = stmt.BindBlob(index, v)
		default:
			err = fmt.Errorf("dimension: unsupported bind type %T", arg)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
