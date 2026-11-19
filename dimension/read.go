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

	"github.com/linkedin/goavro/v2/dimension/internal/sqlite"
)

// Version is a published SCD2 version. Its attributes never change after
// publication; only EndTime may be extended or shortened as later points
// refine the timeline.
type Version struct {
	DimID         int64
	EntityKey     string
	EffectiveTime int64
	// EndTime is the exclusive end of the version's interval. EndTime is only
	// meaningful when OpenEnded is false.
	EndTime int64
	// OpenEnded reports that the version has no successor and its interval
	// extends forward without bound.
	OpenEnded bool
	// Deleted reports a tombstone version: the entity was deleted effective at
	// EffectiveTime.
	Deleted bool
	Attrs   map[string]interface{}
}

// Versions returns every published version of one entity, including superseded
// rows kept alive for frozen facts, ordered by effective time and then dim_id.
func (s *Store) Versions(entityKey string) ([]Version, error) {
	if s == nil || s.db == nil {
		return nil, errClosed
	}
	stmt, err := s.db.Prepare(
		`SELECT dim_id, entity_key, effective_time, end_time, attrs_blob, deleted
		 FROM dim_version WHERE entity_key = ?
		 ORDER BY effective_time ASC, dim_id ASC`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()
	if err := stmt.BindText(1, entityKey); err != nil {
		return nil, err
	}
	var versions []Version
	for {
		ok, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !ok {
			return versions, nil
		}
		attrs, err := decodeAttrs(stmt.Blob(4))
		if err != nil {
			return nil, err
		}
		v := Version{
			DimID:         stmt.Int64(0),
			EntityKey:     stmt.Text(1),
			EffectiveTime: stmt.Int64(2),
			OpenEnded:     stmt.ColumnType(3) == sqlite.ColumnNull,
			Deleted:       stmt.Int64(5) != 0,
			Attrs:         attrs,
		}
		if !v.OpenEnded {
			v.EndTime = stmt.Int64(3)
		}
		versions = append(versions, v)
	}
}

// DimensionAt returns the version in effect for entityKey at instant, or nil
// when no live (non-deleted) version covers it.
func (s *Store) DimensionAt(entityKey string, instant int64) (*Version, error) {
	if s == nil || s.db == nil {
		return nil, errClosed
	}
	stmt, err := s.db.Prepare(
		`SELECT dim_id, entity_key, effective_time, end_time, attrs_blob, deleted
		 FROM dim_version
		 WHERE entity_key = ? AND deleted = 0
		   AND effective_time <= ?
		   AND (end_time IS NULL OR end_time > ?)
		 ORDER BY effective_time DESC, dim_id DESC
		 LIMIT 1`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()
	if err := stmt.BindText(1, entityKey); err != nil {
		return nil, err
	}
	if err := stmt.BindInt64(2, instant); err != nil {
		return nil, err
	}
	if err := stmt.BindInt64(3, instant); err != nil {
		return nil, err
	}
	ok, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	attrs, err := decodeAttrs(stmt.Blob(4))
	if err != nil {
		return nil, err
	}
	v := &Version{
		DimID:         stmt.Int64(0),
		EntityKey:     stmt.Text(1),
		EffectiveTime: stmt.Int64(2),
		OpenEnded:     stmt.ColumnType(3) == sqlite.ColumnNull,
		Deleted:       stmt.Int64(5) != 0,
		Attrs:         attrs,
	}
	if !v.OpenEnded {
		v.EndTime = stmt.Int64(3)
	}
	return v, nil
}

// CurrentDimID returns the dimension version an entity currently resolves to,
// or 0 when the entity has no current row (never seen, or deleted).
func (s *Store) CurrentDimID(entityKey string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, errClosed
	}
	stmt, err := s.db.Prepare(`SELECT current_dim_id FROM dim_entity WHERE entity_key = ?`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()
	if err := stmt.BindText(1, entityKey); err != nil {
		return 0, err
	}
	ok, err := stmt.Step()
	if err != nil {
		return 0, err
	}
	if !ok || stmt.ColumnType(0) == sqlite.ColumnNull {
		return 0, nil
	}
	return stmt.Int64(0), nil
}

// FactState is a fact together with both of its dimension references.
type FactState struct {
	FactID        string
	EntityKey     string
	EffectiveTime int64
	Payload       []byte
	// FrozenDimID is the version chosen when the fact was loaded. It is 0 when
	// no dimension version covered the fact at load time.
	FrozenDimID int64
	// CurrentDimID is the version that currently covers the fact's instant. It
	// is 0 when no live version covers it now.
	CurrentDimID int64
	// FrozenAttrs and CurrentAttrs are the decoded attributes behind the two
	// references, nil when the corresponding reference is absent.
	FrozenAttrs  map[string]interface{}
	CurrentAttrs map[string]interface{}
}

// Fact returns the stored state of one fact.
func (s *Store) Fact(factID string) (*FactState, error) {
	if s == nil || s.db == nil {
		return nil, errClosed
	}
	stmt, err := s.db.Prepare(
		`SELECT fact_id, entity_key, effective_time, payload, frozen_dim_id, current_dim_id
		 FROM dim_fact WHERE fact_id = ?`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()
	if err := stmt.BindText(1, factID); err != nil {
		return nil, err
	}
	ok, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	state := &FactState{
		FactID:        stmt.Text(0),
		EntityKey:     stmt.Text(1),
		EffectiveTime: stmt.Int64(2),
	}
	if stmt.ColumnType(3) != sqlite.ColumnNull {
		state.Payload = stmt.Blob(3)
	}
	if stmt.ColumnType(4) != sqlite.ColumnNull {
		state.FrozenDimID = stmt.Int64(4)
	}
	if stmt.ColumnType(5) != sqlite.ColumnNull {
		state.CurrentDimID = stmt.Int64(5)
	}
	if state.FrozenDimID != 0 {
		state.FrozenAttrs, err = s.attrsFor(state.FrozenDimID)
		if err != nil {
			return nil, err
		}
	}
	if state.CurrentDimID != 0 {
		state.CurrentAttrs, err = s.attrsFor(state.CurrentDimID)
		if err != nil {
			return nil, err
		}
	}
	return state, nil
}

func (s *Store) attrsFor(dimID int64) (map[string]interface{}, error) {
	stmt, err := s.db.Prepare(`SELECT attrs_blob FROM dim_version WHERE dim_id = ?`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()
	if err := stmt.BindInt64(1, dimID); err != nil {
		return nil, err
	}
	ok, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("dimension: version %d not found", dimID)
	}
	return decodeAttrs(stmt.Blob(0))
}
