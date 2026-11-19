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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/linkedin/goavro/v2/dimension/internal/sqlite"
)

// Errors reported by Load and the read helpers. They are sentinel values so
// callers can react programmatically.
var (
	// ErrDuplicatePoint reports two points for the same entity key at the same
	// effective instant. There is no principled way to order them, so the batch
	// is rejected rather than guessed at.
	ErrDuplicatePoint = errors.New("dimension: duplicate point for entity key and effective time")

	// ErrFactConflict reports an existing fact whose content differs from the
	// fact being loaded under the same fact_id.
	ErrFactConflict = errors.New("dimension: fact id already exists with different content")

	// ErrStaleCheckpoint reports a caller-supplied checkpoint that does not
	// match the last published one.
	ErrStaleCheckpoint = errors.New("dimension: stale checkpoint")

	errClosed = errors.New("dimension: store is closed")
)

// Config describes one Load.
type Config struct {
	// PointCodec and FactCodec are the goavro codecs used to decode the
	// supplied bytes. They must decode records shaped like PointSchema and
	// FactSchema; pass NewPointCodec() and NewFactCodec() when the caller has
	// no schema of its own.
	PointCodec *goavro.Codec
	FactCodec  *goavro.Codec

	// PointBytes is a concatenation of binary-encoded dimension-point records.
	PointBytes []byte
	// FactBytes is a concatenation of binary-encoded fact records.
	FactBytes []byte

	// ExpectedCheckpoint, when non-nil, must equal the last published
	// checkpoint or the batch is rejected with ErrStaleCheckpoint. Leave nil
	// to publish unconditionally.
	ExpectedCheckpoint *int64
}

// Batch describes the outcome of Load.
type Batch struct {
	// ID is the batch identity derived from its content.
	ID string
	// Checkpoint is the checkpoint in force after this call. For a replay of an
	// already published batch it is the unchanged prior checkpoint.
	Checkpoint int64
	// Published is false when an identical batch had already been published;
	// its dimensions, facts, and checkpoint are left untouched.
	Published bool
	// Points and Facts count the records accepted in this batch.
	Points int
	Facts  int
}

// Load decodes a batch of points and facts, rebuilds the affected dimension
// timelines, freezes the facts against the batch's final dimension state, and
// publishes everything atomically.
//
// The published unit is: the dimension versions and current_dim_id mapping, the
// facts and both of their dimension foreign keys, the batch row, and the
// checkpoint. If any step fails, no part of the batch becomes visible and the
// previous checkpoint remains current.
func (s *Store) Load(cfg Config) (Batch, error) {
	if s == nil || s.db == nil {
		return Batch{}, errClosed
	}
	if cfg.PointCodec == nil && len(cfg.PointBytes) != 0 {
		return Batch{}, errors.New("dimension: PointBytes supplied without PointCodec")
	}
	if cfg.FactCodec == nil && len(cfg.FactBytes) != 0 {
		return Batch{}, errors.New("dimension: FactBytes supplied without FactCodec")
	}

	points, err := decodePoints(cfg.PointCodec, cfg.PointBytes)
	if err != nil {
		return Batch{}, err
	}
	facts, err := decodeFacts(cfg.FactCodec, cfg.FactBytes)
	if err != nil {
		return Batch{}, err
	}
	if err := rejectDuplicatePoints(points); err != nil {
		return Batch{}, err
	}

	batchID, err := batchIdentity(points, facts)
	if err != nil {
		return Batch{}, err
	}

	// A single Store publishes one batch at a time: the transaction spans many
	// statements, so the connection must not be shared mid-transaction.
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Exec("BEGIN IMMEDIATE"); err != nil {
		return Batch{}, fmt.Errorf("dimension: cannot begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = s.db.Exec("ROLLBACK")
		}
	}()

	prevCheckpoint, err := s.checkpoint()
	if err != nil {
		return Batch{}, err
	}
	if cfg.ExpectedCheckpoint != nil && *cfg.ExpectedCheckpoint != prevCheckpoint {
		return Batch{}, fmt.Errorf("%w: have %d, expected %d", ErrStaleCheckpoint, *cfg.ExpectedCheckpoint, prevCheckpoint)
	}

	// Idempotent replay: an identical batch already published makes no writes
	// and keeps the prior checkpoint.
	if prior, ok, err := s.batchRow(batchID); err != nil {
		return Batch{}, err
	} else if ok {
		if err := s.commit(); err != nil {
			return Batch{}, err
		}
		committed = true
		return Batch{ID: batchID, Checkpoint: prevCheckpoint, Published: false,
			Points: prior.Points, Facts: prior.Facts}, nil
	}

	// Collapse repeated fact ids within the batch: identical repeats are a
	// single fact, conflicting repeats are rejected before any write.
	facts, err = dedupeBatchFacts(facts)
	if err != nil {
		return Batch{}, err
	}
	// Facts already present under this fact_id must be identical (idempotent)
	// or a conflict; resolve that before any write.
	newFacts, err := s.reconcileFacts(facts)
	if err != nil {
		return Batch{}, err
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)
	nextCheckpoint := prevCheckpoint + 1

	// The batch row is the parent of every fact's batch_id foreign key, so it
	// must exist before freezing. It is still invisible outside the transaction
	// until commit, so a failure does not publish it.
	if err := s.exec(
		`INSERT INTO dim_batch(batch_id, content_hash, checkpoint_seq, status, point_count, fact_count, created_at)
		 VALUES (?, ?, ?, 'published', ?, ?, ?)`,
		batchID, batchID, nextCheckpoint, len(points), len(newFacts), now,
	); err != nil {
		return Batch{}, fmt.Errorf("dimension: cannot record batch: %w", err)
	}
	if err := s.applyPoints(points); err != nil {
		return Batch{}, err
	}
	// Freezing observes the whole batch's final dimension state: applyPoints
	// has materialised every point's effect, including same-instant corrections.
	if err := s.freezeFacts(newFacts, batchID, now); err != nil {
		return Batch{}, err
	}
	// Re-point the latest-restatement view against the now-final state. This
	// runs after freezing so a frozen reference is never moved.
	if err := s.repointCurrent(); err != nil {
		return Batch{}, err
	}

	if err := s.exec(
		`INSERT INTO dim_meta(key, value) VALUES ('checkpoint', ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		fmt.Sprintf("%d", nextCheckpoint),
	); err != nil {
		return Batch{}, err
	}

	if err := s.commit(); err != nil {
		return Batch{}, err
	}
	committed = true
	return Batch{ID: batchID, Checkpoint: nextCheckpoint, Published: true,
		Points: len(points), Facts: len(newFacts)}, nil
}

func (s *Store) commit() error {
	if err := s.db.Exec("COMMIT"); err != nil {
		return fmt.Errorf("dimension: cannot commit: %w", err)
	}
	return nil
}

// Checkpoint returns the last published checkpoint, or 0 when nothing has been
// published yet.
func (s *Store) Checkpoint() (int64, error) {
	return s.checkpoint()
}

func (s *Store) checkpoint() (int64, error) {
	stmt, err := s.db.Prepare(`SELECT value FROM dim_meta WHERE key = 'checkpoint'`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()
	ok, err := stmt.Step()
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	var value int64
	if _, err := fmt.Sscanf(stmt.Text(0), "%d", &value); err != nil {
		return 0, fmt.Errorf("dimension: corrupt checkpoint: %w", err)
	}
	return value, nil
}

func (s *Store) batchRow(batchID string) (Batch, bool, error) {
	stmt, err := s.db.Prepare(`SELECT checkpoint_seq, point_count, fact_count FROM dim_batch WHERE batch_id = ?`)
	if err != nil {
		return Batch{}, false, err
	}
	defer func() { _ = stmt.Close() }()
	if err := stmt.BindText(1, batchID); err != nil {
		return Batch{}, false, err
	}
	ok, err := stmt.Step()
	if err != nil {
		return Batch{}, false, err
	}
	if !ok {
		return Batch{}, false, nil
	}
	return Batch{
		ID:         batchID,
		Checkpoint: stmt.Int64(0),
		Published:  true,
		Points:     int(stmt.Int64(1)),
		Facts:      int(stmt.Int64(2)),
	}, true, nil
}

// batchIdentity derives a batch ID from its content, independent of the order
// in which records arrived, so replaying the same logical batch is
// recognisable without a caller-supplied ID.
func batchIdentity(points []Point, facts []Fact) (string, error) {
	type pointKey struct {
		entity  string
		instant int64
		op      string
		blob    string
	}
	pkeys := make([]pointKey, 0, len(points))
	for _, p := range points {
		blob, _, err := canonicalAttrs(p.Attrs)
		if err != nil {
			return "", err
		}
		pkeys = append(pkeys, pointKey{p.EntityKey, p.EffectiveTime, p.Op, string(blob)})
	}
	sort.SliceStable(pkeys, func(i, j int) bool {
		if pkeys[i].entity != pkeys[j].entity {
			return pkeys[i].entity < pkeys[j].entity
		}
		if pkeys[i].instant != pkeys[j].instant {
			return pkeys[i].instant < pkeys[j].instant
		}
		if pkeys[i].op != pkeys[j].op {
			return pkeys[i].op < pkeys[j].op
		}
		return pkeys[i].blob < pkeys[j].blob
	})

	h := sha256.New()
	h.Write([]byte("points\x00"))
	for _, k := range pkeys {
		fmt.Fprintf(h, "%s\x00%d\x00%s\x00", k.entity, k.instant, k.op)
		h.Write([]byte(k.blob))
		h.Write([]byte{0})
	}

	type factKey struct {
		id      string
		entity  string
		instant int64
		payload string
	}
	fkeys := make([]factKey, 0, len(facts))
	for _, f := range facts {
		hash, err := factContentHash(f)
		if err != nil {
			return "", err
		}
		fkeys = append(fkeys, factKey{f.FactID, f.EntityKey, f.EffectiveTime, hash})
	}
	sort.SliceStable(fkeys, func(i, j int) bool {
		if fkeys[i].id != fkeys[j].id {
			return fkeys[i].id < fkeys[j].id
		}
		return fkeys[i].payload < fkeys[j].payload
	})
	h.Write([]byte("facts\x00"))
	for _, k := range fkeys {
		fmt.Fprintf(h, "%s\x00%s\x00%d\x00%s\x00", k.id, k.entity, k.instant, k.payload)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// applyPoints rebuilds the half-open SCD2 timelines for the entity keys touched
// by the batch.
func (s *Store) applyPoints(points []Point) error {
	byEntity := make(map[string][]Point)
	for _, p := range points {
		byEntity[p.EntityKey] = append(byEntity[p.EntityKey], p)
	}
	keys := make([]string, 0, len(byEntity))
	for k := range byEntity {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := s.rebuildEntity(key, byEntity[key]); err != nil {
			return err
		}
	}
	return nil
}

// versionRow is one persisted SCD2 version.
type versionRow struct {
	DimID         int64
	EffectiveTime int64
	EndTime       int64
	OpenEnded     bool
	AttrsBlob     []byte
	AttrsHash     string
	Deleted       bool
}

// rebuildEntity folds the batch's points for one entity into the persisted
// timeline. Published version rows are never modified in place:
//
//   - A point whose content equals the version already live at its instant
//     reuses that immutable version.
//   - A point that changes history at an already covered instant is a
//     correction: a new version row is added and becomes the live version
//     there, while the superseded row is closed to a zero-width [t,t) so it no
//     longer participates in the timeline but remains readable by any fact that
//     froze it.
//   - A late-arriving point at an earlier instant inserts a new live version
//     and recomputes half-open end times; later versions are untouched.
//
// Only end_time is ever written to an existing row. Attributes, effective
// time, and the deleted flag are immutable by trigger.
func (s *Store) rebuildEntity(entityKey string, newPoints []Point) error {
	if err := s.exec(
		`INSERT INTO dim_entity(entity_key, current_dim_id) VALUES (?, NULL)
		 ON CONFLICT(entity_key) DO NOTHING`,
		entityKey,
	); err != nil {
		return err
	}

	existing, err := s.allVersions(entityKey)
	if err != nil {
		return err
	}

	// The live version for an instant is the newest row at that instant.
	live := make(map[int64]*versionRow, len(existing))
	for i := range existing {
		live[existing[i].EffectiveTime] = &existing[i]
	}

	sort.SliceStable(newPoints, func(i, j int) bool {
		return newPoints[i].EffectiveTime < newPoints[j].EffectiveTime
	})
	for _, p := range newPoints {
		// A deletion records the absence of attributes: it must be identical
		// regardless of whatever attribute payload accompanied the op.
		attrs := p.Attrs
		if p.Op == "D" {
			attrs = nil
		}
		blob, hash, err := canonicalAttrs(attrs)
		if err != nil {
			return err
		}
		deleted := p.Op == "D"
		if cur, ok := live[p.EffectiveTime]; ok &&
			cur.AttrsHash == hash && cur.Deleted == deleted {
			continue // unchanged version reused
		}
		dimID, err := s.insertVersion(entityKey, p.EffectiveTime, blob, hash, deleted)
		if err != nil {
			return err
		}
		live[p.EffectiveTime] = &versionRow{
			DimID: dimID, EffectiveTime: p.EffectiveTime,
			AttrsBlob: blob, AttrsHash: hash, Deleted: deleted,
		}
	}

	instants := make([]int64, 0, len(live))
	liveIDs := make(map[int64]struct{}, len(live))
	for instant, row := range live {
		instants = append(instants, instant)
		liveIDs[row.DimID] = struct{}{}
	}
	sort.Slice(instants, func(i, j int) bool { return instants[i] < instants[j] })

	// Supersede every version that is not the live one for its instant. This
	// covers rows pushed out by a same-instant correction and by an earlier
	// insertion that changed the ordering. Their payload stays readable.
	if err := s.supersedeNonLive(entityKey, liveIDs); err != nil {
		return err
	}

	// Half-open intervals for the live versions: [t_i, t_{i+1}); the last one
	// stays open. No deleted-version special case is needed: a deleted version
	// still occupies its interval, it just never matches a fact.
	for i, instant := range instants {
		var end interface{}
		if i+1 < len(instants) {
			end = instants[i+1]
		}
		if err := s.exec(`UPDATE dim_version SET end_time = ? WHERE dim_id = ?`,
			end, live[instant].DimID); err != nil {
			return err
		}
	}

	// current_dim_id is the newest live version, unless that version records a
	// deletion, in which case the entity currently has no dimension row.
	var current int64
	if n := len(instants); n > 0 {
		last := live[instants[n-1]]
		if !last.Deleted {
			current = last.DimID
		}
	}
	return s.exec(`UPDATE dim_entity SET current_dim_id = ? WHERE entity_key = ?`,
		nullableID(current), entityKey)
}

// supersedeNonLive closes every version of the entity that is not part of the
// live set to a zero-width interval.
func (s *Store) supersedeNonLive(entityKey string, liveIDs map[int64]struct{}) error {
	rows, err := s.allVersions(entityKey)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if _, ok := liveIDs[row.DimID]; ok {
			continue
		}
		if row.OpenEnded || row.EndTime != row.EffectiveTime {
			if err := s.exec(`UPDATE dim_version SET end_time = effective_time WHERE dim_id = ?`,
				row.DimID); err != nil {
				return err
			}
		}
	}
	return nil
}

// insertVersion appends an immutable version row.
func (s *Store) insertVersion(entityKey string, effective int64, blob []byte, hash string, deleted bool) (int64, error) {
	deletedFlag := 0
	if deleted {
		deletedFlag = 1
	}
	if err := s.exec(
		`INSERT INTO dim_version(entity_key, effective_time, end_time, attrs_blob, attrs_hash, deleted)
		 VALUES (?, ?, NULL, ?, ?, ?)`,
		entityKey, effective, blob, hash, deletedFlag,
	); err != nil {
		return 0, err
	}
	return s.db.LastInsertRowID(), nil
}

func (s *Store) allVersions(entityKey string) ([]versionRow, error) {
	stmt, err := s.db.Prepare(
		`SELECT dim_id, effective_time, end_time, attrs_blob, attrs_hash, deleted
		 FROM dim_version WHERE entity_key = ?
		 ORDER BY effective_time ASC, dim_id ASC`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()
	if err := stmt.BindText(1, entityKey); err != nil {
		return nil, err
	}
	var rows []versionRow
	for {
		ok, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !ok {
			return rows, nil
		}
		row := versionRow{
			DimID:         stmt.Int64(0),
			EffectiveTime: stmt.Int64(1),
			AttrsBlob:     stmt.Blob(3),
			AttrsHash:     stmt.Text(4),
			Deleted:       stmt.Int64(5) != 0,
			OpenEnded:     stmt.ColumnType(2) == sqlite.ColumnNull,
		}
		if !row.OpenEnded {
			row.EndTime = stmt.Int64(2)
		}
		rows = append(rows, row)
	}
}

// reconcileFacts splits the batch's facts into those that still need freezing
// and validates the ones that already exist.
func (s *Store) reconcileFacts(facts []Fact) ([]Fact, error) {
	pending := make([]Fact, 0, len(facts))
	for _, f := range facts {
		hash, err := factContentHash(f)
		if err != nil {
			return nil, err
		}
		stmt, err := s.db.Prepare(`SELECT content_hash FROM dim_fact WHERE fact_id = ?`)
		if err != nil {
			return nil, err
		}
		if err := stmt.BindText(1, f.FactID); err != nil {
			_ = stmt.Close()
			return nil, err
		}
		ok, err := stmt.Step()
		if err != nil {
			_ = stmt.Close()
			return nil, err
		}
		if !ok {
			_ = stmt.Close()
			pending = append(pending, f)
			continue
		}
		existing := stmt.Text(0)
		_ = stmt.Close()
		if existing != hash {
			return nil, fmt.Errorf("%w: %s", ErrFactConflict, f.FactID)
		}
		// Identical fact already frozen: leave its references untouched.
	}
	return pending, nil
}

func factContentHash(f Fact) (string, error) {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%d\x00", f.FactID, f.EntityKey, f.EffectiveTime)
	// Distinguish an absent payload from an empty one.
	if f.Payload == nil {
		h.Write([]byte{0})
	} else {
		h.Write([]byte{1})
	}
	h.Write(f.Payload)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// freezeFacts writes new facts, resolving frozen_dim_id against the batch's
// final dimension state and setting current_dim_id to the same version.
func (s *Store) freezeFacts(facts []Fact, batchID string, now int64) error {
	for _, f := range facts {
		hash, err := factContentHash(f)
		if err != nil {
			return err
		}
		dimID, err := s.versionInEffect(f.EntityKey, f.EffectiveTime)
		if err != nil {
			return err
		}
		if err := s.exec(
			`INSERT INTO dim_fact(fact_id, entity_key, effective_time, payload, content_hash,
			                     frozen_dim_id, current_dim_id, batch_id, frozen_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			f.FactID, f.EntityKey, f.EffectiveTime, payloadArg(f.Payload), hash,
			nullableID(dimID), nullableID(dimID), batchID, now,
		); err != nil {
			return err
		}
	}
	return nil
}

// versionInEffect returns the dim_id whose half-open interval contains instant,
// or 0 (NULL) when no live version covers it — an unknown entity, a fact that
// predates the entity's earliest point, or an instant covered by a deletion.
//
// When several rows share an instant, the newest (highest dim_id) wins.
func (s *Store) versionInEffect(entityKey string, instant int64) (int64, error) {
	stmt, err := s.db.Prepare(
		`SELECT dim_id FROM dim_version
		 WHERE entity_key = ? AND deleted = 0
		   AND effective_time <= ?
		   AND (end_time IS NULL OR end_time > ?)
		 ORDER BY effective_time DESC, dim_id DESC
		 LIMIT 1`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()
	if err := stmt.BindText(1, entityKey); err != nil {
		return 0, err
	}
	if err := stmt.BindInt64(2, instant); err != nil {
		return 0, err
	}
	if err := stmt.BindInt64(3, instant); err != nil {
		return 0, err
	}
	ok, err := stmt.Step()
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return stmt.Int64(0), nil
}

// repointCurrent refreshes the latest-restatement view. Every fact's
// current_dim_id becomes the live version in effect for its entity at its
// instant in the now-final dimension state; a fact no longer covered gets NULL.
//
// frozen_dim_id is deliberately left alone: it is the immutable record of what
// the fact was loaded against.
func (s *Store) repointCurrent() error {
	return s.db.Exec(`UPDATE dim_fact SET current_dim_id = (
		SELECT v.dim_id FROM dim_version AS v
		WHERE v.entity_key = dim_fact.entity_key
		  AND v.deleted = 0
		  AND v.effective_time <= dim_fact.effective_time
		  AND (v.end_time IS NULL OR v.end_time > dim_fact.effective_time)
		ORDER BY v.effective_time DESC, v.dim_id DESC
		LIMIT 1
	)`)
}

// dedupeBatchFacts collapses repeated fact ids inside one batch. Identical
// repeats are one fact; differing content under the same fact id is a conflict.
func dedupeBatchFacts(facts []Fact) ([]Fact, error) {
	seen := make(map[string]string, len(facts))
	out := make([]Fact, 0, len(facts))
	for _, f := range facts {
		hash, err := factContentHash(f)
		if err != nil {
			return nil, err
		}
		if prior, ok := seen[f.FactID]; ok {
			if prior != hash {
				return nil, fmt.Errorf("%w: %s (within batch)", ErrFactConflict, f.FactID)
			}
			continue
		}
		seen[f.FactID] = hash
		out = append(out, f)
	}
	return out, nil
}

// rejectDuplicatePoints enforces the "one point per key per instant" rule.
func rejectDuplicatePoints(points []Point) error {
	type key struct {
		entity  string
		instant int64
	}
	seen := make(map[key]struct{}, len(points))
	for _, p := range points {
		k := key{p.EntityKey, p.EffectiveTime}
		if _, ok := seen[k]; ok {
			return fmt.Errorf("%w: entity %q at %d", ErrDuplicatePoint, p.EntityKey, p.EffectiveTime)
		}
		seen[k] = struct{}{}
	}
	return nil
}

func nullableID(id int64) interface{} {
	if id == 0 {
		return nil
	}
	return id
}

func payloadArg(payload []byte) interface{} {
	if payload == nil {
		return nil
	}
	return payload
}
