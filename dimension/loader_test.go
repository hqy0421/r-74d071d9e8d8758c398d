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
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
)

// helpers

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "dim.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustPointCodec(t *testing.T) *goavro.Codec {
	t.Helper()
	c, err := goavro.NewCodec(PointSchema)
	if err != nil {
		t.Fatalf("point codec: %v", err)
	}
	return c
}

func mustFactCodec(t *testing.T) *goavro.Codec {
	t.Helper()
	c, err := goavro.NewCodec(FactSchema)
	if err != nil {
		t.Fatalf("fact codec: %v", err)
	}
	return c
}

// pointDatum builds a native map that the real goavro codec encodes.
func pointDatum(key string, millis int64, op string, attrs map[string]interface{}) map[string]interface{} {
	wrapped := map[string]interface{}{}
	for k, v := range attrs {
		switch x := v.(type) {
		case string:
			wrapped[k] = goavro.Union("string", x)
		case int:
			wrapped[k] = goavro.Union("long", int64(x))
		case int64:
			wrapped[k] = goavro.Union("long", x)
		case float64:
			wrapped[k] = goavro.Union("double", x)
		case bool:
			wrapped[k] = goavro.Union("boolean", x)
		case []byte:
			wrapped[k] = goavro.Union("bytes", x)
		case nil:
			wrapped[k] = nil
		default:
			panic(fmt.Sprintf("unsupported attr %T", v))
		}
	}
	return map[string]interface{}{
		"entity_key":     key,
		"effective_time": time.UnixMilli(millis).UTC(),
		"op":             op,
		"attrs":          wrapped,
	}
}

func factDatum(id, key string, millis int64, payload []byte) map[string]interface{} {
	var p interface{}
	if payload != nil {
		p = goavro.Union("bytes", payload)
	}
	return map[string]interface{}{
		"fact_id":        id,
		"entity_key":     key,
		"effective_time": time.UnixMilli(millis).UTC(),
		"payload":        p,
	}
}

func encodeAll(t *testing.T, codec *goavro.Codec, datums ...map[string]interface{}) []byte {
	t.Helper()
	var out []byte
	for _, d := range datums {
		b, err := codec.BinaryFromNative(nil, d)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		out = append(out, b...)
	}
	return out
}

// load is a convenience that encodes the datums and loads them in one batch.
func load(t *testing.T, s *Store, points []map[string]interface{}, facts []map[string]interface{}) Batch {
	t.Helper()
	return loadWithCheckpoint(t, s, points, facts, nil)
}

func loadWithCheckpoint(t *testing.T, s *Store, points, facts []map[string]interface{}, expected *int64) Batch {
	t.Helper()
	pc := mustPointCodec(t)
	fc := mustFactCodec(t)
	batch, err := s.Load(Config{
		PointCodec:         pc,
		FactCodec:          fc,
		PointBytes:         encodeAll(t, pc, points...),
		FactBytes:          encodeAll(t, fc, facts...),
		ExpectedCheckpoint: expected,
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return batch
}

func attrsOf(t *testing.T, s *Store, key string, instant int64) map[string]interface{} {
	t.Helper()
	v, err := s.DimensionAt(key, instant)
	if err != nil {
		t.Fatalf("DimensionAt: %v", err)
	}
	if v == nil {
		return nil
	}
	return v.Attrs
}

func int64Ptr(v int64) *int64 { return &v }

// tests

func TestLoad_PointCreatesOpenVersion(t *testing.T) {
	s := newTestStore(t)
	start := int64(1000)
	load(t, s,
		[]map[string]interface{}{pointDatum("e1", start, "I", map[string]interface{}{"name": "ann"})},
		nil)

	got := attrsOf(t, s, "e1", start)
	if got["name"] != "ann" {
		t.Fatalf("attrs at instant: %#v", got)
	}
	vs, err := s.Versions("e1")
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 1 {
		t.Fatalf("want 1 version, got %d", len(vs))
	}
	if !vs[0].OpenEnded {
		t.Fatalf("first version should be open ended: %#v", vs[0])
	}
	if vs[0].EffectiveTime != start {
		t.Fatalf("effective time: %d", vs[0].EffectiveTime)
	}
}

func TestLoad_LateArrivingPointRebuildsHalfOpenIntervals(t *testing.T) {
	s := newTestStore(t)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 200, "I", map[string]interface{}{"name": "second"}),
	}, nil)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"name": "first"}),
	}, nil)

	vs, err := s.Versions("e1")
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 2 {
		t.Fatalf("want 2 versions, got %d: %#v", len(vs), vs)
	}
	if vs[0].EffectiveTime != 100 || vs[0].EndTime != 200 || vs[0].OpenEnded {
		t.Fatalf("first interval wrong: %#v", vs[0])
	}
	if vs[1].EffectiveTime != 200 || !vs[1].OpenEnded {
		t.Fatalf("second interval wrong: %#v", vs[1])
	}
	// Half-open: 199 is first, 200 is second.
	if got := attrsOf(t, s, "e1", 199)["name"]; got != "first" {
		t.Fatalf("at 199: %v", got)
	}
	if got := attrsOf(t, s, "e1", 200)["name"]; got != "second" {
		t.Fatalf("at 200: %v", got)
	}
}

func TestLoad_UnchangedVersionIsReusedNotDuplicated(t *testing.T) {
	s := newTestStore(t)
	attrs := map[string]interface{}{"name": "ann"}
	load(t, s, []map[string]interface{}{pointDatum("e1", 100, "I", attrs)}, nil)
	before, _ := s.Versions("e1")

	load(t, s, []map[string]interface{}{pointDatum("e1", 100, "U", attrs)}, nil)
	after, _ := s.Versions("e1")

	if len(before) != 1 || len(after) != 1 {
		t.Fatalf("versions before=%d after=%d", len(before), len(after))
	}
	if before[0].DimID != after[0].DimID {
		t.Fatalf("unchanged version should be reused: %d vs %d", before[0].DimID, after[0].DimID)
	}
}

func TestLoad_CorrectionAtSameInstantAddsVersionAndKeepsOldForFrozen(t *testing.T) {
	s := newTestStore(t)
	// Fact frozen against the first version at instant 100.
	b1 := load(t, s,
		[]map[string]interface{}{pointDatum("e1", 100, "I", map[string]interface{}{"name": "orig"})},
		[]map[string]interface{}{factDatum("f1", "e1", 150, []byte("p"))})
	_ = b1
	frozen, err := s.Fact("f1")
	if err != nil {
		t.Fatal(err)
	}
	if frozen.FrozenAttrs["name"] != "orig" {
		t.Fatalf("frozen attrs: %#v", frozen.FrozenAttrs)
	}
	origDim := frozen.FrozenDimID

	// Correct the same instant.
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "U", map[string]interface{}{"name": "fixed"}),
	}, nil)

	frozen2, err := s.Fact("f1")
	if err != nil {
		t.Fatal(err)
	}
	if frozen2.FrozenDimID != origDim {
		t.Fatalf("frozen ref moved: %d -> %d", origDim, frozen2.FrozenDimID)
	}
	if frozen2.FrozenAttrs["name"] != "orig" {
		t.Fatalf("frozen attrs changed: %#v", frozen2.FrozenAttrs)
	}
	if frozen2.CurrentAttrs["name"] != "fixed" {
		t.Fatalf("current attrs should follow restatement: %#v", frozen2.CurrentAttrs)
	}
	if frozen2.CurrentDimID == origDim {
		t.Fatalf("current should have been re-pointed")
	}

	// Both versions still exist and read back their own attributes.
	vs, _ := s.Versions("e1")
	if len(vs) != 2 {
		t.Fatalf("want 2 versions (superseded + live), got %d", len(vs))
	}
	if attrsOf(t, s, "e1", 100)["name"] != "fixed" {
		t.Fatalf("dimension view should show correction")
	}
}

func TestLoad_DeleteMovesCurrentToNULLAndKeepsFrozen(t *testing.T) {
	s := newTestStore(t)
	load(t, s,
		[]map[string]interface{}{pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"})},
		[]map[string]interface{}{factDatum("f1", "e1", 150, nil)})

	fresh, _ := s.Fact("f1")
	frozenDim := fresh.FrozenDimID
	if frozenDim == 0 {
		t.Fatal("fact should have frozen a version")
	}

	load(t, s, []map[string]interface{}{
		pointDatum("e1", 200, "D", nil),
	}, nil)

	// Fact at 150 remains inside the live interval? No: deletion at 200 closes
	// the prior version at 200, so 150 still resolves. The fact's current must
	// stay on the original version.
	after, _ := s.Fact("f1")
	if after.FrozenDimID != frozenDim {
		t.Fatalf("frozen moved on delete: %d -> %d", frozenDim, after.FrozenDimID)
	}
	if after.CurrentDimID != frozenDim {
		t.Fatalf("current at 150 should still resolve: %d", after.CurrentDimID)
	}

	// A fact after the deletion has no dimension row.
	load(t, s, nil, []map[string]interface{}{factDatum("f2", "e1", 250, nil)})
	late, _ := s.Fact("f2")
	if late.FrozenDimID != 0 || late.CurrentDimID != 0 {
		t.Fatalf("post-delete fact should be NULL: frozen=%d current=%d", late.FrozenDimID, late.CurrentDimID)
	}

	// Entity's current row is gone.
	cur, err := s.CurrentDimID("e1")
	if err != nil {
		t.Fatal(err)
	}
	if cur != 0 {
		t.Fatalf("current dim after delete: %d", cur)
	}

	// The deleted version is preserved and the frozen one is still readable.
	vs, _ := s.Versions("e1")
	if len(vs) != 2 {
		t.Fatalf("want 2 versions, got %d", len(vs))
	}
	if !vs[1].Deleted {
		t.Fatalf("expected tombstone: %#v", vs[1])
	}
}

func TestLoad_DeleteThenReinsertRepointsCurrent(t *testing.T) {
	s := newTestStore(t)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"}),
	}, nil)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 200, "D", nil),
	}, nil)
	first, _ := s.CurrentDimID("e1")
	_ = first
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 300, "I", map[string]interface{}{"name": "ann2"}),
	}, nil)

	cur, err := s.CurrentDimID("e1")
	if err != nil {
		t.Fatal(err)
	}
	if cur == 0 {
		t.Fatal("reinsert should restore a current row")
	}
	v, _ := s.DimensionAt("e1", 300)
	if v == nil || v.Attrs["name"] != "ann2" {
		t.Fatalf("reinsert attrs: %#v", v)
	}
}

func TestLoad_DuplicatePointRejected(t *testing.T) {
	s := newTestStore(t)
	pc := mustPointCodec(t)
	_, err := s.Load(Config{
		PointCodec: pc,
		PointBytes: encodeAll(t, pc,
			pointDatum("e1", 100, "I", map[string]interface{}{"v": 1}),
			pointDatum("e1", 100, "I", map[string]interface{}{"v": 2})),
	})
	if err == nil {
		t.Fatal("expected duplicate point error")
	}
	if !isErr(err, ErrDuplicatePoint) {
		t.Fatalf("wrong error: %v", err)
	}
	// Nothing was published.
	cp, _ := s.Checkpoint()
	if cp != 0 {
		t.Fatalf("checkpoint advanced on rejected batch: %d", cp)
	}
}

func TestLoad_FactIdempotentAndConflict(t *testing.T) {
	s := newTestStore(t)
	points := []map[string]interface{}{pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"})}
	facts := []map[string]interface{}{factDatum("f1", "e1", 150, []byte("same"))}

	b1 := load(t, s, points, facts)
	if !b1.Published {
		t.Fatalf("first load should publish")
	}
	b2 := load(t, s, points, facts)
	if b2.Published {
		t.Fatalf("identical replay should not publish")
	}
	if b2.Checkpoint != b1.Checkpoint {
		t.Fatalf("replay checkpoint changed: %d -> %d", b1.Checkpoint, b2.Checkpoint)
	}

	// Same fact_id, different payload: conflict, and nothing published.
	pc, fc := mustPointCodec(t), mustFactCodec(t)
	_, err := s.Load(Config{
		PointCodec: pc,
		FactCodec:  fc,
		FactBytes:  encodeAll(t, fc, factDatum("f1", "e1", 150, []byte("different"))),
	})
	if err == nil || !isErr(err, ErrFactConflict) {
		t.Fatalf("expected fact conflict, got %v", err)
	}
	cp, _ := s.Checkpoint()
	if cp != b2.Checkpoint {
		t.Fatalf("checkpoint advanced on conflict: %d", cp)
	}
}

func TestLoad_FactFrozenAgainstWholeBatchFinalState(t *testing.T) {
	// A fact and a later point for the same entity in one batch: the frozen
	// version must be the one in effect at the fact's instant under the final
	// dimension state, not the pre-batch state.
	s := newTestStore(t)
	load(t, s, nil, nil) // initialise

	load(t, s,
		[]map[string]interface{}{
			pointDatum("e1", 100, "I", map[string]interface{}{"name": "old"}),
			pointDatum("e1", 200, "U", map[string]interface{}{"name": "new"}),
		},
		[]map[string]interface{}{factDatum("f1", "e1", 250, nil)})

	f, err := s.Fact("f1")
	if err != nil {
		t.Fatal(err)
	}
	if f.FrozenAttrs["name"] != "new" {
		t.Fatalf("frozen should see final batch state: %#v", f.FrozenAttrs)
	}
}

func TestLoad_FactBeforeAnyVersionIsNull(t *testing.T) {
	s := newTestStore(t)
	load(t, s,
		nil,
		[]map[string]interface{}{factDatum("f1", "missing", 100, nil)})
	f, err := s.Fact("f1")
	if err != nil {
		t.Fatal(err)
	}
	if f.FrozenDimID != 0 {
		t.Fatalf("unmatched fact should freeze NULL, got %d", f.FrozenDimID)
	}

	load(t, s, []map[string]interface{}{
		pointDatum("missing", 200, "I", map[string]interface{}{"name": "arrived"}),
	}, nil)
	f2, _ := s.Fact("f1")
	if f2.FrozenDimID != 0 {
		t.Fatalf("frozen must never change: %d", f2.FrozenDimID)
	}
	// The earlier fact is still before the entity's earliest version, so its
	// current view is also NULL.
	if f2.CurrentDimID != 0 {
		t.Fatalf("current before earliest version should be NULL: %d", f2.CurrentDimID)
	}
}

func TestLoad_VersionAttributesImmutableInPlace(t *testing.T) {
	s := newTestStore(t)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"}),
	}, nil)

	// Attempting to rewrite a version's attributes through the raw connection
	// must fail: corrections add rows, they do not edit history.
	if err := s.db.Exec(`UPDATE dim_version SET attrs_hash = 'tampered'`); err == nil {
		t.Fatal("expected immutability trigger to reject attribute update")
	}
	if err := s.db.Exec(`UPDATE dim_version SET effective_time = 999`); err == nil {
		t.Fatal("expected immutability trigger to reject effective_time update")
	}
	// end_time bookkeeping is allowed because it is not an attribute.
	if err := s.db.Exec(`UPDATE dim_version SET end_time = 500`); err != nil {
		t.Fatalf("end_time update should be allowed: %v", err)
	}
}

func TestLoad_FrozenVersionCannotBeDeleted(t *testing.T) {
	s := newTestStore(t)
	load(t, s,
		[]map[string]interface{}{pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"})},
		[]map[string]interface{}{factDatum("f1", "e1", 150, nil)})
	f, _ := s.Fact("f1")

	if err := s.exec(`DELETE FROM dim_version WHERE dim_id = ?`, f.FrozenDimID); err == nil {
		t.Fatal("expected frozen version delete to be rejected")
	}
	// A version that is neither frozen nor the entity's current row may be
	// removed by maintenance.
	load(t, s, []map[string]interface{}{
		pointDatum("e2", 100, "I", map[string]interface{}{"name": "one"}),
	}, nil)
	load(t, s, []map[string]interface{}{
		pointDatum("e2", 200, "U", map[string]interface{}{"name": "two"}),
	}, nil)
	vs, _ := s.Versions("e2")
	if len(vs) != 2 {
		t.Fatalf("want 2 versions for e2, got %d", len(vs))
	}
	superseded := vs[0].DimID // e2@100 is no longer current
	if err := s.exec(`DELETE FROM dim_version WHERE dim_id = ?`, superseded); err != nil {
		t.Fatalf("superseded, unfrozen version delete should succeed: %v", err)
	}
}

func TestLoad_StaleCheckpointRejected(t *testing.T) {
	s := newTestStore(t)
	b := load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"}),
	}, nil)
	if b.Checkpoint != 1 {
		t.Fatalf("first checkpoint: %d", b.Checkpoint)
	}
	pc, fc := mustPointCodec(t), mustFactCodec(t)
	_, err := s.Load(Config{
		PointCodec:         pc,
		FactCodec:          fc,
		PointBytes:         encodeAll(t, pc, pointDatum("e2", 100, "I", map[string]interface{}{"name": "bob"})),
		ExpectedCheckpoint: int64Ptr(0),
	})
	if err == nil || !isErr(err, ErrStaleCheckpoint) {
		t.Fatalf("expected stale checkpoint, got %v", err)
	}
	cp, _ := s.Checkpoint()
	if cp != 1 {
		t.Fatalf("checkpoint changed: %d", cp)
	}
}

func TestLoad_FailureLeavesPreviousStateAndCheckpoint(t *testing.T) {
	s := newTestStore(t)
	load(t, s,
		[]map[string]interface{}{pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"})},
		[]map[string]interface{}{factDatum("f1", "e1", 150, []byte("p"))})
	before, _ := s.Versions("e1")
	cpBefore, _ := s.Checkpoint()

	// A batch whose fact conflicts must roll back its points too.
	pc, fc := mustPointCodec(t), mustFactCodec(t)
	_, err := s.Load(Config{
		PointCodec: pc,
		FactCodec:  fc,
		PointBytes: encodeAll(t, pc, pointDatum("e1", 200, "U", map[string]interface{}{"name": "changed"})),
		FactBytes:  encodeAll(t, fc, factDatum("f1", "e1", 150, []byte("conflict"))),
	})
	if err == nil {
		t.Fatal("expected conflict")
	}
	after, _ := s.Versions("e1")
	if len(after) != len(before) {
		t.Fatalf("versions changed on failed batch: %d -> %d", len(before), len(after))
	}
	cpAfter, _ := s.Checkpoint()
	if cpAfter != cpBefore {
		t.Fatalf("checkpoint changed on failed batch: %d -> %d", cpBefore, cpAfter)
	}
	// The failed batch's point at 200 must not have materialised a version; the
	// instant is still covered by the earlier open-ended version.
	if v, _ := s.DimensionAt("e1", 200); v == nil || v.EffectiveTime != 100 {
		t.Fatalf("point from failed batch leaked: %#v", v)
	}
}

func TestLoad_ReopenIsCorrect(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dim.db")

	s1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	load(t, s1,
		[]map[string]interface{}{pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"})},
		[]map[string]interface{}{factDatum("f1", "e1", 150, nil)})
	cp1, _ := s1.Checkpoint()
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	cp2, err := s2.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if cp2 != cp1 {
		t.Fatalf("checkpoint after reopen: %d != %d", cp2, cp1)
	}
	f, err := s2.Fact("f1")
	if err != nil {
		t.Fatal(err)
	}
	if f.FrozenAttrs["name"] != "ann" {
		t.Fatalf("fact not durable: %#v", f.FrozenAttrs)
	}
	// Batches are also replay-safe after reopen.
	b := load(t, s2,
		[]map[string]interface{}{pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"})},
		[]map[string]interface{}{factDatum("f1", "e1", 150, nil)})
	if b.Published {
		t.Fatal("replay after reopen should be idempotent")
	}
	if b.Checkpoint != cp1 {
		t.Fatalf("replay after reopen changed checkpoint: %d", b.Checkpoint)
	}
}

func TestLoad_BatchAndCheckpointAtomic(t *testing.T) {
	s := newTestStore(t)
	b := load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"}),
	}, []map[string]interface{}{factDatum("f1", "e1", 150, nil)})
	if b.Checkpoint != 1 || !b.Published || b.Points != 1 || b.Facts != 1 {
		t.Fatalf("batch: %#v", b)
	}
	cp, _ := s.Checkpoint()
	if cp != 1 {
		t.Fatalf("checkpoint: %d", cp)
	}
	if b.ID == "" {
		t.Fatal("batch id should be content derived")
	}
}

func TestLoad_MultipleEntitiesAndInstants(t *testing.T) {
	s := newTestStore(t)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"v": 1}),
		pointDatum("e2", 100, "I", map[string]interface{}{"v": 2}),
		pointDatum("e1", 200, "U", map[string]interface{}{"v": 3}),
	}, []map[string]interface{}{
		factDatum("f1", "e1", 150, nil),
		factDatum("f2", "e2", 250, nil),
	})

	if got := attrsOf(t, s, "e1", 150)["v"]; got != int64(1) {
		t.Fatalf("e1@150: %v", got)
	}
	if got := attrsOf(t, s, "e1", 250)["v"]; got != int64(3) {
		t.Fatalf("e1@250: %v", got)
	}
	if got := attrsOf(t, s, "e2", 250)["v"]; got != int64(2) {
		t.Fatalf("e2@250: %v", got)
	}

	f1, _ := s.Fact("f1")
	f2, _ := s.Fact("f2")
	if f1.FrozenDimID == f2.FrozenDimID {
		t.Fatal("facts on different entities should freeze different versions")
	}
}

func TestLoad_AttributeTypesRoundTrip(t *testing.T) {
	s := newTestStore(t)
	attrs := map[string]interface{}{
		"str":  "hello",
		"int":  int64(42),
		"f":    3.5,
		"bool": true,
		"blob": []byte{0, 1, 2},
		"null": nil,
		"nan":  math.NaN(),
	}
	load(t, s, []map[string]interface{}{pointDatum("e1", 100, "I", attrs)}, nil)
	got := attrsOf(t, s, "e1", 100)
	if got["str"] != "hello" || got["int"] != int64(42) || got["f"] != 3.5 || got["bool"] != true {
		t.Fatalf("attrs: %#v", got)
	}
	if b, ok := got["blob"].([]byte); !ok || len(b) != 3 || b[2] != 2 {
		t.Fatalf("blob: %#v", got["blob"])
	}
	if got["null"] != nil {
		t.Fatalf("null: %#v", got["null"])
	}
	if f, ok := got["nan"].(float64); !ok || !math.IsNaN(f) {
		t.Fatalf("nan: %#v", got["nan"])
	}
}

func TestLoad_EmptyAttrsDistinctFromNil(t *testing.T) {
	// nil and empty attribute maps are different states; a point that omits
	// attrs is not the same as one that sets it to {}.
	blobNil, hashNil, err := canonicalAttrs(nil)
	if err != nil {
		t.Fatal(err)
	}
	blobEmpty, hashEmpty, err := canonicalAttrs(map[string]interface{}{})
	if err != nil {
		t.Fatal(err)
	}
	if hashNil == hashEmpty {
		t.Fatal("nil and empty attrs should not share a hash")
	}
	if string(blobNil) == string(blobEmpty) {
		t.Fatal("nil and empty attrs should not share an encoding")
	}
}

func isErr(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestLoad_LateInsertOpensANewCurrentInterval(t *testing.T) {
	s := newTestStore(t)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 300, "I", map[string]interface{}{"v": 3}),
	}, nil)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"v": 1}),
	}, nil)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 200, "I", map[string]interface{}{"v": 2}),
	}, nil)

	vs, err := s.Versions("e1")
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 3 {
		t.Fatalf("want 3 versions, got %d", len(vs))
	}
	want := [][2]int64{{100, 200}, {200, 300}, {300, 0}}
	for i, w := range want {
		if vs[i].EffectiveTime != w[0] {
			t.Fatalf("version %d start: %d != %d", i, vs[i].EffectiveTime, w[0])
		}
		if w[1] == 0 {
			if !vs[i].OpenEnded {
				t.Fatalf("version %d should be open", i)
			}
		} else if vs[i].OpenEnded || vs[i].EndTime != w[1] {
			t.Fatalf("version %d end: %#v", i, vs[i])
		}
	}
	for instant, wantV := range map[int64]int64{100: 1, 199: 1, 200: 2, 299: 2, 300: 3, 9999: 3} {
		if got := attrsOf(t, s, "e1", instant)["v"]; got != wantV {
			t.Fatalf("at %d: got %v want %d", instant, got, wantV)
		}
	}
}

func TestLoad_DeletionSplitsIntervalAndCurrentBecomesNull(t *testing.T) {
	s := newTestStore(t)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"}),
		pointDatum("e1", 300, "U", map[string]interface{}{"name": "ann3"}),
	}, nil)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 200, "D", nil),
	}, nil)

	if v, _ := s.DimensionAt("e1", 150); v == nil || v.Attrs["name"] != "ann" {
		t.Fatalf("before delete: %#v", v)
	}
	if v, _ := s.DimensionAt("e1", 200); v != nil {
		t.Fatalf("at delete instant should be nil: %#v", v)
	}
	if v, _ := s.DimensionAt("e1", 250); v != nil {
		t.Fatalf("inside deleted interval should be nil: %#v", v)
	}
	if v, _ := s.DimensionAt("e1", 300); v == nil || v.Attrs["name"] != "ann3" {
		t.Fatalf("after reinsert: %#v", v)
	}
	// current row points at the latest live version.
	cur, _ := s.CurrentDimID("e1")
	v, _ := s.DimensionAt("e1", 300)
	if cur == 0 || v == nil || cur != v.DimID {
		t.Fatalf("current=%d want %v", cur, v)
	}
}

func TestLoad_DeleteLatestMakesCurrentNull(t *testing.T) {
	s := newTestStore(t)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"}),
	}, nil)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 200, "D", nil),
	}, nil)
	cur, _ := s.CurrentDimID("e1")
	if cur != 0 {
		t.Fatalf("current after latest delete: %d", cur)
	}
}

func TestLoad_BatchDuplicateFactIds(t *testing.T) {
	s := newTestStore(t)
	pc, fc := mustPointCodec(t), mustFactCodec(t)

	// Identical repeats collapse to one fact.
	b, err := s.Load(Config{
		PointCodec: pc,
		FactCodec:  fc,
		PointBytes: encodeAll(t, pc, pointDatum("e1", 100, "I", map[string]interface{}{"v": 1})),
		FactBytes: encodeAll(t, fc,
			factDatum("f1", "e1", 150, []byte("p")),
			factDatum("f1", "e1", 150, []byte("p"))),
	})
	if err != nil {
		t.Fatalf("identical repeats should be idempotent: %v", err)
	}
	if b.Facts != 1 {
		t.Fatalf("want 1 fact, got %d", b.Facts)
	}

	// Conflicting repeats are rejected.
	_, err = s.Load(Config{
		PointCodec: pc,
		FactCodec:  fc,
		FactBytes: encodeAll(t, fc,
			factDatum("f2", "e1", 150, []byte("a")),
			factDatum("f2", "e1", 150, []byte("b"))),
	})
	if err == nil || !isErr(err, ErrFactConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
}

func TestLoad_FactFrozenIsStableAcrossManyCorrections(t *testing.T) {
	s := newTestStore(t)
	load(t, s,
		[]map[string]interface{}{pointDatum("e1", 100, "I", map[string]interface{}{"name": "v0"})},
		[]map[string]interface{}{factDatum("f1", "e1", 150, nil)})
	f0, _ := s.Fact("f1")

	for i, name := range []string{"v1", "v2", "v3"} {
		load(t, s, []map[string]interface{}{
			pointDatum("e1", 100, "U", map[string]interface{}{"name": name}),
		}, nil)
		f, err := s.Fact("f1")
		if err != nil {
			t.Fatal(err)
		}
		if f.FrozenDimID != f0.FrozenDimID || f.FrozenAttrs["name"] != "v0" {
			t.Fatalf("correction %d moved frozen: %#v", i, f)
		}
		if f.CurrentAttrs["name"] != name {
			t.Fatalf("correction %d current: %#v", i, f.CurrentAttrs)
		}
	}

	// All frozen versions remain readable; superseded ones are zero-width.
	vs, _ := s.Versions("e1")
	zeroWidth := 0
	for _, v := range vs {
		if v.DimID == f0.FrozenDimID {
			if !v.OpenEnded && v.EndTime != v.EffectiveTime {
				t.Fatalf("superseded frozen version should be zero-width: %#v", v)
			}
			if v.Attrs["name"] != "v0" {
				t.Fatalf("frozen version attrs mutated: %#v", v.Attrs)
			}
		}
		if !v.OpenEnded && v.EndTime == v.EffectiveTime {
			zeroWidth++
		}
	}
	if zeroWidth != 3 {
		t.Fatalf("want 3 superseded versions, got %d", zeroWidth)
	}
	if len(vs) != 4 {
		t.Fatalf("want 4 rows (1 live + 3 superseded), got %d", len(vs))
	}
}

func TestLoad_RepeatedSameContentAcrossBatchesIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	points := []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"v": 1}),
		pointDatum("e1", 200, "U", map[string]interface{}{"v": 2}),
	}
	b1 := load(t, s, points, nil)
	vs1, _ := s.Versions("e1")
	b2 := load(t, s, points, nil)
	vs2, _ := s.Versions("e1")
	if b1.Checkpoint != b2.Checkpoint {
		t.Fatalf("checkpoint moved: %d %d", b1.Checkpoint, b2.Checkpoint)
	}
	if len(vs1) != len(vs2) {
		t.Fatalf("versions changed on replay: %d %d", len(vs1), len(vs2))
	}
}

func TestLoad_DimensionAtBeforeFirstVersionIsNil(t *testing.T) {
	s := newTestStore(t)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"v": 1}),
	}, nil)
	if v, _ := s.DimensionAt("e1", 99); v != nil {
		t.Fatalf("half-open start violated: %#v", v)
	}
	if v, _ := s.DimensionAt("unknown", 100); v != nil {
		t.Fatalf("unknown entity: %#v", v)
	}
}

func TestLoad_ConfigValidation(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Load(Config{PointBytes: []byte{1, 2, 3}}); err == nil {
		t.Fatal("expected error for bytes without codec")
	}
	if _, err := s.Load(Config{FactBytes: []byte{1, 2, 3}}); err == nil {
		t.Fatal("expected error for fact bytes without codec")
	}
	pc := mustPointCodec(t)
	if _, err := s.Load(Config{PointCodec: pc, PointBytes: []byte{0xff, 0xff}}); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestLoad_DeleteIgnoresAttributePayload(t *testing.T) {
	// Two delete points for the same key/instant that differ only in the
	// attributes they carry are the same state; a delete must not become two
	// versions just because of an irrelevant payload.
	s := newTestStore(t)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"name": "ann"}),
		pointDatum("e1", 200, "D", nil),
	}, nil)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 200, "D", map[string]interface{}{"ignored": "x"}),
	}, nil)
	vs, _ := s.Versions("e1")
	deleted := 0
	for _, v := range vs {
		if v.Deleted {
			deleted++
		}
	}
	if deleted != 1 {
		t.Fatalf("want exactly one tombstone version, got %d (versions=%#v)", deleted, vs)
	}
}

func TestLoad_ReorderedReplayIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	pc, fc := mustPointCodec(t), mustFactCodec(t)
	p := []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"v": 1}),
		pointDatum("e2", 100, "I", map[string]interface{}{"v": 2}),
	}
	f := []map[string]interface{}{
		factDatum("f1", "e1", 150, nil),
		factDatum("f2", "e2", 150, nil),
	}
	b1, err := s.Load(Config{PointCodec: pc, FactCodec: fc,
		PointBytes: encodeAll(t, pc, p...), FactBytes: encodeAll(t, fc, f...)})
	if err != nil {
		t.Fatal(err)
	}
	// Same logical content, records presented in a different order.
	b2, err := s.Load(Config{PointCodec: pc, FactCodec: fc,
		PointBytes: encodeAll(t, pc, p[1], p[0]),
		FactBytes:  encodeAll(t, fc, f[1], f[0])})
	if err != nil {
		t.Fatal(err)
	}
	if b2.Published || b2.Checkpoint != b1.Checkpoint {
		t.Fatalf("reordered replay should be idempotent: %#v vs %#v", b1, b2)
	}
}
