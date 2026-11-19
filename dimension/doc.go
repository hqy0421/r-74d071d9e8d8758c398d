// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

// Package dimension loads Avro-encoded dimension points and facts into a SQLite
// database while preserving two different views of a dimension at once:
//
//   - The "as-of-load" view is rebuilt into a half-open SCD2 timeline keyed by
//     entity key and effective time. Each version row is immutable after it is
//     written.
//   - The "latest restatement" view is tracked by each fact's current_dim_id,
//     which is re-pointed whenever a late arriving point, correction, or delete
//     changes what the dimension says for that fact's effective instant.
//
// Facts additionally carry a frozen_dim_id, chosen once from the whole batch's
// final dimension state and never changed afterwards. A fact's frozen version
// is therefore a durable reference to the exact attributes it was loaded
// against, even if the dimension is later corrected.
//
// All writes for a batch, together with the batch bookkeeping and the
// checkpoint that makes the batch re-openable, are published in a single
// SQLite transaction. A failed batch leaves the previous state untouched.
//
// The package consumes bytes decoded by a real goavro Codec: callers supply the
// *goavro.Codec for their dimension-point and fact records, and the loader
// validates the decoded native values against the record schemas exported by
// this package.
package dimension

// PointSchema is the Avro schema of a dimension point record.
//
// A point is one observation about an entity at an instant:
//
//   - entity_key identifies the dimension member.
//   - effective_time is the instant the observation takes effect, as
//     timestamp-millis.
//   - op is the change type: "I" (insert), "U" (update) or "D" (delete).
//   - attrs carries the full attribute set in effect at effective_time. It is
//     ignored for "D" points.
//
// Points are ordered by effective_time and then by their position within the
// batch; two points for the same entity key at the same effective instant are
// rejected as ambiguous.
const PointSchema = `{
  "type": "record",
  "name": "DimensionPoint",
  "namespace": "com.linkedin.goavro.dimension",
  "fields": [
    {"name": "entity_key", "type": "string"},
    {"name": "effective_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "op", "type": {"type": "enum", "name": "DimensionOp", "symbols": ["I", "U", "D"]}},
    {"name": "attrs", "type": {"type": "map", "values": ["null", "string", "long", "double", "boolean", "bytes"]}, "default": {}}
  ]
}`

// FactSchema is the Avro schema of a fact record.
//
// A fact is an immutable event that references a dimension member by entity
// key at an effective instant:
//
//   - fact_id is the fact's stable identity; re-loading the same fact with the
//     same content is idempotent, while a different content for the same
//     fact_id is a conflict.
//   - entity_key names the dimension member the fact refers to.
//   - effective_time is the instant at which the fact observed the dimension,
//     as timestamp-millis.
//   - payload is opaque application data, stored verbatim.
const FactSchema = `{
  "type": "record",
  "name": "Fact",
  "namespace": "com.linkedin.goavro.dimension",
  "fields": [
    {"name": "fact_id", "type": "string"},
    {"name": "entity_key", "type": "string"},
    {"name": "effective_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "payload", "type": ["null", "bytes"], "default": null}
  ]
}`
