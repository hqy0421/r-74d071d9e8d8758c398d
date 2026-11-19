# dimension

`goavro/dimension` loads Avro-encoded dimension points and facts into SQLite
while keeping two views of a dimension at the same time.

- **As-of-load (SCD2)**: a half-open timeline `[effective_time, end_time)` per
  entity, rebuilt from `entity_key` / `effective_time`. Version rows are
  immutable once published.
- **Latest restatement**: each fact's `current_dim_id` is re-pointed whenever a
  late-arriving point, a correction, or a delete changes what the dimension says
  for that fact's instant.

Every fact also gets a `frozen_dim_id`, chosen once from the batch's *final*
dimension state and never modified afterwards. That reference, and the
attributes it points at, remain stable even if the dimension is later corrected.

## Records

The package exports two Avro schemas:

- `PointSchema` — `entity_key`, `effective_time` (timestamp-millis), `op`
  (`I` / `U` / `D`), `attrs` (`map` of `["null","string","long","double","boolean","bytes"]`).
- `FactSchema` — `fact_id`, `entity_key`, `effective_time`, `payload`
  (`["null","bytes"]`).

Points and facts are supplied as concatenated **binary** records and decoded by a
real `*goavro.Codec`. Use `NewPointCodec()` / `NewFactCodec()` if you do not
already have a codec for these shapes.

## Example

```go
store, err := dimension.Open("dim.db")
if err != nil {
    return err
}
defer store.Close()

pointCodec, _ := dimension.NewPointCodec()
factCodec, _ := dimension.NewFactCodec()

batch, err := store.Load(dimension.Config{
    PointCodec: pointCodec,
    FactCodec:  factCodec,
    PointBytes: pointBytes,
    FactBytes:  factBytes,
})
if err != nil {
    return err
}
```

## Semantics

- **One point per key per instant.** Two points for the same `entity_key` at the
  same `effective_time` in one batch are rejected (`ErrDuplicatePoint`).
- **Immutable versions.** A correction adds a new version row; the superseded
  row is closed to a zero-width interval `[t, t)` and stays readable for facts
  that froze it. It is never edited in place.
- **Reuse, don't duplicate.** A point whose content matches the live version at
  its instant reuses that version.
- **Freezing.** A new fact's `frozen_dim_id` is the version in effect at the
  fact's instant under the batch's final dimension state. Unmatched → `NULL`.
- **Restatement.** `current_dim_id` follows late arrivals, corrections, and
  deletes; it becomes `NULL` when no live version covers the instant.
- **Frozen versions cannot be deleted** (enforced by a SQLite trigger).
- **Idempotency.** Replaying an identical batch makes no writes and keeps the
  checkpoint. The same `fact_id` with different content is `ErrFactConflict`.
- **Atomicity.** Versions, the `current_dim_id` map, facts with both foreign
  keys, the batch row, and the checkpoint are published in one transaction. A
  failed batch leaves the previous state and checkpoint untouched.

## Requirements

The bundled SQLite binding needs `cgo` and the system `libsqlite3` shared
library (`libsqlite3.so.0`) on Linux. On other configurations the package still
compiles, but every database entry point returns an explicit error.
