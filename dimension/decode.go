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
	"time"

	"github.com/linkedin/goavro/v2"
)

// NewPointCodec returns a goavro codec for PointSchema, suitable for
// Config.PointCodec and for decoding caller-supplied point bytes.
func NewPointCodec() (*goavro.Codec, error) {
	return goavro.NewCodec(PointSchema)
}

// NewFactCodec returns a goavro codec for FactSchema, suitable for
// Config.FactCodec and for decoding caller-supplied fact bytes.
func NewFactCodec() (*goavro.Codec, error) {
	return goavro.NewCodec(FactSchema)
}

// decodePoints decodes a concatenation of binary point records.
func decodePoints(codec *goavro.Codec, blob []byte) ([]Point, error) {
	if codec == nil || len(blob) == 0 {
		return nil, nil
	}
	var points []Point
	remaining := blob
	for len(remaining) > 0 {
		native, rest, err := codec.NativeFromBinary(remaining)
		if err != nil {
			return nil, fmt.Errorf("dimension: cannot decode point %d: %w", len(points)+1, err)
		}
		if len(rest) >= len(remaining) {
			return nil, fmt.Errorf("dimension: point %d decoder made no progress", len(points)+1)
		}
		remaining = rest

		point, err := pointFromNative(native)
		if err != nil {
			return nil, fmt.Errorf("dimension: point %d: %w", len(points)+1, err)
		}
		points = append(points, point)
	}
	return points, nil
}

func pointFromNative(native interface{}) (Point, error) {
	record, ok := native.(map[string]interface{})
	if !ok {
		return Point{}, fmt.Errorf("expected record, got %T", native)
	}
	entityKey, err := stringField(record, "entity_key")
	if err != nil {
		return Point{}, err
	}
	instant, err := timeField(record, "effective_time")
	if err != nil {
		return Point{}, err
	}
	op, err := stringField(record, "op")
	if err != nil {
		return Point{}, err
	}
	switch op {
	case "I", "U", "D":
	default:
		return Point{}, fmt.Errorf("field %q: unsupported operation %q", "op", op)
	}
	attrs, err := attrsField(record, "attrs")
	if err != nil {
		return Point{}, err
	}
	return Point{EntityKey: entityKey, EffectiveTime: instant, Op: op, Attrs: attrs}, nil
}

// decodeFacts decodes a concatenation of binary fact records.
func decodeFacts(codec *goavro.Codec, blob []byte) ([]Fact, error) {
	if codec == nil || len(blob) == 0 {
		return nil, nil
	}
	var facts []Fact
	remaining := blob
	for len(remaining) > 0 {
		native, rest, err := codec.NativeFromBinary(remaining)
		if err != nil {
			return nil, fmt.Errorf("dimension: cannot decode fact %d: %w", len(facts)+1, err)
		}
		if len(rest) >= len(remaining) {
			return nil, fmt.Errorf("dimension: fact %d decoder made no progress", len(facts)+1)
		}
		remaining = rest

		fact, err := factFromNative(native)
		if err != nil {
			return nil, fmt.Errorf("dimension: fact %d: %w", len(facts)+1, err)
		}
		facts = append(facts, fact)
	}
	return facts, nil
}

func factFromNative(native interface{}) (Fact, error) {
	record, ok := native.(map[string]interface{})
	if !ok {
		return Fact{}, fmt.Errorf("expected record, got %T", native)
	}
	factID, err := stringField(record, "fact_id")
	if err != nil {
		return Fact{}, err
	}
	entityKey, err := stringField(record, "entity_key")
	if err != nil {
		return Fact{}, err
	}
	instant, err := timeField(record, "effective_time")
	if err != nil {
		return Fact{}, err
	}
	payload, err := bytesUnionField(record, "payload")
	if err != nil {
		return Fact{}, err
	}
	return Fact{FactID: factID, EntityKey: entityKey, EffectiveTime: instant, Payload: payload}, nil
}

func stringField(record map[string]interface{}, name string) (string, error) {
	value, ok := record[name]
	if !ok {
		return "", fmt.Errorf("missing field %q", name)
	}
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("field %q: expected string, got %T", name, value)
	}
	return s, nil
}

// timeField reads a timestamp-millis field, accepting either the time.Time the
// logical type decodes to or a raw integer for codecs that decode as long.
func timeField(record map[string]interface{}, name string) (int64, error) {
	value, ok := record[name]
	if !ok {
		return 0, fmt.Errorf("missing field %q", name)
	}
	switch v := value.(type) {
	case time.Time:
		return v.UnixNano() / int64(time.Millisecond), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("field %q: expected timestamp, got %T", name, value)
	}
}

func attrsField(record map[string]interface{}, name string) (map[string]interface{}, error) {
	value, ok := record[name]
	if !ok || value == nil {
		return nil, nil
	}
	raw, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("field %q: expected map, got %T", name, value)
	}
	return unwrapAttrValues(raw)
}

// bytesUnionField reads a ["null","bytes"] union, accepting either the union
// wrapper that goavro produces or a bare []byte.
func bytesUnionField(record map[string]interface{}, name string) ([]byte, error) {
	value, ok := record[name]
	if !ok || value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	case map[string]interface{}:
		if len(v) != 1 {
			return nil, fmt.Errorf("field %q: union ought to have one member, got %d", name, len(v))
		}
		for member, inner := range v {
			if member == "null" {
				return nil, nil
			}
			if member != "bytes" {
				return nil, fmt.Errorf("field %q: unsupported union member %q", name, member)
			}
			switch b := inner.(type) {
			case []byte:
				return b, nil
			case string:
				return []byte(b), nil
			default:
				return nil, fmt.Errorf("field %q: expected bytes, got %T", name, inner)
			}
		}
	}
	return nil, fmt.Errorf("field %q: expected bytes union, got %T", name, value)
}
