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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
)

// Canonical attribute tags. They are part of the on-disk format: changing them
// changes every stored attribute hash, so treat them as a schema version.
const (
	tagNull    byte = 0
	tagString  byte = 1
	tagLong    byte = 2
	tagDouble  byte = 3
	tagBoolean byte = 4
	tagBytes   byte = 5
)

// canonicalAttrs serialises an attribute map deterministically: keys are
// sorted, and every value is written with an explicit type tag. The result,
// together with its SHA-256, is what the loader stores and compares. A nil map
// encodes to the empty byte slice so it is distinct from an empty map (which
// encodes to a single zero varint).
func canonicalAttrs(attrs map[string]interface{}) ([]byte, string, error) {
	if attrs == nil {
		sum := sha256.Sum256(nil)
		return nil, hex.EncodeToString(sum[:]), nil
	}
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	buf := make([]byte, 0, 1+len(attrs)*16)
	buf = appendUvarint(buf, uint64(len(keys)))
	for _, k := range keys {
		buf = appendUvarint(buf, uint64(len(k)))
		buf = append(buf, k...)
		var err error
		buf, err = appendAttrValue(buf, attrs[k])
		if err != nil {
			return nil, "", fmt.Errorf("attribute %q: %w", k, err)
		}
	}
	sum := sha256.Sum256(buf)
	return buf, hex.EncodeToString(sum[:]), nil
}

func appendAttrValue(buf []byte, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return append(buf, tagNull), nil
	case string:
		buf = append(buf, tagString)
		buf = appendUvarint(buf, uint64(len(v)))
		return append(buf, v...), nil
	case int64:
		buf = append(buf, tagLong)
		var raw [8]byte
		binary.BigEndian.PutUint64(raw[:], uint64(v))
		return append(buf, raw[:]...), nil
	case float64:
		buf = append(buf, tagDouble)
		var raw [8]byte
		binary.BigEndian.PutUint64(raw[:], math.Float64bits(normalizeNaN(v)))
		return append(buf, raw[:]...), nil
	case bool:
		if v {
			return append(buf, tagBoolean, 1), nil
		}
		return append(buf, tagBoolean, 0), nil
	case []byte:
		buf = append(buf, tagBytes)
		buf = appendUvarint(buf, uint64(len(v)))
		return append(buf, v...), nil
	default:
		return nil, fmt.Errorf("unsupported attribute type %T", value)
	}
}

// appendUvarint appends v in SQLite/Avro-style base-128 varint form.
func appendUvarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	return append(buf, byte(v))
}

// normalizeNaN makes all NaN payloads compare equal so that re-encoding a value
// that travelled through a decoder cannot change its content hash.
func normalizeNaN(f float64) float64 {
	if math.IsNaN(f) {
		return math.Float64frombits(0x7ff8000000000000)
	}
	return f
}

// decodeAttrs reverses canonicalAttrs. nil input decodes to a nil map.
func decodeAttrs(blob []byte) (map[string]interface{}, error) {
	if len(blob) == 0 {
		return nil, nil
	}
	count, n := binary.Uvarint(blob)
	if n <= 0 {
		return nil, fmt.Errorf("invalid attribute blob: bad map length")
	}
	blob = blob[n:]
	if count > uint64(len(blob)) { // each entry needs at least a key length varint
		return nil, fmt.Errorf("invalid attribute blob: map length %d exceeds payload", count)
	}
	attrs := make(map[string]interface{}, count)
	for i := uint64(0); i < count; i++ {
		keyLen, n := binary.Uvarint(blob)
		if n <= 0 {
			return nil, fmt.Errorf("invalid attribute blob: bad key length")
		}
		blob = blob[n:]
		if keyLen > uint64(len(blob)) {
			return nil, fmt.Errorf("invalid attribute blob: key length %d exceeds payload", keyLen)
		}
		key := string(blob[:keyLen])
		blob = blob[keyLen:]

		value, rest, err := decodeAttrValue(blob)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", key, err)
		}
		attrs[key] = value
		blob = rest
	}
	if len(blob) != 0 {
		return nil, fmt.Errorf("invalid attribute blob: %d trailing bytes", len(blob))
	}
	return attrs, nil
}

func decodeAttrValue(blob []byte) (interface{}, []byte, error) {
	if len(blob) == 0 {
		return nil, nil, fmt.Errorf("invalid attribute blob: missing type tag")
	}
	tag := blob[0]
	blob = blob[1:]
	switch tag {
	case tagNull:
		return nil, blob, nil
	case tagString:
		raw, rest, err := readSizedBytes(blob)
		if err != nil {
			return nil, nil, err
		}
		return string(raw), rest, nil
	case tagLong:
		if len(blob) < 8 {
			return nil, nil, fmt.Errorf("invalid attribute blob: truncated long")
		}
		return int64(binary.BigEndian.Uint64(blob[:8])), blob[8:], nil
	case tagDouble:
		if len(blob) < 8 {
			return nil, nil, fmt.Errorf("invalid attribute blob: truncated double")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(blob[:8])), blob[8:], nil
	case tagBoolean:
		if len(blob) < 1 {
			return nil, nil, fmt.Errorf("invalid attribute blob: truncated boolean")
		}
		if blob[0] > 1 {
			return nil, nil, fmt.Errorf("invalid attribute blob: invalid boolean %d", blob[0])
		}
		return blob[0] == 1, blob[1:], nil
	case tagBytes:
		raw, rest, err := readSizedBytes(blob)
		if err != nil {
			return nil, nil, err
		}
		out := make([]byte, len(raw))
		copy(out, raw)
		return out, rest, nil
	default:
		return nil, nil, fmt.Errorf("invalid attribute blob: unknown type tag %d", tag)
	}
}

func readSizedBytes(blob []byte) ([]byte, []byte, error) {
	length, n := binary.Uvarint(blob)
	if n <= 0 {
		return nil, nil, fmt.Errorf("invalid attribute blob: bad value length")
	}
	blob = blob[n:]
	if length > uint64(len(blob)) {
		return nil, nil, fmt.Errorf("invalid attribute blob: value length %d exceeds payload", length)
	}
	return blob[:length], blob[length:], nil
}

// unwrapAttrValues converts the union-wrapped values produced by a goavro map
// decoder into the plain values stored by the loader.
func unwrapAttrValues(raw map[string]interface{}) (map[string]interface{}, error) {
	if raw == nil {
		return nil, nil
	}
	out := make(map[string]interface{}, len(raw))
	for key, value := range raw {
		unwrapped, err := unwrapUnionValue(value)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", key, err)
		}
		out[key] = unwrapped
	}
	return out, nil
}

func unwrapUnionValue(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	wrapped, ok := value.(map[string]interface{})
	if !ok {
		// Tolerate already-unwrapped native values.
		switch value.(type) {
		case string, int64, float64, bool, []byte:
			return value, nil
		default:
			return nil, fmt.Errorf("unsupported attribute type %T", value)
		}
	}
	if len(wrapped) != 1 {
		return nil, fmt.Errorf("union value ought to have exactly one member, got %d", len(wrapped))
	}
	for member, inner := range wrapped {
		switch member {
		case "null":
			return nil, nil
		case "string", "long", "double", "boolean", "bytes":
			return inner, nil
		default:
			return nil, fmt.Errorf("unsupported union member %q", member)
		}
	}
	return nil, fmt.Errorf("empty union value")
}
