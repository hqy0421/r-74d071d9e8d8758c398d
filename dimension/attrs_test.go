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
	"bytes"
	"math"
	"reflect"
	"testing"
)

func TestCanonicalAttrs_Deterministic(t *testing.T) {
	a := map[string]interface{}{"b": int64(2), "a": "x", "c": true}
	b := map[string]interface{}{"c": true, "a": "x", "b": int64(2)}
	blobA, hashA, err := canonicalAttrs(a)
	if err != nil {
		t.Fatal(err)
	}
	blobB, hashB, err := canonicalAttrs(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(blobA, blobB) || hashA != hashB {
		t.Fatalf("map iteration order leaked into encoding")
	}
}

func TestCanonicalAttrs_RoundTrip(t *testing.T) {
	cases := []map[string]interface{}{
		nil,
		{},
		{"s": "x", "l": int64(-1), "f": 1.25, "b": false, "n": nil, "raw": []byte{1, 2}},
	}
	for _, want := range cases {
		blob, _, err := canonicalAttrs(want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeAttrs(blob)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip: want %#v, got %#v", want, got)
		}
	}
}

func TestCanonicalAttrs_NaNIsNormalised(t *testing.T) {
	nan1 := math.Float64frombits(0x7ff8000000000001)
	nan2 := math.Float64frombits(0x7ff8000000000002)
	_, h1, _ := canonicalAttrs(map[string]interface{}{"x": nan1})
	_, h2, _ := canonicalAttrs(map[string]interface{}{"x": nan2})
	if h1 != h2 {
		t.Fatalf("NaN payloads should hash equally")
	}
}

func TestCanonicalAttrs_TypeTagsDistinguishValues(t *testing.T) {
	pairs := [][2]interface{}{
		{int64(1), float64(1)},
		{int64(1), "1"},
		{"a", []byte("a")},
		{false, int64(0)},
		{nil, ""},
	}
	for _, p := range pairs {
		_, h1, _ := canonicalAttrs(map[string]interface{}{"x": p[0]})
		_, h2, _ := canonicalAttrs(map[string]interface{}{"x": p[1]})
		if h1 == h2 {
			t.Fatalf("%T(%v) and %T(%v) should not hash equally", p[0], p[0], p[1], p[1])
		}
	}
}

func TestDecodeAttrs_RejectsMalformed(t *testing.T) {
	cases := [][]byte{
		{0x05, 0x01},            // claims 5 entries, too few bytes
		{0x01, 0x05, 'a'},       // key length past end
		{0x01, 0x01, 'a', 0x7f}, // unknown tag
		{0x00, 0x01},            // trailing bytes
		{0x01, 0x01, 'a', 0x02}, // truncated long
	}
	for _, c := range cases {
		if _, err := decodeAttrs(c); err == nil {
			t.Fatalf("expected error for %v", c)
		}
	}
}

func TestUnwrapAttrValues(t *testing.T) {
	raw := map[string]interface{}{
		"a": map[string]interface{}{"string": "x"},
		"b": map[string]interface{}{"long": int64(5)},
		"c": nil,
		"d": map[string]interface{}{"bytes": []byte{1}},
	}
	got, err := unwrapAttrValues(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got["a"] != "x" || got["b"] != int64(5) || got["c"] != nil {
		t.Fatalf("%#v", got)
	}
	if _, ok := got["d"].([]byte); !ok {
		t.Fatalf("bytes: %#v", got["d"])
	}
	if _, err := unwrapAttrValues(map[string]interface{}{"x": map[string]interface{}{"a": 1, "b": 2}}); err == nil {
		t.Fatal("expected multi-member union error")
	}
}
