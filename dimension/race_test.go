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
	"sync"
	"testing"
)

func TestStore_ConcurrentReadsWhileLoading(t *testing.T) {
	s := newTestStore(t)
	load(t, s, []map[string]interface{}{
		pointDatum("e1", 100, "I", map[string]interface{}{"v": 1}),
	}, []map[string]interface{}{factDatum("f1", "e1", 150, nil)})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if _, err := s.DimensionAt("e1", 150); err != nil {
					t.Errorf("DimensionAt: %v", err)
					return
				}
				if _, err := s.Fact("f1"); err != nil {
					t.Errorf("Fact: %v", err)
					return
				}
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				instant := int64(200 + i*100 + j)
				pc := mustPointCodec(t)
				_, err := s.Load(Config{
					PointCodec: pc,
					PointBytes: encodeAll(t, pc, pointDatum("e1", instant, "U",
						map[string]interface{}{"v": instant})),
				})
				if err != nil {
					t.Errorf("Load: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestStore_ConcurrentLoadsSerialise(t *testing.T) {
	s := newTestStore(t)
	pc := mustPointCodec(t)
	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.Load(Config{
				PointCodec: pc,
				PointBytes: encodeAll(t, pc, pointDatum(fmt.Sprintf("e%d", i), 100, "I",
					map[string]interface{}{"v": i})),
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Load: %v", err)
	}
	cp, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if cp != n {
		t.Fatalf("checkpoint after %d batches: %d", n, cp)
	}
}
