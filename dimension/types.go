// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package dimension

// Point is a decoded dimension point.
type Point struct {
	EntityKey     string
	EffectiveTime int64 // milliseconds since the Unix epoch
	Op            string
	Attrs         map[string]interface{}
}

// Fact is a decoded fact.
type Fact struct {
	FactID        string
	EntityKey     string
	EffectiveTime int64
	Payload       []byte
}
