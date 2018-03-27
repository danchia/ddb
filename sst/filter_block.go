//    Copyright 2018 Google LLC
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package sst

type filterBlockBuilder struct {
	bloom *bloom
}

func newFilterBlockBuilder() *filterBlockBuilder {
	return &filterBlockBuilder{bloom: newBloom()}
}

// Append adds a key to the filter block.
func (b *filterBlockBuilder) Append(key string) {
	b.bloom.Add([]byte(key))
}

// Finish finishes building the filter block and returns a slice to its contents.
func (b *filterBlockBuilder) Finish() []byte {
	return b.bloom.Bytes()
}

type filterBlock struct {
	bloom *bloom
}

func newFilterBlock(b []byte) *filterBlock {
	return &filterBlock{
		bloom: newBloomFromBytes(b),
	}
}

func (b *filterBlock) Test(key string) bool {
	return b.bloom.Test([]byte(key))
}
