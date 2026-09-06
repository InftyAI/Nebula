/*
Copyright 2026 The InftyAI Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package modal

import (
	"sync"
	"testing"
)

func TestNewPool_SizesFromTheStreamCount(t *testing.T) {
	// grpc.NewClient does not connect, so this exercises the arithmetic without a server. Rounding
	// up matters: 1,000 streams over 100 per connection is 10, and 1,001 must not be 10 as well.
	for _, tc := range []struct{ streams, want int }{
		{0, 1}, {1, 1}, {100, 1}, {101, 2}, {1000, 10}, {1001, 11},
	} {
		p, err := NewPool(Credentials{ServerURL: "http://localhost:1"}, tc.streams)
		if err != nil {
			t.Fatalf("NewPool(%d): %v", tc.streams, err)
		}
		if p.Len() != tc.want {
			t.Errorf("NewPool(%d).Len() = %d, want %d", tc.streams, p.Len(), tc.want)
		}
		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

func TestPool_SpreadsStreamsEvenly(t *testing.T) {
	// The point of the pool: 1,000 streams on one connection stall silently, so an uneven hand-out
	// would reproduce the bug on a subset.
	p, err := NewPool(Credentials{ServerURL: "http://localhost:1"}, 300)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = p.Close() }()

	counts := map[LogsClient]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 300 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := p.Client()
			mu.Lock()
			counts[c]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(counts) != 3 {
		t.Fatalf("used %d of 3 connections", len(counts))
	}
	for c, n := range counts {
		if n != 100 {
			t.Errorf("connection %p took %d streams, want 100", c, n)
		}
	}
}

func TestNewPool_RejectsABadServerURL(t *testing.T) {
	if _, err := NewPool(Credentials{ServerURL: "localhost:443"}, 10); err == nil {
		t.Fatal("NewPool accepted a URL with no scheme")
	}
}
