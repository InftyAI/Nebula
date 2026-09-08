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
			c, _ := p.Client() // not released: all 300 streams are live at once
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

func TestPool_HoldsTheCapWhileItGrows(t *testing.T) {
	// The fleet's real sequence: Reserve widens the pool BEFORE each instance's streams open, two
	// streams per instance. Modulo round-robin passed the one-connection case and then drifted, because
	// a pool that gains a connection re-partitions the residues without moving the streams already
	// handed out — at 200 instances it left 208 streams on the first connection against a cap of 100.
	p, err := NewPool(Credentials{ServerURL: "http://localhost:1"}, len(Descriptors))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = p.Close() }()

	const instances = 200
	live := map[LogsClient]int{}
	for k := 1; k <= instances; k++ {
		if err := p.Grow(k * len(Descriptors)); err != nil {
			t.Fatalf("Grow(%d): %v", k*len(Descriptors), err)
		}
		for range len(Descriptors) {
			c, _ := p.Client() // held: every one of these streams is still running
			live[c]++
		}
	}

	want := instances * len(Descriptors) / StreamsPerConn
	if p.Len() != want || len(live) != want {
		t.Fatalf("%d connections and %d used, want %d of each", p.Len(), len(live), want)
	}
	for c, n := range live {
		if n > StreamsPerConn {
			t.Errorf("connection %p carries %d streams, over the %d cap", c, n, StreamsPerConn)
		}
	}
}

func TestPool_ReleaseFreesTheSlot(t *testing.T) {
	// Without a release the pool counts a finished stream forever, and since every retry builds a fresh
	// Source, a fleet that never grew would still walk its connections up past the cap.
	p, err := NewPool(Credentials{ServerURL: "http://localhost:1"}, 2*StreamsPerConn)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = p.Close() }()

	first, release := p.Client()
	if second, _ := p.Client(); second == first {
		t.Fatal("the second stream took the connection already carrying one")
	}
	release()
	if again, _ := p.Client(); again != first {
		t.Fatal("after a release the emptiest connection was not the one picked")
	}

	release()
	release()
	// Read directly: a double release must not invent capacity, and a negative count would make this
	// connection win every pick from here on — the exact overload the count exists to prevent.
	if p.live[0] != 1 {
		t.Fatalf("live[0] = %d after repeated releases, want 1", p.live[0])
	}
}

func TestNewPool_RejectsABadServerURL(t *testing.T) {
	if _, err := NewPool(Credentials{ServerURL: "localhost:443"}, 10); err == nil {
		t.Fatal("NewPool accepted a URL with no scheme")
	}
}
