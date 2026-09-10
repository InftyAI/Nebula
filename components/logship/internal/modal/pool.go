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
	"errors"
	"sync"

	"google.golang.org/grpc"
)

// StreamsPerConn is how many log streams share one connection.
//
// The number exists because of how gRPC-go fails here, not because of a measured optimum: HTTP/2
// caps concurrent streams per connection via SETTINGS_MAX_CONCURRENT_STREAMS, and gRPC-go *queues*
// RPCs past that cap rather than returning an error. On a single connection, 1,000 log streams would
// leave most of them waiting forever with nothing logged and no error surfaced anywhere — the worst
// possible shape for a component whose job is not losing logs. 100 is comfortably under the 100-ish
// that servers typically advertise while keeping the pool small enough that a connection failure
// takes out a bounded slice of the fleet.
const StreamsPerConn = 100

// Pool spreads log streams across several connections to Modal.
//
// Least-loaded, and every stream is one long-lived RPC of near-identical cost, so a count of live
// streams IS the load — which is why Client hands back a release. Round-robin will not do here: a
// modulo over a pool that has gained a connection re-partitions the residues without moving the
// streams already handed out, so the first connection keeps its whole historical share and ends up
// several times over StreamsPerConn, which is the silent-queueing case that constant exists to
// prevent. Safe for concurrent use.
type Pool struct {
	creds Credentials

	// One plain Mutex: picking a connection writes the count that made the pick correct, so there is
	// no read-only path left for an RWMutex to help.
	mu      sync.Mutex
	conns   []*grpc.ClientConn
	clients []LogsClient
	// live counts the streams holding each client, parallel to clients.
	live []int
}

// NewPool dials enough connections for streams. It rounds up, and always dials at least one.
//
// Grow it with Grow as the fleet grows; there is no fleet size to know here.
func NewPool(creds Credentials, streams int) (*Pool, error) {
	p := &Pool{creds: creds}
	if err := p.Grow(streams); err != nil {
		// Close what did open; a half-built pool leaks connections for the process's life.
		return nil, errors.Join(err, p.Close())
	}
	return p, nil
}

// Grow dials whatever streams needs beyond what the pool already has, and never shrinks.
//
// It must be called BEFORE the streams it accounts for are opened, which is the whole contract: a
// connection added afterwards cannot take over streams already queued behind HTTP/2's concurrency
// cap, and gRPC-go queues them silently rather than failing, so by the time the symptom is visible
// the stuck streams stay stuck. Growing ahead of demand is what keeps that case from arising —
// there is no cap to guess and no fleet size to configure, only the invariant that no connection is
// handed more than StreamsPerConn.
//
// Not shrinking is deliberate: connections are cheap (grpc.NewClient does not connect until an RPC
// needs it) and closing one under a live stream would end it.
func (p *Pool) Grow(streams int) error {
	want := (max(streams, 1) + StreamsPerConn - 1) / StreamsPerConn

	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.conns) < want {
		conn, client, err := Dial(p.creds)
		if err != nil {
			// Keep what did open: they are already carrying streams, and the caller's own error
			// path decides whether the instance that needed the new one ships.
			return err
		}
		p.conns = append(p.conns, conn)
		p.clients = append(p.clients, client)
		p.live = append(p.live, 0)
	}
	return nil
}

// Client picks the connection carrying the fewest streams, and returns it with the release that gives
// the slot back.
//
// Callers hold the client for the life of their stream — re-picking per RPC would move a re-opened
// stream to a different connection every 55 seconds and make the distribution drift — and must call
// release when that stream ends, or the pool never learns the connection is free again. Release is
// idempotent, so a defer that overlaps an error path cannot double-count a slot into existence.
//
// A pool whose every connection already sits at StreamsPerConn still hands out the least-bad one
// rather than failing. Grow is what reserves capacity ahead of demand; refusing here would abandon an
// instance's logs entirely over an overshoot whose actual cost is some queueing.
func (p *Pool) Client() (LogsClient, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()

	i := 0
	for j, n := range p.live {
		if n < p.live[i] {
			i = j
		}
	}
	p.live[i]++

	var once sync.Once
	return p.clients[i], func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			// Close empties live, so a stream ending after it has nothing left to give back.
			if i < len(p.live) && p.live[i] > 0 {
				p.live[i]--
			}
		})
	}
}

// Live is how many slots are handed out and not yet returned.
//
// It is what reserving capacity has to count against, because a cancelled stream keeps its slot until
// its RPC unwinds: any figure derived from a tracked-instance count runs ahead of this one during a
// teardown. See Provider.Reserve.
func (p *Pool) Live() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.live {
		n += c
	}
	return n
}

// Len is the number of connections, which is what a caller checks a stream count against.
func (p *Pool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	errs := make([]error, 0, len(p.conns))
	for _, c := range p.conns {
		errs = append(errs, c.Close())
	}
	p.conns, p.clients, p.live = nil, nil, nil
	return errors.Join(errs...)
}
