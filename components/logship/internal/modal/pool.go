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
	"sync/atomic"

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
// Round-robin and nothing cleverer: every stream is one long-lived RPC of near-identical cost, so
// the only imbalance a smarter policy could fix is one that does not arise. Safe for concurrent use.
type Pool struct {
	creds Credentials

	mu      sync.RWMutex
	conns   []*grpc.ClientConn
	clients []LogsClient

	next atomic.Uint64
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
	}
	return nil
}

// Client returns the next connection's client. Callers hold on to it for the life of their stream:
// re-picking per RPC would move a re-opened stream to a different connection every 55 seconds and
// make the distribution drift.
func (p *Pool) Client() LogsClient {
	i := p.next.Add(1) - 1

	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.clients[i%uint64(len(p.clients))]
}

// Len is the number of connections, which is what a caller checks a stream count against.
func (p *Pool) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.conns)
}

func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	errs := make([]error, 0, len(p.conns))
	for _, c := range p.conns {
		errs = append(errs, c.Close())
	}
	p.conns, p.clients = nil, nil
	return errors.Join(errs...)
}
