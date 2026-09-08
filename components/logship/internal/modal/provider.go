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
	"fmt"

	"github.com/InftyAI/Nebula/components/logship/internal/ship"
)

// Provider reads Modal sandbox logs. It implements the log provider port; see internal/provider.
//
// One per process, holding the pool every sandbox's streams are spread over — which is why it is a
// long-lived object rather than something built per instance.
type Provider struct {
	pool *Pool
}

// Open reads the tokens from the environment and builds the pool at its smallest useful size, one
// sandbox's worth. Reserve widens it from there.
func Open() (*Provider, error) {
	creds, err := CredentialsFromEnv()
	if err != nil {
		return nil, err
	}
	pool, err := NewPool(creds, len(Descriptors))
	if err != nil {
		return nil, err
	}
	return &Provider{pool: pool}, nil
}

func (p *Provider) Streams() []string { return Descriptors }

// Source picks the connection this stream keeps for its life. See Pool.Client for why it is not
// re-picked per RPC, and Source.Follow for where the slot goes back.
func (p *Provider) Source(instanceID, stream string) (ship.Source, error) {
	fd, err := ParseDescriptor(stream)
	if err != nil {
		// Before Client, so a name that maps to no descriptor cannot leak a slot.
		return nil, err
	}
	client, release := p.pool.Client()
	return Source{Client: client, Sandbox: instanceID, FD: fd, release: release}, nil
}

// Reserve widens the pool for instances sandboxes. See Pool.Grow: the ordering — before the streams
// open — is the whole contract, and growing past what is needed costs an unconnected socket.
//
// Floored at what the pool is already carrying plus this instance's own streams, because an instance
// count is not that: Supervisor.Forget drops an instance before its streams release their slots, so a
// replacement arriving while the pool sits exactly on a connection boundary would open into a pool
// sized as if the departing streams were already gone. A floor and not a lock — concurrent Ensures can
// still overshoot, which Pool.Client tolerates on purpose.
//
// The connection count rides in the error because the caller reports the failure and does not
// otherwise know the shape of what failed.
func (p *Provider) Reserve(instances int) error {
	streams := max(instances*len(Descriptors), p.pool.Live()+len(Descriptors))
	if err := p.pool.Grow(streams); err != nil {
		return fmt.Errorf("widening the pool past its %d connections: %w", p.pool.Len(), err)
	}
	return nil
}

func (p *Provider) Close() error { return p.pool.Close() }
