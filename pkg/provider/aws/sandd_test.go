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

package aws

import (
	"context"
	"errors"
	"testing"

	"github.com/InftyAI/Nebula/pkg/provider"
)

// fakeMinter is a provider.DaemonKeyMinter that returns a canned key (or error) and
// counts calls, so tests can assert a FRESH key is minted per provision.
type fakeMinter struct {
	key  string
	err  error
	call int
}

func (m *fakeMinter) MintDaemonKey(_ context.Context) (string, error) {
	m.call++
	if m.err != nil {
		return "", m.err
	}
	return m.key, nil
}

// TestProvision_MintsPerDaemonKey: with a KeyMinter wired, each Provision mints a
// fresh key and that minted key (not the static one) is what lands on the spec.
func TestProvision_MintsPerDaemonKey(t *testing.T) {
	f := &fakeClient{runID: "i-mint"}
	p := newTestProvider(f)
	minter := &fakeMinter{key: "tskey-fresh-123"}
	p.sandd = provider.SanddConfig{
		AuthKey:       "tskey-STATIC-should-not-be-used",
		ControlServer: "http://headscale:8080",
		ServerURL:     "ws://ctrl.nebula.mesh:8765/ws",
		KeyMinter:     minter,
	}

	if _, err := p.Provision(context.Background(), gpuPod("H100", 8), provider.ProvisionRequest{
		ClaimName: "claim-mint",
		Region:    testRegion,
	}); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	if minter.call != 1 {
		t.Fatalf("minter called %d times, want 1", minter.call)
	}
	// The minted key, not the static AuthKey, must be baked onto the spec.
	if got := f.lastSpec.Sandd.AuthKey; got != "tskey-fresh-123" {
		t.Fatalf("spec AuthKey = %q, want the minted tskey-fresh-123", got)
	}
	// The minter seam must NOT ride onto the spec — only the resolved key travels.
	if f.lastSpec.Sandd.KeyMinter != nil {
		t.Fatalf("spec KeyMinter should be nil after resolution")
	}
	// The rest of the SandD config rides through unchanged.
	if f.lastSpec.Sandd.ControlServer != "http://headscale:8080" {
		t.Fatalf("ControlServer = %q", f.lastSpec.Sandd.ControlServer)
	}
}

// TestProvision_SeparateProvisionsMintSeparateKeys: two DISTINCT workloads => two
// mints, so each gets its OWN single-use credential (the isolation guarantee).
// Distinct claim names avoid the idempotency short-circuit (which would return the
// existing instance without launching or minting again).
func TestProvision_SeparateProvisionsMintSeparateKeys(t *testing.T) {
	f := &fakeClient{runID: "i-x"}
	p := newTestProvider(f)
	minter := &fakeMinter{key: "tskey-k"}
	p.sandd = provider.SanddConfig{ControlServer: "http://h:8080", KeyMinter: minter}

	for _, claim := range []string{"claim-1", "claim-2"} {
		if _, err := p.Provision(context.Background(), gpuPod("H100", 8), provider.ProvisionRequest{
			ClaimName: claim,
			Region:    testRegion,
		}); err != nil {
			t.Fatalf("Provision %s: %v", claim, err)
		}
	}
	if minter.call != 2 {
		t.Fatalf("minter called %d times, want 2 (one fresh key per workload)", minter.call)
	}
}

// TestProvision_MintErrorAbortsProvision: a mint failure must abort provisioning
// (no instance launched), not silently inject a keyless daemon.
func TestProvision_MintErrorAbortsProvision(t *testing.T) {
	f := &fakeClient{runID: "i-nope"}
	p := newTestProvider(f)
	p.sandd = provider.SanddConfig{
		ControlServer: "http://h:8080",
		KeyMinter:     &fakeMinter{err: errors.New("broker down")},
	}

	_, err := p.Provision(context.Background(), gpuPod("H100", 8), provider.ProvisionRequest{
		ClaimName: "claim-err",
		Region:    testRegion,
	})
	if err == nil {
		t.Fatalf("Provision succeeded, want mint error")
	}
	if f.runCnt != 0 {
		t.Fatalf("RunInstance called %d times, want 0 (provision must abort before launch)", f.runCnt)
	}
}

// TestProvision_NoMinterInjectsNothing: with no KeyMinter, SandD is off — nothing is
// minted and no auth key lands on the spec (the daemon is not injected).
func TestProvision_NoMinterInjectsNothing(t *testing.T) {
	f := &fakeClient{runID: "i-off"}
	p := newTestProvider(f)
	p.sandd = provider.SanddConfig{
		ControlServer: "http://h:8080",
	}

	if _, err := p.Provision(context.Background(), gpuPod("H100", 8), provider.ProvisionRequest{
		ClaimName: "claim-off",
		Region:    testRegion,
	}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if f.lastSpec.Sandd.Enabled() {
		t.Fatalf("SandD should be disabled with no KeyMinter, got enabled spec %+v", f.lastSpec.Sandd)
	}
	if got := f.lastSpec.Sandd.AuthKey; got != "" {
		t.Fatalf("spec AuthKey = %q, want empty (no key minted)", got)
	}
}
