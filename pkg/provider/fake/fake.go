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

// Package fake implements a fully in-memory provider.Provider. It provisions
// nothing real: Provision records the claim in a map and reports the instance
// Running immediately, List/Get read that map, and Terminate forgets it. This
// lets the whole control-plane loop — webhook gate → placement → NodeClaim →
// virtual-kubelet Provision/List → Pod Running — run against a plain Kind
// cluster with no cloud credentials, which is exactly what the e2e suite needs.
//
// It is NOT registered by default. cmd/main.go wires it only when
// NEBULA_ENABLE_FAKE_PROVIDER=true, so it never registers in production even
// though it ships in the binary (see registerProviders).
package fake

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/provider/catalog"
)

// ProviderName is the fake provider's stable identifier. It is intentionally not
// one of the canonical NeoCloud names in pkg/provider (runpod, modal, ...): a
// NodePool referencing "fake" can only resolve when the fake is explicitly
// enabled, so a real cluster never places onto it by accident.
const ProviderName = "fake"

// compile-time assertion that Provider satisfies the interface.
var _ provider.Provider = (*Provider)(nil)

// Provider is the in-memory backend. Offerings/Name/MapAccelerator come from the
// embedded catalog.Base (fed a static in-memory catalog); the lifecycle methods
// operate on the guarded instances map.
type Provider struct {
	catalog.Base

	mu        sync.Mutex
	instances map[string]*provider.Instance // key: instance id
	nextID    int
}

// New returns a fake Provider whose catalog offers a small, fixed set of
// accelerators (so GPU Pods match in selectPlacement) plus CPU-only workloads,
// all as OnDemand.
func New() *Provider {
	return &Provider{
		Base:      catalog.Base{ProviderName: ProviderName, Catalog: fixedCatalog{}},
		instances: make(map[string]*provider.Instance),
	}
}

// Capabilities reports a plain OnDemand-only, tag-bearing backend with no
// preemption — the simplest shape, so the control plane exercises its default
// paths.
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		SupportsStop:     false,
		SupportsSpot:     false,
		NativeTags:       true,
		PreemptionNotice: 0,
		PollInterval:     0, // use the vnode default cadence
	}
}

// Provision records one instance for the claim and reports it Running at once.
// Idempotent on ClaimName: a repeat returns the existing instance's id rather
// than creating a second (matching the real adapters' contract).
func (p *Provider) Provision(_ context.Context, pod *corev1.Pod, req provider.ProvisionRequest) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("fake: nil pod")
	}
	if req.ClaimName == "" {
		return "", fmt.Errorf("fake: empty ClaimName in ProvisionRequest")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, inst := range p.instances {
		if inst.ClaimName == req.ClaimName {
			return inst.ID, nil // idempotent reuse
		}
	}

	p.nextID++
	id := fmt.Sprintf("fake-%d", p.nextID)
	p.instances[id] = &provider.Instance{
		ID:           id,
		ClaimName:    req.ClaimName,
		State:        provider.InstanceRunning,
		Endpoint:     fmt.Sprintf("fake://%s", id),
		CapacityType: req.CapacityType,
	}
	return id, nil
}

// Terminate forgets the instance. Idempotent: terminating an already-gone (or
// never-created) instance returns nil.
func (p *Provider) Terminate(_ context.Context, instanceID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.instances, instanceID)
	return nil
}

// Get returns one instance, or (nil, nil) if it no longer exists.
func (p *Provider) Get(_ context.Context, instanceID string) (*provider.Instance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	inst, ok := p.instances[instanceID]
	if !ok {
		return nil, nil
	}
	out := *inst // copy so callers can't mutate our map entry
	return &out, nil
}

// List returns every instance this fake currently holds.
func (p *Provider) List(_ context.Context) ([]provider.Instance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]provider.Instance, 0, len(p.instances))
	for _, inst := range p.instances {
		out = append(out, *inst)
	}
	return out, nil
}

// ClassifyProvisionError never really fails to provision, so any error it is
// asked to classify is treated as a whole-provider block on OnDemand — the same
// shared derivation the real adapters use.
func (p *Provider) ClassifyProvisionError(err error, accelerator, _ string) provider.BlockScope {
	// Region is ignored: the fake is region-simple, matching the OnDemand NeoClouds.
	return provider.ClassifyError(err, nebulav1alpha1.CapacityOnDemand, accelerator)
}

// fixedCatalog is a trivial catalog.Lookup: a static set of OnDemand offerings
// so a GPU Pod matches the fake in selectPlacement (via the embedded Base's
// MapAccelerator) without loading any CSV.
type fixedCatalog struct{}

func (fixedCatalog) Offerings(string) []provider.Offering {
	return []provider.Offering{
		{AcceleratorType: "H100", CapacityType: nebulav1alpha1.CapacityOnDemand, PricePerHour: 1, Available: true},
		{AcceleratorType: "A100-80GB", CapacityType: nebulav1alpha1.CapacityOnDemand, PricePerHour: 1, Available: true},
	}
}
