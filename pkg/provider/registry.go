package provider

import (
	"fmt"
	"sort"
	"sync"
)

// The canonical provider names for the v1 NeoCloud set. These are the stable
// identifiers used everywhere a provider is referenced by string: a
// Provider.Name() return value, a NodePool.spec.providers[].name entry, and the
// ProviderLabel value on each provider's virtual node. Keeping them as consts in
// one place stops those three call sites from drifting apart on a typo.
//
// AWS is the first hyperscaler (region-aware) backend; GCP/Azure are a planned
// near-term expansion and will be added here when their adapters land, with
// nothing else about the registry changing.
const (
	ProviderRunPod     = "runpod"
	ProviderModal      = "modal"
	ProviderCoreWeave  = "coreweave"
	ProviderLambda     = "lambda"
	ProviderKubernetes = "kubernetes"
	ProviderAWS        = "aws"
)

// registry is the process-wide map of registered provider backends, keyed by
// Provider.Name(). It is populated at startup (typically each adapter package
// calls Register from an init or a wiring function) and read concurrently by the
// placement controller, the poll loop and the NodeClaim controller, so it is
// guarded by a mutex.
var registry = struct {
	sync.RWMutex
	m map[string]Provider
}{m: make(map[string]Provider)}

// Register adds a provider backend under its Name(). It panics on a duplicate
// name or a nil provider, because both are programmer errors that must surface
// at startup rather than as a silently-missing provider at placement time.
func Register(p Provider) {
	if p == nil {
		panic("provider: Register called with nil provider")
	}
	name := p.Name()
	if name == "" {
		panic("provider: Register called with empty provider name")
	}

	registry.Lock()
	defer registry.Unlock()
	if _, dup := registry.m[name]; dup {
		panic(fmt.Sprintf("provider: duplicate registration for %q", name))
	}
	registry.m[name] = p
}

// Get returns the registered provider for name, or ok=false if none is
// registered. Callers on the control-plane hot path (e.g. resolving a NodePool
// provider ref) should treat ok=false as a configuration error for that pool,
// not a fatal one — other pools may still be serviceable.
func Get(name string) (p Provider, ok bool) {
	registry.RLock()
	defer registry.RUnlock()
	p, ok = registry.m[name]
	return p, ok
}

// Names returns the sorted names of all registered providers. Sorted so that
// callers (logs, status, tests) get a stable, deterministic order.
func Names() []string {
	registry.RLock()
	defer registry.RUnlock()
	names := make([]string, 0, len(registry.m))
	for name := range registry.m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
