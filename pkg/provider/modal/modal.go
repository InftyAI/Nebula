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

// Package modal implements the provider.Provider interface for Modal
// (https://modal.com), a serverless GPU compute platform.
//
// Modal's shape drives several adapter decisions:
//   - Lifecycle is create/terminate only. A Modal Sandbox is spun up and later
//     terminated; there is no stop/resume, so Capabilities.SupportsStop=false.
//   - Modal does not expose a user-facing spot/preemptible tier, so
//     SupportsSpot=false. The optimizer therefore only ever sends OnDemand
//     ProvisionRequests here (the NodePool capacity-tier loop skips Spot for
//     providers that don't advertise it).
//   - Modal Sandboxes carry native tags, so NativeTags=true and the ClaimName
//     is stored as a tag rather than smuggled into the instance name.
//   - There is no preemption push; detection is poll-based like every provider.
//
// The concrete Modal API (auth, sandbox create/terminate/list) lives behind the
// Client seam so this package holds only provider-agnostic translation and is
// unit-testable without network access. A real Client wrapping Modal's API is
// wired in separately.
package modal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/provider/catalog"
	"github.com/InftyAI/Nebula/pkg/util"
)

// defaultSandboxTimeout is the maximum lifetime the adapter sets when a Pod does
// not pin its own activeDeadlineSeconds. Modal requires a bounded timeout (a zero
// value silently means its 5-minute default, which would kill a real workload),
// so we default high — 24h — for a long-running GPU job. A Pod that wants a
// different ceiling sets spec.activeDeadlineSeconds, which maps straight through.
const defaultSandboxTimeout = 24 * time.Hour

// compile-time assertion that Provider satisfies the interface.
var _ provider.Provider = (*Provider)(nil)

// Client is the narrow seam over Modal's API. It is intentionally small: only
// the operations the adapter needs, expressed in provider-agnostic terms, so a
// real implementation (Modal SDK/HTTP) and a fake (tests) are interchangeable.
type Client interface {
	// CreateSandbox launches one sandbox from spec and returns its Modal id.
	CreateSandbox(ctx context.Context, spec SandboxSpec) (id string, err error)
	// TerminateSandbox terminates a sandbox by id. Must be idempotent:
	// terminating an already-gone sandbox returns nil.
	TerminateSandbox(ctx context.Context, id string) error
	// GetSandbox returns one sandbox, or (nil, nil) if it no longer exists.
	GetSandbox(ctx context.Context, id string) (*Sandbox, error)
	// ListSandboxes returns every Nebula-owned sandbox, filtered by the tag the
	// adapter sets at create time, in as few calls as possible.
	ListSandboxes(ctx context.Context) ([]Sandbox, error)
}

// SandboxSpec is the resolved, Modal-shaped request the Client turns into a
// sandbox. The adapter builds it from the Pod (source of truth) plus the
// resolved accelerator id.
type SandboxSpec struct {
	// Image is the container image, from the Pod's first container.
	Image string
	// Command is the container command+args, from the Pod.
	Command []string
	// Env is the environment, flattened from the Pod's container env.
	Env map[string]string
	// GPU is Modal's accelerator identifier (e.g. "H100", "A100-80GB"), or ""
	// for a CPU-only sandbox.
	GPU string
	// GPUCount is how many accelerators to attach (0 for CPU-only).
	GPUCount int32
	// CPU is the requested cores (fractional, physical), from the Pod's first
	// container resource request. Zero lets Modal apply its own default.
	CPU float64
	// MemoryMiB is the requested memory in MiB, from the Pod's request. Zero lets
	// Modal apply its own default.
	MemoryMiB int
	// Ports are the container ports to expose as encrypted tunnels, from the Pod's
	// containerPorts. The reachable endpoint (reported as the Pod's address) is a
	// tunnel to one of these, so a Pod that declares no port has no endpoint.
	Ports []int
	// Timeout is the sandbox's maximum lifetime. It MUST be non-zero: Modal treats
	// a zero timeout as its 5-minute default, which would terminate a real
	// workload almost immediately. The adapter always sets it (from the Pod's
	// activeDeadlineSeconds, else a long default).
	Timeout time.Duration
	// Tags carry Nebula identity; ClaimTagKey holds the NodeClaim name.
	Tags map[string]string
	// ReadinessProbe, when non-nil, is the Pod's first-container readinessProbe
	// carried through so the Client can configure Modal's own readiness probe at
	// create time. Modal enforces the probe internally (it gates its own traffic
	// routing on it); Nebula does not read the result back — observe reports status
	// from the cheap point-in-time Poll signal and never blocks on WaitUntilReady.
	// We only ever pass a user-supplied probe; the adapter never fabricates one.
	ReadinessProbe *corev1.Probe
}

// Sandbox is the adapter-level view of a Modal sandbox as observed.
type Sandbox struct {
	ID       string
	Tags     map[string]string
	Status   string // Modal's own status string, normalized by toState.
	Endpoint string
}

// ClaimTagKey is the sandbox tag under which the NodeClaim name is stored, so
// List/Get can recover Nebula identity. Modal supports native tags, so no
// name-encoding hack is needed.
const ClaimTagKey = "nebula.inftyai.com/claim"

// Provider is the Modal implementation of provider.Provider. It embeds
// catalog.Base for the generic catalog methods (Name, Offerings, and the
// identity MapAccelerator — Modal names its GPUs exactly like Nebula's canonical
// names) and implements only the Modal-specific lifecycle here.
type Provider struct {
	catalog.Base
	client Client
}

// New returns a Modal Provider backed by client and price catalog. Both must be
// non-nil; use catalog.Load() to build the catalog from the CSV/ConfigMap data.
// cat is the catalog.Lookup seam, so tests can inject a fake.
func New(client Client, cat catalog.Lookup) *Provider {
	return &Provider{
		Base:   catalog.Base{ProviderName: provider.ProviderModal, Catalog: cat},
		client: client,
	}
}

// Capabilities implements provider.Provider. See the package doc for why each
// trait is set the way it is.
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		SupportsStop:     false, // create/terminate only
		SupportsSpot:     false, // no user-facing preemptible tier
		NativeTags:       true,  // sandbox tags carry identity
		PreemptionNotice: 0,     // no push; poll-based detection
		PollInterval:     0,     // OnDemand-only (never preempts) → the default cadence is fine
	}
}

// Provision implements provider.Provider. The Pod is the source of truth for
// the workload; req carries only the claim identity and capacity tier.
func (p *Provider) Provision(ctx context.Context, pod *corev1.Pod, req provider.ProvisionRequest) (string, error) {
	if pod == nil {
		return "", errors.New("modal: nil pod")
	}
	if req.ClaimName == "" {
		return "", errors.New("modal: empty ClaimName in ProvisionRequest")
	}

	// Idempotency: if a sandbox already carries this claim tag, return it rather
	// than creating a second (guards against a retry after a partial create).
	if existing, err := p.findByClaim(ctx, req.ClaimName); err != nil {
		return "", err
	} else if existing != nil {
		return existing.ID, nil
	}

	spec, err := p.sandboxSpecFromPod(pod, req)
	if err != nil {
		return "", err
	}
	return p.client.CreateSandbox(ctx, spec)
}

// Terminate implements provider.Provider. Idempotent by the Client contract.
func (p *Provider) Terminate(ctx context.Context, instanceID string) error {
	if instanceID == "" {
		return nil // nothing provisioned yet; treat as already gone
	}
	return p.client.TerminateSandbox(ctx, instanceID)
}

// Get implements provider.Provider.
func (p *Provider) Get(ctx context.Context, instanceID string) (*provider.Instance, error) {
	sb, err := p.client.GetSandbox(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if sb == nil {
		return nil, nil // absent => terminated, per interface contract
	}
	inst := p.toInstance(*sb)
	return &inst, nil
}

// List implements provider.Provider.
func (p *Provider) List(ctx context.Context) ([]provider.Instance, error) {
	sandboxes, err := p.client.ListSandboxes(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]provider.Instance, 0, len(sandboxes))
	for _, sb := range sandboxes {
		out = append(out, p.toInstance(sb))
	}
	return out, nil
}

// ClassifyProvisionError implements provider.Provider. The failure CATEGORIES
// and the scope-derivation rule are shared across all adapters
// (provider.ClassifyError, provider.ErrNoCapacity, ...), so this method only
// supplies what is Modal-specific: the capacity tier to stamp on an
// accelerator-scoped block. Modal is OnDemand-only, so that is always OnDemand.
//
// A real Modal Client that recognizes an SDK error condition should wrap the
// matching shared sentinel (e.g. fmt.Errorf("...: %w", provider.ErrNoCapacity));
// ClassifyError honours those first and falls back to string heuristics for raw
// API messages, so no Modal-specific matching is duplicated here.
func (p *Provider) ClassifyProvisionError(err error, accelerator, _ string) provider.BlockScope {
	// Region is ignored: Modal is region-simple (no region axis), so the block's
	// Region stays nil — see BlockScope's three-state rule.
	return provider.ClassifyError(err, nebulav1alpha1.CapacityOnDemand, accelerator)
}

// findByClaim returns the sandbox tagged with claimName, or nil if none.
func (p *Provider) findByClaim(ctx context.Context, claimName string) (*provider.Instance, error) {
	sandboxes, err := p.client.ListSandboxes(ctx)
	if err != nil {
		return nil, err
	}
	for _, sb := range sandboxes {
		if sb.Tags[ClaimTagKey] == claimName {
			inst := p.toInstance(sb)
			return &inst, nil
		}
	}
	return nil, nil
}

// sandboxSpecFromPod reads the workload off the Pod (source of truth) and the
// accelerator type (from the AcceleratorTypeLabel) and count (from the
// nvidia.com/gpu resource), then maps the accelerator to Modal's identifier.
func (p *Provider) sandboxSpecFromPod(pod *corev1.Pod, req provider.ProvisionRequest) (SandboxSpec, error) {
	if len(pod.Spec.Containers) == 0 {
		return SandboxSpec{}, errors.New("modal: pod has no containers")
	}
	c := pod.Spec.Containers[0]

	env := make(map[string]string, len(c.Env))
	for _, e := range c.Env {
		// ValueFrom (secrets/configmaps) is not resolved here; the real Client
		// wiring must project those. Plain values are copied through.
		if e.ValueFrom == nil {
			env[e.Name] = e.Value
		}
	}

	spec := SandboxSpec{
		Image:          c.Image,
		Command:        append(append([]string{}, c.Command...), c.Args...),
		Env:            env,
		CPU:            cpuCores(&c),
		MemoryMiB:      memoryMiB(&c),
		Ports:          containerPorts(&c),
		Timeout:        sandboxTimeout(pod),
		Tags:           map[string]string{ClaimTagKey: req.ClaimName},
		ReadinessProbe: c.ReadinessProbe,
	}

	// Accelerator type comes from the AcceleratorTypeLabel; count from the
	// container's nvidia.com/gpu resource (see util.AcceleratorRequest).
	canonical, count, err := util.AcceleratorRequest(pod)
	if err != nil {
		return SandboxSpec{}, fmt.Errorf("modal: %w", err)
	}
	if canonical != "" {
		// Modal takes the count as a free parameter and has no interchangeable
		// alternates, so it always maps to a single id; take the primary (ids[0]).
		ids, ok := p.MapAccelerator(canonical, count)
		if !ok {
			return SandboxSpec{}, fmt.Errorf("modal: unsupported accelerator %q", canonical)
		}
		spec.GPU = ids[0]
		spec.GPUCount = count
	}
	// No annotation => CPU-only sandbox (GPU/"" GPUCount 0), handled naturally.
	return spec, nil
}

// cpuCores reads the container's CPU request as fractional physical cores (Modal's
// unit). It prefers requests, falling back to limits, and returns 0 (→ Modal
// default) when neither is set.
func cpuCores(c *corev1.Container) float64 {
	q := resourceQty(c, corev1.ResourceCPU)
	if q == nil {
		return 0
	}
	// MilliValue is cores*1000; convert to fractional cores.
	return float64(q.MilliValue()) / 1000.0
}

// memoryMiB reads the container's memory request in MiB (Modal's unit), preferring
// requests over limits. Returns 0 (→ Modal default) when neither is set.
func memoryMiB(c *corev1.Container) int {
	q := resourceQty(c, corev1.ResourceMemory)
	if q == nil {
		return 0
	}
	const miB = 1024 * 1024
	return int(q.Value() / miB)
}

// resourceQty returns the container's request for name, falling back to its limit,
// or nil when neither is present.
func resourceQty(c *corev1.Container, name corev1.ResourceName) *resource.Quantity {
	if q, ok := c.Resources.Requests[name]; ok {
		return &q
	}
	if q, ok := c.Resources.Limits[name]; ok {
		return &q
	}
	return nil
}

// containerPorts collects the container's declared ports so the Client can open a
// tunnel per port. The observed tunnel URL becomes the Pod's endpoint, so a Pod
// that declares no port is reachable-less by design.
func containerPorts(c *corev1.Container) []int {
	if len(c.Ports) == 0 {
		return nil
	}
	ports := make([]int, 0, len(c.Ports))
	for _, p := range c.Ports {
		ports = append(ports, int(p.ContainerPort))
	}
	return ports
}

// sandboxTimeout maps the Pod's activeDeadlineSeconds (Kubernetes' own "maximum
// lifetime of the pod") onto Modal's sandbox Timeout, defaulting to
// defaultSandboxTimeout when the Pod does not pin one. It is never zero: a zero
// Timeout is Modal's 5-minute default and would kill a real workload.
func sandboxTimeout(pod *corev1.Pod) time.Duration {
	if d := pod.Spec.ActiveDeadlineSeconds; d != nil && *d > 0 {
		return time.Duration(*d) * time.Second
	}
	return defaultSandboxTimeout
}

// toInstance normalizes a Modal sandbox into the provider-agnostic Instance.
func (p *Provider) toInstance(sb Sandbox) provider.Instance {
	return provider.Instance{
		ID:        sb.ID,
		ClaimName: sb.Tags[ClaimTagKey],
		State:     toState(sb.Status),
		Endpoint:  sb.Endpoint,
		// Modal is OnDemand-only; reflect that on observed instances.
		CapacityType: nebulav1alpha1.CapacityOnDemand,
	}
}

// Sandbox status strings observe produces (and toState consumes). observe
// derives status from Poll, so it only ever emits statusRunning (live) or
// statusTerminated (process exited); an unset status ("") means Poll errored.
const (
	statusRunning    = "running"
	statusTerminated = "terminated"
)

// toState maps the status strings observe produces to the provider-agnostic
// lifecycle state. observe emits only statusRunning (live) or statusTerminated
// (process exited); "ready" is also accepted as a live synonym. Anything else —
// including the empty string observe leaves when Poll itself errors — maps to
// Pending, so the poll loop keeps watching rather than declaring a premature
// terminal state.
func toState(modalStatus string) provider.InstanceState {
	switch strings.ToLower(modalStatus) {
	case statusRunning, "ready":
		return provider.InstanceRunning
	case statusTerminated:
		return provider.InstanceTerminated
	default:
		return provider.InstancePending
	}
}
