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
//   - Lifecycle is create/terminate only — no stop/resume, so SupportsStop=false.
//   - The pinned Go SDK exposes no spot knob (Modal's own API has one), so
//     SupportsSpot=false. Placement SKIPS a Spot candidate rather than downgrading it
//     silently, so only OnDemand requests reach here and CapacityType is never read.
//   - Region is OPTIONAL, unlike AWS: Modal places freely when none is given, and that
//     unconstrained mode is both the widest capacity pool and the cheapest — pinning
//     costs 1.5x (broad, "us") or 1.75x (narrow, "us-east") on the whole bill. So empty
//     is the preferred path, not a fallback. The vocabulary is Modal's own and needs no
//     translation table.
//   - A pool's regions go out in ONE call rather than being walked. Create never fails
//     on capacity (it queues), so nothing would re-drive placement to a second region
//     and walking would strand the workload in whichever came first. Modal's scheduler
//     has the live capacity view, so ExpandRegions collapses the declaration into one
//     opaque candidate and regionsOf splits it back at the API boundary. The cost is
//     blocklist precision, which is free here since no Modal failure is
//     region-attributable.
//   - Sandboxes carry native tags, so NativeTags=true and ClaimName is a tag rather
//     than smuggled into the instance name.
//   - There is no preemption push; detection is poll-based.
//
// The concrete Modal API lives behind the Client seam, so this package holds only
// provider-agnostic translation and is unit-testable without network access.
package modal

import (
	"context"
	"errors"
	"fmt"
	"io"
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

// compile-time assertions that Provider satisfies the interfaces. LogStreamer and
// Executor are the optional halves: they are what make `kubectl logs` and `kubectl exec`
// work here, and asserting them separately is the point — a provider is free to serve
// neither.
var (
	_ provider.Provider    = (*Provider)(nil)
	_ provider.LogStreamer = (*Provider)(nil)
	_ provider.Executor    = (*Provider)(nil)
)

// Client is the narrow seam over Modal's API. It is intentionally small: only
// the operations the adapter needs, expressed in provider-agnostic terms, so a
// real implementation (Modal SDK/HTTP) and a fake (tests) are interchangeable.
type Client interface {
	// CreateSandbox launches one sandbox from spec and returns its Modal id plus the
	// connect credential minted for it. The credential is returned HERE and nowhere
	// else — minting is one-shot and there is no read-back, so a caller that drops it
	// has lost it for the sandbox's life. Zero when none could be minted; see
	// sdkClient.mintCredential.
	CreateSandbox(ctx context.Context, spec SandboxSpec) (id string, cred Credential, err error)
	// TerminateSandbox terminates a sandbox by id. Must be idempotent:
	// terminating an already-gone sandbox returns nil.
	TerminateSandbox(ctx context.Context, id string) error
	// GetSandbox returns one sandbox, or (nil, nil) if it no longer exists.
	GetSandbox(ctx context.Context, id string) (*Sandbox, error)
	// ListSandboxes returns every Nebula-owned sandbox, filtered by the tag the
	// adapter sets at create time, in as few calls as possible.
	ListSandboxes(ctx context.Context) ([]Sandbox, error)
	// SandboxLogs returns merged stdout+stderr, from the first byte, following until
	// the sandbox exits (see provider.LogStreamer). The caller owns Close.
	SandboxLogs(ctx context.Context, id string) (io.ReadCloser, error)
	// SandboxExec starts cmd inside a running sandbox and returns the handle to its
	// streams and exit code (see provider.Executor). The caller owns Close.
	SandboxExec(ctx context.Context, id string, cmd []string, opts provider.ExecOptions) (provider.Process, error)
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
	// Ports are the container ports to expose, from the Pod's containerPorts. They
	// declare to Modal which ports may receive traffic at all, and the connect URL
	// routes to the first of them (see firstPort) — one token routes to one port.
	// Empty leaves both the exposed set and the routed port to Modal's own default.
	Ports []int
	// Regions constrains where Modal may place the sandbox, in Modal's own
	// vocabulary — a broad region ("us", "eu", "ap") or a narrow one ("us-east",
	// "eu-west", "jp"). It comes from ProviderSpec.Regions via ProvisionRequest.Region
	// and is forwarded unvalidated, since Modal owns that vocabulary and gains regions
	// faster than this adapter ships.
	//
	// EMPTY IS THE PREFERRED VALUE unless a workload has a real placement requirement.
	// Modal charges 1.5x for a broad region and 1.75x for a narrow one, on the whole
	// compute bill, and an unconstrained sandbox also draws on the widest capacity pool.
	// So this trades money and availability for locality: a data-residency knob, not a
	// performance one.
	//
	// It carries EVERY region the pool declared, not one per attempt, because a Modal
	// create cannot fail over — it returns an accepted id with no capacity error, so
	// nothing here could try a second region afterwards. See ExpandRegions and regionsOf.
	Regions []string
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
	// routing on it), and observe reads the result back so a live-but-not-yet-ready
	// sandbox is reported statusInitializing rather than statusRunning.
	// We only ever pass a user-supplied probe; the adapter never fabricates one.
	ReadinessProbe *corev1.Probe
}

// Sandbox is the adapter-level view of a Modal sandbox as observed.
//
// No endpoint here, deliberately: Modal's reachable address is the connect URL, minted at
// CREATE time and published to the Pod's endpoint annotation, where it persists for the
// sandbox's life. Re-deriving it per read would be a round trip for a value the API server
// already holds. The only other candidate, a tunnel URL, is PUBLIC to anyone who learns
// it, so substituting it for an authenticated URL would downgrade access.
type Sandbox struct {
	ID     string
	Tags   map[string]string
	Status string // Modal's own status string, normalized by toState.
}

// Credential is the connect pair CreateSandbox mints for a new sandbox: the URL a
// consumer calls and the bearer token that authenticates against it —
//
//	curl -H "Authorization: Bearer $token" $url
//
// Token is a SECRET. It is never tagged (Modal's tags are plaintext and
// bulk-listable), never annotated onto the Pod, and never logged; the virtual kubelet
// writes the pair to a Secret in the Pod's namespace. It exists on the create path
// only — see provider.ProvisionResult, which this maps onto.
type Credential struct {
	URL   string
	Token string
}

// ClaimTagKey is the sandbox tag under which the NodeClaim name is stored, so
// List/Get can recover Nebula identity. Modal supports native tags, so no
// name-encoding hack is needed.
const ClaimTagKey = "nebula.inftyai.com/claim"

// ProbeTagKey records that a sandbox was created WITH a readiness probe;
// probeTagValue is the only value it is ever set to.
//
// Nebula has to carry this itself. observe needs it to tell "no probe, nothing to wait
// for" from "the probe has not passed yet", and WaitUntilReady errors on the former so it
// cannot just be attempted. The Pod cannot answer either: the read path takes no Pod, and
// the one GetPod synthesizes on re-adoption has no spec.
//
// Modal's control plane does return it (SandboxInfo.ReadinessProbe), but the Go SDK builds
// its *Sandbox from the id alone and keeps the control-plane client private. If a future
// SDK exposes SandboxInfo, this tag and observe's WaitUntilReady both become unnecessary.
//
// A tag is the right carrier: observe already fetches tags before status (so the gate is
// free), and tags live with the sandbox, so probe-ness is recovered the way identity is —
// even for a sandbox this process never created.
const (
	ProbeTagKey   = "nebula.inftyai.com/readiness-probe"
	probeTagValue = "true"
)

// Provider is the Modal implementation of provider.Provider. It embeds
// catalog.Base for the generic catalog methods (Name, Offerings, and the
// catalog-driven MapAccelerator — Modal names its GPUs nearly identically to
// Nebula's canonical names, so most rows map by identity and the ones that don't
// carry an accelerator_id in modal.csv) and implements only the Modal-specific
// lifecycle here.
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

// regionSeparator joins several Modal regions into the ONE candidate placement
// walks. It is deliberately a character no region name contains, so splitting is
// unambiguous, and deliberately not a comma: the value lands in RegionAnnotation and
// a comma reads like a list a consumer might re-split with different rules.
const regionSeparator = "|"

// ExpandRegions implements provider.Provider, overriding catalog.Base's
// pass-through. It resolves the pool's whole declaration to at most ONE candidate,
// carrying every declared region in it, rather than one candidate per region.
//
// The opposite of AWS, because Modal cannot fail over. An AWS CreateFleet reports a
// shortage synchronously, so walking regions one at a time lets the next be tried.
// Sandboxes.Create instead ACCEPTS immediately and returns a real id with the GPU maybe
// still queued. No error means ClassifyProvisionError never runs, nothing is blocklisted,
// and placement is never re-driven — so the first region walked would be the only one ever
// tried, shrinking the pool to one region and discarding the rest.
//
// Handing Modal the full set moves the choice to the party that can act on it: its
// scheduler takes several regions and picks with a live view of capacity.
//
// The cost is that the candidate's region is a joined token, so a blocklist entry covers
// the whole set. That loses nothing today, since a queued sandbox never reports which
// region ran dry.
func (p *Provider) ExpandRegions(declared []string) []string {
	seen := make(map[string]bool)
	regions := make([]string, 0, len(declared))
	for _, d := range declared {
		d = strings.TrimSpace(d)
		if d == "" || seen[d] {
			continue
		}
		seen[d] = true
		regions = append(regions, d)
	}
	if len(regions) == 0 {
		return nil // unconstrained: the widest and cheapest case
	}
	return []string{strings.Join(regions, regionSeparator)}
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
//
// A fresh sandbox is NEVER reserved. Create returns as soon as Modal accepts it: the id is
// real (listable, terminable, fully on the hook for teardown) but the GPU may be queued for
// minutes. So id and capacity are two separate facts here, unlike AWS, and reserved=false
// is what keeps the Pod at the provisioning reason instead of claiming to be initializing.
// The queued→running transition is then observed through the poll loop's List.
//
// The connect credential comes back on this call and only this call, since Modal mints it
// once with no read-back. The caller must persist it or it is lost; see mintCredential.
func (p *Provider) Provision(
	ctx context.Context, pod *corev1.Pod, req provider.ProvisionRequest,
) (provider.ProvisionResult, error) {
	if pod == nil {
		return provider.ProvisionResult{}, errors.New("modal: nil pod")
	}
	if req.ClaimName == "" {
		return provider.ProvisionResult{}, errors.New("modal: empty ClaimName in ProvisionRequest")
	}

	// Idempotency: if a sandbox already carries this claim tag, return it rather
	// than creating a second (guards against a retry after a partial create).
	//
	// Unlike a fresh create, this sandbox has been OBSERVED, so its state is known: one
	// the poll loop reports Running has capacity, one still queued does not. That is more
	// information than a create can return, so report it rather than a flat false.
	//
	// It carries NO credential, per the interface contract: the original cannot be
	// re-read, and minting a second would hand the consumer a token that changed on every
	// retry. That leaves a real gap — if the first create succeeded but its credential
	// never reached a Secret, nothing recovers it. Closing it needs cluster access this
	// layer does not have.
	if existing, err := p.findByClaim(ctx, req.ClaimName); err != nil {
		return provider.ProvisionResult{}, err
	} else if existing != nil {
		return provider.ProvisionResult{
			InstanceID: existing.ID,
			Reserved:   existing.State == provider.InstanceRunning,
		}, nil
	}

	spec, err := p.sandboxSpecFromPod(pod, req)
	if err != nil {
		return provider.ProvisionResult{}, err
	}
	id, cred, err := p.client.CreateSandbox(ctx, spec)
	if err != nil {
		return provider.ProvisionResult{}, err
	}
	return provider.ProvisionResult{
		InstanceID:   id,
		Reserved:     false,
		ConnectURL:   cred.URL,
		ConnectToken: cred.Token,
	}, nil
}

// Terminate implements provider.Provider. Idempotent by the Client contract.
func (p *Provider) Terminate(ctx context.Context, instanceID string) error {
	if instanceID == "" {
		return nil // nothing provisioned yet; treat as already gone
	}
	return p.client.TerminateSandbox(ctx, instanceID)
}

// Logs implements provider.LogStreamer, and is what `kubectl logs` reads. A straight
// pass-through: the options are the caller's job. An empty id (nothing provisioned
// yet) is an error, not an empty stream, which would misreport as "logged nothing".
func (p *Provider) Logs(ctx context.Context, instanceID string) (io.ReadCloser, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("modal: no sandbox for this pod yet")
	}
	return p.client.SandboxLogs(ctx, instanceID)
}

// Exec implements provider.Executor, and is what `kubectl exec` runs. A pass-through:
// the streams are the caller's job (pkg/vnode/exec.go pumps them).
//
// Modal needs no agent in the image for this — its own worker runs the command — so exec
// works on any sandbox, including the `sleep infinity` placeholder a Sandbox gets. The
// sandbox must be RUNNING though: Modal routes an exec through the container's task, so
// one still queued fails here rather than waiting.
func (p *Provider) Exec(
	ctx context.Context, instanceID string, cmd []string, opts provider.ExecOptions,
) (provider.Process, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("modal: no sandbox for this pod yet")
	}
	return p.client.SandboxExec(ctx, instanceID, cmd, opts)
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

// ClassifyProvisionError implements provider.Provider. The categories and the
// scope-derivation rule are shared (provider.ClassifyError, provider.ErrNoCapacity), so
// this only supplies the Modal-specific part: the tier to stamp on an accelerator block,
// always OnDemand. A Client that recognizes an SDK error should wrap the matching shared
// sentinel; ClassifyError honours those first, then falls back to string heuristics.
//
// It confines the block to the failing CANDIDATE, which is not necessarily one region:
// ExpandRegions sends every declared region at once, so the token may name the whole set
// and the block then covers all of it. That is honest — a multi-region create never says
// which region was short — and a pool wanting per-region blocking declares per-region
// pools. An empty region leaves Region nil, which per BlockScope matches only candidates
// with no region, so the block never leaks onto region-pinned ones.
func (p *Provider) ClassifyProvisionError(err error, accelerator, region string) provider.BlockScope {
	// No failure, no block. ClassifyError already returns the zero scope here, but
	// the region decoration below would repopulate it into a non-empty scope that
	// recordBlock would install — so the guard has to come first, as it does in AWS.
	if err == nil {
		return provider.BlockScope{}
	}
	scope := provider.ClassifyError(err, nebulav1alpha1.CapacityOnDemand, accelerator)
	// DenyAll already covers every region (auth fails everywhere), so narrowing it
	// would contradict the category.
	if region != "" && !scope.DenyAll {
		scope.Region = &region
	}
	return scope
}

// findByClaim returns the sandbox tagged with claimName, or nil if none.
func (p *Provider) findByClaim(ctx context.Context, claimName string) (*provider.Instance, error) {
	// TODO: do we have performance issue here?
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

	tags := map[string]string{ClaimTagKey: req.ClaimName}
	// Record probe-ness alongside identity so observe can recover it later; see
	// ProbeTagKey for why this cannot be re-derived at observation time. The tag
	// tracks whether Modal will actually RECEIVE a probe, not merely whether the Pod
	// declares one — planProbe drops shapes Modal cannot express (a named port, an
	// unsupported handler), and tagging those would claim a readiness signal that
	// does not exist.
	if _, ok := planProbe(c.ReadinessProbe); ok {
		tags[ProbeTagKey] = probeTagValue
	}

	spec := SandboxSpec{
		Image:     c.Image,
		Command:   append(append([]string{}, c.Command...), c.Args...),
		Env:       env,
		CPU:       cpuCores(&c),
		MemoryMiB: memoryMiB(&c),
		Ports:     containerPorts(&c),
		// An empty request region stays an empty slice, not a one-element [""]: that
		// is the unconstrained case (no region declared on the pool), and it must
		// reach Modal as "no placement constraint" — its widest pool and its
		// un-multiplied price. See SandboxSpec.Regions.
		Regions:        regionsOf(req.Region),
		Timeout:        sandboxTimeout(pod),
		Tags:           tags,
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

// regionsOf turns placement's single region candidate back into the slice Modal's
// API takes. It is the exact inverse of ExpandRegions' join: that collapses the
// pool's whole declaration into ONE candidate (see there for why Modal cannot fail
// over region by region), and this expands it again at the call boundary, so the
// set the operator declared is what Modal's scheduler gets to choose among.
//
// An empty region means "unconstrained" and must produce a nil slice rather than
// [""], since a one-element slice holding the empty string would ask Modal to place
// in a region named "" — turning the cheapest, widest case into a malformed request.
func regionsOf(region string) []string {
	if region == "" {
		return nil
	}
	var out []string
	for _, r := range strings.Split(region, regionSeparator) {
		if r = strings.TrimSpace(r); r != "" {
			out = append(out, r)
		}
	}
	return out
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

// containerPorts collects the container's declared ports, which is what tells Modal
// which ports may receive traffic at all. The connect URL then routes to one of them
// (see firstPort).
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

// firstPort returns the port the connect URL should route to, or 0 when the
// container declares none, which leaves the port to Modal's own default. Modal
// routes one port per token, so the first declared port wins; a workload serving on a
// second port has no address of its own today.
func firstPort(ports []int) int {
	if len(ports) == 0 {
		return 0
	}
	return ports[0]
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
//
// Endpoint is left empty: Modal's address is published from the create path (see
// Credential), and an observed empty endpoint never clears the annotation already on
// the Pod — the write paths all skip "".
func (p *Provider) toInstance(sb Sandbox) provider.Instance {
	return provider.Instance{
		ID:        sb.ID,
		ClaimName: sb.Tags[ClaimTagKey],
		State:     toState(sb.Status),
		// Modal is OnDemand-only; reflect that on observed instances.
		CapacityType: nebulav1alpha1.CapacityOnDemand,
	}
}

// Sandbox status strings observe produces (and toState consumes). An unset
// status ("") means Poll errored.
const (
	// statusRunning: the sandbox process is live AND, when a readiness probe is
	// configured, that probe has passed.
	statusRunning = "running"
	// statusInitializing: the process is live but its readiness probe has not
	// passed yet — the sandbox is queued, pulling its image, attaching a GPU, or
	// booting. Only ever produced for a sandbox carrying ProbeTagKey, since Modal
	// has no readiness signal without a probe.
	statusInitializing = "initializing"
	// statusTerminated: the process exited in a way that is NOT a failure — it ran
	// to completion (exit 0), or Modal reclaimed it (our own Terminate, a sandbox
	// timeout). "Gone", with no claim about why.
	statusTerminated = "terminated"
	// statusFailed: the process exited nonzero — it crashed, or never came up at all (bad
	// image, unavailable GPU, OOM at init). Distinct from terminated because a sandbox
	// that never started reads like a clean teardown if reported as gone.
	statusFailed = "failed"
)

// toState maps the status strings observe produces to the provider-agnostic
// lifecycle state.
//
// A live sandbox is only reported Running once its readiness probe has passed. Poll cannot
// see readiness — it only answers "has the process exited?" — so a still-scheduling sandbox
// reads like a serving one, and reporting Running that early advances the Pod (and its
// Deployment's ready replicas) before the box is reachable. Same as AWS holding an instance
// at Pending until its 2/2 status checks clear.
//
// statusInitializing needs no case: it falls to the default, where every unrecognized
// status maps to Pending so the poll loop keeps watching instead of going terminal early.
func toState(modalStatus string) provider.InstanceState {
	switch strings.ToLower(modalStatus) {
	case statusRunning:
		return provider.InstanceRunning
	case statusTerminated:
		return provider.InstanceTerminated
	case statusFailed:
		return provider.InstanceFailed
	default:
		return provider.InstancePending
	}
}
