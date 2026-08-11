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
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	modal "github.com/modal-labs/modal-client/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/InftyAI/Nebula/pkg/provider/catalog"
)

// sdkClient is the real Client, backed by Modal's official Go SDK
// (github.com/modal-labs/modal-client/go, beta). It is the seam between our
// provider-agnostic adapter and Modal's API; all Modal-specific SDK calls live
// here so the adapter and its tests stay SDK-free.
//
// A NodeClaim maps to one Modal Sandbox. Identity is carried in the sandbox's
// native Tags (ClaimTagKey), which are server-side filterable, so List/Get can
// recover the owning claim without a naming hack.
type sdkClient struct {
	mc      *modal.Client
	appName string

	// endpointTimeout bounds the best-effort tunnel lookup used to report an
	// instance's reachable address; tunnels only exist once the sandbox is up.
	endpointTimeout time.Duration

	// readyTimeout is the budget one background readiness waiter gets. It is a real
	// budget, not a hint: WaitUntilReady BLOCKS and returns early only to say
	// "ready" — never to say "not ready" — so a budget below the call's own setup
	// cost cannot produce an answer, it can only produce a deadline. Setup dominates
	// (getCommandRouter polls for the task id, then dials a fresh TLS gRPC
	// connection to the task's own router), measured at ~16s cold, so this must be
	// comfortably above that. It is affordable precisely because the wait no longer
	// runs on the read path.
	readyTimeout time.Duration

	// Readiness latch. WaitUntilReady is a one-shot blocking WAIT, but observe is a
	// level-triggered READ that must answer from state on every poll tick; wrapping
	// the wait in a short timeout to fake a read is what made a timeout masquerade
	// as "not ready". So the wait happens once, in the background, and its result is
	// latched here for observe to read.
	//
	// ready is latched on CONFIRMED readiness only — the default is not-ready, so an
	// ambiguous error can no longer promote a sandbox that is still coming up.
	// waiting dedupes waiters so repeated ticks don't pile up goroutines on one
	// sandbox. Both are keyed by sandbox id and dropped by forgetReady when the
	// sandbox goes away, so neither grows without bound.
	//
	// The latch does not DEMOTE: a probe that passes and later starts failing leaves
	// the sandbox Running. Poll still observes process exit, so death is caught;
	// sickness is not. Demotion would need a re-armed waiter and a staleness stamp.
	readyMu sync.Mutex
	ready   map[string]bool
	waiting map[string]struct{}
}

// compile-time assertion that sdkClient satisfies the adapter's Client seam.
var _ Client = (*sdkClient)(nil)

// NewSDKClient builds a Modal-backed Client. It reads Modal credentials from the
// environment / ~/.modal.toml via the SDK's default profile. appName is the
// Modal App all Nebula sandboxes are created under (created if missing at first
// use). The returned *Provider is ready to register.
//
// Example wiring:
//
//	c, err := modal.NewSDKClient(ctx, "nebula")
//	if err != nil { return err }
//	provider.Register(modal.New(c))
func NewSDKClient(ctx context.Context, appName string) (*Provider, error) {
	mc, err := modal.NewClient()
	if err != nil {
		return nil, fmt.Errorf("modal: init SDK client: %w", err)
	}
	if appName == "" {
		appName = "nebula"
	}
	cat, err := catalog.Load()
	if err != nil {
		return nil, fmt.Errorf("modal: load price catalog: %w", err)
	}
	return New(&sdkClient{
		mc:              mc,
		appName:         appName,
		endpointTimeout: 5 * time.Second,
		readyTimeout:    30 * time.Second,
		ready:           make(map[string]bool),
		waiting:         make(map[string]struct{}),
	}, cat), nil
}

// app resolves (creating if missing) the Modal App all sandboxes live under.
func (c *sdkClient) app(ctx context.Context) (*modal.App, error) {
	return c.mc.Apps.FromName(ctx, c.appName, &modal.AppFromNameParams{CreateIfMissing: true})
}

// CreateSandbox implements Client.
func (c *sdkClient) CreateSandbox(ctx context.Context, spec SandboxSpec) (string, error) {
	app, err := c.app(ctx)
	if err != nil {
		return "", fmt.Errorf("modal: resolve app: %w", err)
	}
	if spec.Image == "" {
		return "", fmt.Errorf("modal: empty image in sandbox spec")
	}
	image := c.mc.Images.FromRegistry(spec.Image, nil)

	probe, err := modalProbe(spec.ReadinessProbe)
	if err != nil {
		return "", fmt.Errorf("modal: readiness probe: %w", err)
	}

	sb, err := c.mc.Sandboxes.Create(ctx, app, image, &modal.SandboxCreateParams{
		Command:        spec.Command,
		Env:            spec.Env,
		GPU:            gpuReservation(spec.GPU, spec.GPUCount),
		CPU:            spec.CPU,
		MemoryMiB:      spec.MemoryMiB,
		EncryptedPorts: spec.Ports,
		Timeout:        spec.Timeout,
		Tags:           spec.Tags,
		ReadinessProbe: probe,
	})
	if err != nil {
		return "", err
	}
	return sb.SandboxID, nil
}

// modalProbe maps a Pod readinessProbe onto Modal's Probe. Modal supports only
// TCP and Exec probes, so an HTTPGet probe degrades to a TCP probe on its port
// (readiness ≈ the port accepting connections). Returns (nil, nil) when p is nil
// or names no supported handler, so a probe-less (or unsupported) workload simply
// gets no Modal probe. PeriodSeconds maps to the probe interval; zero leaves the
// SDK default (a zero interval is rejected by the SDK constructors).
//
// A TCP/HTTPGet probe with a NAMED port (e.g. port: http) is treated as
// unsupported: resolving the name to a number needs the container's ports list,
// which this helper does not have, and passing the intstr's 0 fallback would
// create an invalid Modal TCP probe on port 0. So a named port omits the probe
// rather than emitting a bogus one.
func modalProbe(p *corev1.Probe) (*modal.Probe, error) {
	plan, ok := planProbe(p)
	if !ok {
		return nil, nil
	}
	if plan.exec != nil {
		return modal.NewExecProbe(plan.exec, &modal.ExecProbeParams{Interval: plan.interval})
	}
	return modal.NewTCPProbe(plan.port, &modal.TCPProbeParams{Interval: plan.interval})
}

// probePlan is the SDK-free decision behind modalProbe: WHETHER a Pod probe maps
// to a Modal probe, and if so with what. Exactly one of exec/port is set.
type probePlan struct {
	exec     []string
	port     int
	interval time.Duration
}

// planProbe decides whether a Pod readinessProbe maps onto a Modal probe,
// reporting ok=false when it does not (nil probe, no handler, or a named port —
// see modalProbe for why a named port cannot be resolved here).
//
// This is split out from modalProbe so the CREATE path and the ProbeTagKey gate
// cannot disagree: the tag asserts "Modal received a probe for this sandbox", and
// deriving it from `pod.Spec.Containers[0].ReadinessProbe != nil` made that a lie
// for every probe shape modalProbe drops — those sandboxes advertised a probe
// Modal never got, so the readiness wait was asked about a probe that did not
// exist. One predicate, two callers, no drift.
func planProbe(p *corev1.Probe) (probePlan, bool) {
	if p == nil {
		return probePlan{}, false
	}
	// PeriodSeconds maps to the probe interval; zero leaves the SDK default (the
	// SDK constructors reject a zero interval).
	var interval time.Duration
	if p.PeriodSeconds > 0 {
		interval = time.Duration(p.PeriodSeconds) * time.Second
	}
	switch {
	case p.Exec != nil && len(p.Exec.Command) > 0:
		return probePlan{exec: p.Exec.Command, interval: interval}, true
	case p.TCPSocket != nil:
		port, ok := numericPort(p.TCPSocket.Port)
		return probePlan{port: port, interval: interval}, ok
	case p.HTTPGet != nil:
		port, ok := numericPort(p.HTTPGet.Port)
		return probePlan{port: port, interval: interval}, ok
	default:
		return probePlan{}, false
	}
}

// numericPort returns an intstr port as an int when it is numeric (>0), reporting
// ok=false for a named port or a non-positive value. IntValue() yields 0 for a
// named port, which is not a usable Modal probe target, so callers must skip the
// probe rather than emit port 0.
func numericPort(p intstr.IntOrString) (int, bool) {
	if p.Type != intstr.Int {
		return 0, false
	}
	if v := p.IntValue(); v > 0 {
		return v, true
	}
	return 0, false
}

// TerminateSandbox implements Client. Idempotent: a sandbox that no longer
// exists resolves to a not-found from FromID, which we treat as already gone.
func (c *sdkClient) TerminateSandbox(ctx context.Context, id string) error {
	// Drop any latched readiness up front, so it is released even on the error
	// paths below: this sandbox is on its way out either way, and a live waiter on
	// it is now pointless work.
	c.forgetReady(id)

	sb, err := c.mc.Sandboxes.FromID(ctx, id, &modal.SandboxFromIDParams{})
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	if _, err := sb.Terminate(ctx, &modal.SandboxTerminateParams{}); err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// GetSandbox implements Client. Returns (nil, nil) when the sandbox is gone.
func (c *sdkClient) GetSandbox(ctx context.Context, id string) (*Sandbox, error) {
	sb, err := c.mc.Sandboxes.FromID(ctx, id, &modal.SandboxFromIDParams{})
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	out := c.observe(ctx, sb)
	return &out, nil
}

// ListSandboxes implements Client. It scopes the list server-side to Nebula's
// own Modal App (AppID), which is what isolates Nebula-owned sandboxes: every
// sandbox Nebula creates lives under this one App, so sandboxes in other Apps
// are never returned. It does NOT filter by the claim tag — the SDK's Tags
// filter matches exact key=value pairs, but ClaimTagKey carries a distinct
// value (the claim name) per sandbox, so there is no "has this key with any
// value" filter to select all Nebula sandboxes by. App scoping already does it.
func (c *sdkClient) ListSandboxes(ctx context.Context) ([]Sandbox, error) {
	app, err := c.app(ctx)
	if err != nil {
		return nil, fmt.Errorf("modal: resolve app: %w", err)
	}
	seq, err := c.mc.Sandboxes.List(ctx, &modal.SandboxListParams{AppID: app.AppID})
	if err != nil {
		return nil, err
	}
	// seq is an iterator with no length, so out is grown as sandboxes arrive.
	out := make([]Sandbox, 0) //nolint:prealloc // unknown length: iterator, not a slice
	for sb, err := range seq {
		if err != nil {
			return nil, err
		}
		out = append(out, c.observe(ctx, sb))
	}
	return out, nil
}

// observe normalizes a live SDK *Sandbox into the adapter-level Sandbox view:
// status (from Poll), tags (from GetTags), and a best-effort endpoint (from
// Tunnels). Tag/tunnel/poll errors are tolerated so a single flaky sandbox
// doesn't fail the whole List — the poll loop will re-observe next tick.
//
// observe is a BOUNDED read: every call it makes carries a short deadline, so it
// returns promptly even for a sandbox that is still coming up. It must be, since
// it runs once per sandbox inside the List iteration.
func (c *sdkClient) observe(ctx context.Context, sb *modal.Sandbox) Sandbox {
	out := Sandbox{ID: sb.SandboxID}

	// Tags carry Nebula identity (ClaimTagKey), recovered by toInstance, and
	// probe-ness (ProbeTagKey), read by isReady below — so this must precede the
	// status block.
	if tags, err := sb.GetTags(ctx, &modal.SandboxGetTagsParams{}); err == nil {
		out.Tags = tags
	}

	// Status. Poll (== sandboxWait(0)) reports whether the sandbox PROCESS HAS
	// EXITED — a non-nil exit code means it is gone (terminated), nil means it is
	// still live. Poll alone cannot tell "still scheduling" (queued, image pull,
	// GPU attach, container boot) apart from "running and serving": both read as
	// nil. So liveness comes from Poll, and readiness — the part that decides
	// whether the Pod may be advanced to Running — is read from the latch by
	// isReady, which never blocks (see there).
	if code, err := sb.Poll(ctx, &modal.SandboxPollParams{}); err == nil {
		switch {
		case code != nil:
			out.Status = exitStatus(*code)
			c.forgetReady(sb.SandboxID)
		case c.observeReady(ctx, sb.SandboxID, out.Tags):
			out.Status = statusRunning
		default:
			out.Status = statusInitializing
		}
	}

	// Endpoint is only meaningful once running; look it up best-effort.
	// TODO: why we need the tunnel.
	if out.Status == statusRunning {
		tctx, cancel := context.WithTimeout(ctx, c.endpointTimeout)
		if tunnels, err := sb.Tunnels(tctx, c.endpointTimeout, &modal.SandboxTunnelsParams{}); err == nil {
			for _, t := range tunnels {
				out.Endpoint = t.URL()
				break
			}
		}
		cancel()
	}
	return out
}

// Exit codes Modal substitutes for a non-exit outcome, since Poll conforms to the
// subprocess API and has only an int to say it with (see getReturnCode in the
// SDK). Both mean "Modal ended this sandbox", not "the workload failed":
//
//	sandboxExitTerminated  the sandbox was terminated — by our own Terminate on the
//	                       teardown path, or by Modal.
//	sandboxExitTimeout     the sandbox hit its configured Timeout.
//
// They are the conventional signal-derived codes (128+SIGKILL, 128-4), which is
// exactly why they are AMBIGUOUS: a workload that genuinely exits 137 is
// indistinguishable from a Modal termination. See exitStatus for why that is
// tolerable here.
const (
	sandboxExitTerminated = 137
	sandboxExitTimeout    = 124
)

// exitStatus classifies an exited sandbox from its Poll exit code, splitting "it
// failed" from "it is gone".
//
// The distinction is worth recovering because Poll is LOSSY in a way that
// flattens the two: the control plane's GenericResult carries eight statuses
// (SUCCESS, FAILURE, INIT_FAILURE, INTERNAL_FAILURE, TERMINATED, TIMEOUT,
// IDLE_TIMEOUT), and getReturnCode collapses every one of them into a single int
// before we ever see it. Treating any exit as terminated therefore reported a
// sandbox that never came up — a bad image, an unavailable GPU, an OOM at init —
// as "the instance is gone", which reads like a clean teardown and hides the
// failure from whoever has to fix it.
//
// The mapping is deliberately conservative, because the collapse cannot be
// undone: only the two codes Modal SUBSTITUTES for a non-exit outcome, plus a
// clean 0, count as terminated; everything else is the workload's own nonzero
// exit and counts as failed. The ambiguity is real but harmless in the direction
// it errs — a workload exiting exactly 137 is read as terminated rather than
// failed, so an odd exit code can understate a failure. It cannot invent one, and
// it cannot affect teardown either way: both states are terminal for the claim,
// which reclaims by asking the provider what exists, not by reading this.
func exitStatus(code int) string {
	switch code {
	case 0, sandboxExitTerminated, sandboxExitTimeout:
		return statusTerminated
	default:
		return statusFailed
	}
}

// observeReady reports whether a live sandbox may be treated as Running, WITHOUT
// making a network call: it reads the latch and, on a miss, starts the one
// background waiter that will fill it. This is what keeps observe bounded — the
// cost per sandbox per tick is a mutex, not a ~16s blocking wait, so List does
// not degrade with the number of sandboxes.
//
// A sandbox with no probe (no ProbeTagKey) is ready by definition: Modal has no
// readiness concept without one, so there is nothing to wait for and asking would
// error. This folds the startup window into Running for exactly those sandboxes
// Nebula cannot observe — including any created before ProbeTagKey existed.
//
// A miss reports NOT ready. That is the point of the inversion: the previous code
// defaulted to ready and let an ambiguous error promote a sandbox that was still
// booting, which flapped the Pod between Running and Pending (and permanently
// latched its NodeClaim to Bound off one spurious tick). Not-ready is the safe
// default because it is self-correcting — the waiter promotes it within one
// budget — whereas a wrong Running is not.
// It takes no context on purpose: there is nothing here to cancel, which is the
// property that makes it safe to call once per sandbox inside List.
func (c *sdkClient) observeReady(ctx context.Context, id string, tags map[string]string) bool {
	if tags[ProbeTagKey] != probeTagValue {
		return true
	}

	c.readyMu.Lock()
	ready, waiting := c.ready[id], false
	if !ready {
		if _, waiting = c.waiting[id]; !waiting {
			c.waiting[id] = struct{}{}
		}
	}
	c.readyMu.Unlock()

	if !ready && !waiting {
		go c.awaitReady(id)
	}
	return ready
}

// awaitReady runs the one blocking readiness wait for a sandbox and latches the
// result. It runs in its own goroutine, off the poll loop, which is what lets the
// wait have a budget big enough to actually reach an answer.
//
// It takes its own context rather than inheriting the caller's: the caller is
// List, whose context is cancelled the moment List returns — long before this
// wait could finish. The sandbox is re-resolved by id for the same reason, since
// the *modal.Sandbox the List iterator yielded belongs to that finished call.
//
// Only a CONFIRMED answer latches:
//
//   - err == nil: the probe passed.
//   - FailedPrecondition: Modal's "sandbox does not have a readiness probe
//     configured". A definitive no-probe answer, so it latches ready for the same
//     reason a missing ProbeTagKey does. It is reachable when the tag and the
//     actual probe disagree (see sandboxSpecFromPod), and latching keeps that from
//     costing a full wait every tick.
//
// Anything else — a deadline, a transient API failure, a not-found — latches
// nothing and just clears the in-flight marker, so the next tick retries. Note
// the deliberate absence of error classification: the four shapes an expired
// deadline can take (*status.Error with code DeadlineExceeded,
// context.deadlineExceededError, modal.TimeoutError, modal.SandboxTimeoutError —
// and errors.Is(grpcErr, context.DeadlineExceeded) is FALSE for the first) no
// longer need telling apart, because none of them can promote a sandbox now.
func (c *sdkClient) awaitReady(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.readyTimeout)
	defer cancel()

	confirmed := false
	if sb, err := c.mc.Sandboxes.FromID(ctx, id, &modal.SandboxFromIDParams{}); err == nil {
		err := sb.WaitUntilReady(ctx, c.readyTimeout, &modal.SandboxWaitUntilReadyParams{})
		confirmed = err == nil || status.Code(err) == codes.FailedPrecondition
	}

	c.readyMu.Lock()
	defer c.readyMu.Unlock()
	delete(c.waiting, id)
	if confirmed {
		c.ready[id] = true
	}
}

// forgetReady drops a sandbox's latch state. Called when the sandbox is observed
// terminated or explicitly terminated, so the maps track live sandboxes only and
// a recycled id can never inherit a stale ready.
func (c *sdkClient) forgetReady(id string) {
	c.readyMu.Lock()
	defer c.readyMu.Unlock()
	delete(c.ready, id)
	delete(c.waiting, id)
}

// gpuReservation renders Modal's GPU reservation string. Modal expresses count
// as a "type:count" suffix (e.g. "A100:2"); a count of 0/1 needs no suffix, and
// an empty type means a CPU-only sandbox.
func gpuReservation(gpuType string, count int32) string {
	if gpuType == "" {
		return ""
	}
	if count > 1 {
		return gpuType + ":" + strconv.FormatInt(int64(count), 10)
	}
	return gpuType
}

// isNotFound reports whether err indicates the sandbox no longer exists, so
// Terminate/Get can treat it as already gone (idempotency). The SDK does not
// export a typed not-found error at this beta version, so we match on message.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") || strings.Contains(msg, "notfound") ||
		strings.Contains(msg, "does not exist")
}
