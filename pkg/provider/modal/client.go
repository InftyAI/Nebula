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
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	modal "github.com/modal-labs/modal-client/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/provider/catalog"
)

// sdkClient is the real Client, backed by Modal's official Go SDK (beta). Every
// Modal-specific SDK call lives here so the adapter and its tests stay SDK-free.
//
// A NodeClaim maps to one Modal Sandbox, identified by the sandbox's native Tags
// (ClaimTagKey) — server-side filterable, so List/Get need no naming hack.
type sdkClient struct {
	mc      *modal.Client
	appName string

	// readyTimeout is how long one background readiness waiter gets. WaitUntilReady
	// returns early only to say "ready", never "not ready", so a budget under the call's
	// own setup cost yields a deadline rather than an answer. Setup dominates (~16s cold:
	// poll for the task id, then dial a fresh TLS gRPC connection to its router), so keep
	// this comfortably above it — affordable now that the wait is off the read path.
	readyTimeout time.Duration

	// Readiness latch. WaitUntilReady is a one-shot blocking WAIT, but observe is a
	// level-triggered READ answering from state on every tick — wrapping the wait in a
	// short timeout to fake a read made a timeout masquerade as "not ready". So the wait
	// runs once in the background and latches its result here.
	//
	// ready is set on CONFIRMED readiness only, so an ambiguous error cannot promote a
	// sandbox that is still coming up. waiting dedupes waiters so repeated ticks don't
	// pile up goroutines. Both are keyed by sandbox id and dropped by forgetReady.
	//
	// The latch never DEMOTES: a probe that passes and later fails leaves the sandbox
	// Running. Poll still catches process exit, so death is noticed; sickness is not.
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
		mc:           mc,
		appName:      appName,
		readyTimeout: 30 * time.Second,
		ready:        make(map[string]bool),
		waiting:      make(map[string]struct{}),
	}, cat), nil
}

// app resolves (creating if missing) the Modal App all sandboxes live under.
func (c *sdkClient) app(ctx context.Context) (*modal.App, error) {
	return c.mc.Apps.FromName(ctx, c.appName, &modal.AppFromNameParams{CreateIfMissing: true})
}

// CreateSandbox implements Client.
func (c *sdkClient) CreateSandbox(ctx context.Context, spec SandboxSpec) (string, Credential, error) {
	app, err := c.app(ctx)
	if err != nil {
		return "", Credential{}, fmt.Errorf("modal: resolve app: %w", err)
	}
	if spec.Image == "" {
		return "", Credential{}, fmt.Errorf("modal: empty image in sandbox spec")
	}
	image := c.mc.Images.FromRegistry(spec.Image, nil)

	probe, err := modalProbe(spec.ReadinessProbe)
	if err != nil {
		return "", Credential{}, fmt.Errorf("modal: readiness probe: %w", err)
	}

	sb, err := c.mc.Sandboxes.Create(ctx, app, image, &modal.SandboxCreateParams{
		Command:        spec.Command,
		Env:            spec.Env,
		GPU:            gpuReservation(spec.GPU, spec.GPUCount),
		CPU:            spec.CPU,
		MemoryMiB:      spec.MemoryMiB,
		EncryptedPorts: spec.Ports,
		// Nil leaves Modal's SchedulerPlacement unset entirely (the SDK only builds one
		// when Regions is non-empty), which is the unconstrained, un-multiplied case.
		Regions:        spec.Regions,
		Timeout:        spec.Timeout,
		Tags:           spec.Tags,
		ReadinessProbe: probe,
	})
	if err != nil {
		return "", Credential{}, err
	}
	return sb.SandboxID, c.mintCredential(ctx, sb, spec), nil
}

// mintCredential issues the sandbox's connect credential and RETURNS it, storing nothing.
// Not in a tag, since Modal's tags are plaintext and bulk-listable — one ListSandboxes
// would hand over every workload's token. Not in memory, which is not durable. A
// credential belongs in an access-controlled Secret, and this layer has no cluster
// access, so it hands the pair up to the virtual kubelet, which writes it.
//
// Minting is one-shot: every CreateConnectToken call mints a FRESH token, with no
// read-back. A caller that drops the return value has lost it for the sandbox's life.
// That is also why this cannot move to the read path — observe would hand out a token
// that changed every tick. (The endpoint lives on the Pod annotation instead, so Modal
// reports no observed endpoint at all; see observe.)
//
// It can run this early because the RPC needs only the sandbox id and port — no task id,
// no running container, no booted GPU (contrast Tunnels, which needs the container up).
// So the credential is in hand while the sandbox is still queued.
//
// Every workload gets one: an authenticated URL is the only general way to reach a
// NeoCloud instance, and a workload with nothing to serve just leaves it unused. The URL
// routes to the first of spec.Ports, or Modal's default 8080 if none are declared.
//
// TODO: a Sandbox is reached by identity (`kubectl exec sbx-alice`), not by address, so it
// should not get a credential — it does today because it arrives here as an ordinary Pod.
//
// Best-effort: a sandbox that exists must be reported and reclaimed whether or not it got
// a credential, so a failure returns the zero Credential and the instance is simply
// unreachable. Returning the error would fail a Provision whose sandbox is already
// running, leaking a paid instance to save an address. The text is dropped rather than
// logged because it can echo the request.
func (c *sdkClient) mintCredential(ctx context.Context, sb *modal.Sandbox, spec SandboxSpec) Credential {
	creds, err := sb.CreateConnectToken(ctx, &modal.SandboxCreateConnectTokenParams{
		// Derived from the exposed set rather than carried separately, so the routed
		// port cannot name one the sandbox was never told to accept traffic on.
		Port: firstPort(spec.Ports),
	})
	if err != nil || creds == nil || creds.Token == "" {
		return Credential{}
	}
	return Credential{URL: creds.URL, Token: creds.Token}
}

// modalProbe maps a Pod readinessProbe onto Modal's Probe. Modal supports only
// TCP and Exec probes, so an HTTPGet probe degrades to a TCP probe on its port
// (readiness ≈ the port accepting connections). Returns (nil, nil) when p is nil
// or names no supported handler, so a probe-less (or unsupported) workload simply
// gets no Modal probe. PeriodSeconds maps to the probe interval; zero leaves the
// SDK default (a zero interval is rejected by the SDK constructors).
//
// A NAMED port (port: http) counts as unsupported: resolving it needs the container's
// ports list, which this helper does not have, and the intstr's 0 fallback would build an
// invalid Modal probe on port 0. Better to omit the probe than emit a bogus one.
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
// Split out from modalProbe so the CREATE path and the ProbeTagKey gate cannot disagree:
// the tag asserts "Modal received a probe", and deriving it from `ReadinessProbe != nil`
// made that a lie for every shape modalProbe drops. One predicate, two callers, no drift.
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
	out, err := c.observe(ctx, sb)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// SandboxLogs implements Client: stdout and stderr merged into one stream, from the
// sandbox's first byte, following until it exits — the shape provider.LogStreamer
// requires.
//
// The SDK's streams already have those semantics (SandboxGetLogs with LastEntryId
// "0-0" replays then long-polls), so there is no offset bookkeeping here. They are
// lazy: no gRPC call until the first Read.
//
// Interleaving is best-effort — the two streams share no sequence, so stderr can
// appear before an earlier stdout line. Same guarantee the kubelet gives, and dropping
// stderr would hide the output users read logs to find.
func (c *sdkClient) SandboxLogs(ctx context.Context, id string) (io.ReadCloser, error) {
	sb, err := c.mc.Sandboxes.FromID(ctx, id, &modal.SandboxFromIDParams{})
	if err != nil {
		if isNotFound(err) {
			// An error, not an empty stream: silence would read as "logged nothing".
			return nil, fmt.Errorf("modal: sandbox %s not found: %w", id, err)
		}
		return nil, err
	}
	return mergeStreams(ctx, sb.Stdout, sb.Stderr), nil
}

// SandboxExec implements Client: it starts cmd in the sandbox's running container and
// hands back the process handle, which is what `kubectl exec` is pumped from.
//
// No agent is installed for this. Modal's own worker runs the command, so exec works on
// any image — but it goes through the container's TASK, so the sandbox has to be running:
// the SDK polls briefly for a task id and then errors, which is the honest answer for a
// sandbox still queued for a GPU.
//
// A TTY is requested when the client asked for one, and Modal then multiplexes stderr into
// stdout — the same thing a real terminal does. Its window size is Modal's fixed 24x80: the
// SDK exposes no way to set it and the command router has no resize call.
//
// ctx bounds the start only, as provider.Executor requires. That holds because the SDK runs
// the stdio streams and stdin writes on their own background contexts — the sandbox lookup,
// the task-id wait and ExecStart are all this ctx covers, and those are exactly what a
// caller wants to give up on.
//
// Setup is paid PER EXEC and dominates it (~16s cold: resolve the sandbox, poll for its
// task id, dial a fresh TLS connection to that task's router). Caching the handle would
// make later execs cheap, but the manager would hold one connection per sandbox with
// nothing reliable to close it — a sandbox that dies on its own leaves Modal's list
// (IncludeFinished: false) and is never observed again. A slow exec beats a leak here.
//
// No Timeout is set, so the command runs until it exits or the sandbox does. Any cap we
// picked would truncate somebody's job — and, since it surfaces as a stream error rather
// than an exit code, it would look like a network glitch. The cost is that a command whose
// client disconnected keeps running: Modal has no "kill this exec" call, and closing the
// connection leaves the process alive in the container. The sandbox's own lifetime is what
// finally reaps it.
func (c *sdkClient) SandboxExec(
	ctx context.Context, id string, cmd []string, opts provider.ExecOptions,
) (provider.Process, error) {
	sb, err := c.mc.Sandboxes.FromID(ctx, id, &modal.SandboxFromIDParams{})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("modal: sandbox %s not found: %w", id, err)
		}
		return nil, err
	}
	cp, err := sb.Exec(ctx, cmd, &modal.SandboxExecParams{PTY: opts.TTY})
	if err != nil {
		return nil, fmt.Errorf("modal: exec in sandbox %s: %w", id, err)
	}
	return &sandboxProcess{sb: sb, cp: cp, tty: opts.TTY}, nil
}

// sandboxProcess adapts Modal's ContainerProcess to provider.Process.
//
// It holds the *modal.Sandbox as well as the process because Exec dialled a private gRPC
// connection to the container's command router through it, and Detach is the only way to
// close that — without it every exec would leak a connection for the manager's life.
type sandboxProcess struct {
	sb  *modal.Sandbox
	cp  *modal.ContainerProcess
	tty bool

	closeOnce sync.Once
}

var _ provider.Process = (*sandboxProcess)(nil)

func (p *sandboxProcess) Stdin() io.WriteCloser { return p.cp.Stdin }
func (p *sandboxProcess) Stdout() io.Reader     { return p.cp.Stdout }

// Stderr is nil under a TTY: Modal multiplexes both streams into stdout there, leaving
// this one permanently empty, and reading it would just park a goroutine.
func (p *sandboxProcess) Stderr() io.Reader {
	if p.tty {
		return nil
	}
	return p.cp.Stderr
}

// Wait blocks until the command exits and reports its exit code. Modal renders a signal
// death as 128+signal, the shell convention, so ^C reads as 130.
func (p *sandboxProcess) Wait(ctx context.Context) (int, error) {
	return p.cp.Wait(ctx, &modal.ContainerProcessWaitParams{})
}

// Close releases the streams and the command-router connection. Idempotent, since the
// caller may close after a teardown already ran. Errors are dropped: the exec is over
// either way, and there is no caller left to act on them.
func (p *sandboxProcess) Close() error {
	p.closeOnce.Do(func() {
		_ = p.cp.Stdin.Close()
		_ = p.cp.Stdout.Close()
		_ = p.cp.Stderr.Close()
		_ = p.sb.Detach()
	})
	return nil
}

// mergeStreams fans log streams into one ReadCloser. Close tears everything down: it
// closes the sources (unblocking the copiers) and the pipe (returning an in-flight
// Read). ctx does the same, so a disconnected `kubectl logs -f` cannot leave
// goroutines long-polling Modal forever.
//
// A stream that FAILS ends the merged stream with its error, not at EOF. Silence is the
// normal answer here — Modal serves only recent output — so a swallowed error made an
// outage look exactly like a workload that had logged nothing.
func mergeStreams(ctx context.Context, streams ...io.ReadCloser) io.ReadCloser {
	pr, pw := io.Pipe()

	// Closed before the sources are, so a read that fails because WE tore down is not
	// reported as Modal breaking.
	tearing := make(chan struct{})

	var (
		mu       sync.Mutex
		firstErr error
	)
	// io.Copy reports EOF as success, so anything arriving here is a real failure —
	// except a closed pipe, which is the client having gone away.
	fail := func(err error) {
		if err == nil || errors.Is(err, io.ErrClosedPipe) {
			return
		}
		select {
		case <-tearing:
			return
		default:
		}
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}

	var wg sync.WaitGroup
	for _, s := range streams {
		if s == nil {
			continue
		}
		wg.Add(1)
		go func(src io.ReadCloser) {
			defer wg.Done()
			// Held, not raised here: one stream ending must not truncate the other.
			_, err := io.Copy(pw, src)
			fail(err)
		}(s)
	}

	// Only close the pipe once BOTH sources are done, so EOF means no more output.
	go func() {
		wg.Wait()
		mu.Lock()
		err := firstErr
		mu.Unlock()
		// CloseWithError(nil) is a plain EOF, so this covers both endings.
		_ = pw.CloseWithError(err)
	}()

	closeAll := func() {
		for _, s := range streams {
			if s != nil {
				_ = s.Close()
			}
		}
	}
	// ctx and Close converge on the same teardown, safe to run twice. stop is buffered
	// so a Close after ctx already fired does not block.
	stop := make(chan struct{}, 1)
	go func() {
		select {
		case <-ctx.Done():
		case <-stop:
		}
		close(tearing)
		closeAll()
		_ = pw.CloseWithError(ctx.Err())
	}()

	return &mergedStream{ReadCloser: pr, stop: stop}
}

// mergedStream is the pipe plus the teardown signal, so Close releases the upstream
// Modal streams too — the bare pipe would leak the copiers.
type mergedStream struct {
	io.ReadCloser
	stop     chan struct{}
	stopOnce sync.Once
}

func (m *mergedStream) Close() error {
	m.stopOnce.Do(func() { close(m.stop) })
	return m.ReadCloser.Close()
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
		observed, err := c.observe(ctx, sb)
		if err != nil {
			return nil, err
		}
		out = append(out, observed)
	}
	return out, nil
}

// observe normalizes a live SDK *Sandbox into the adapter-level Sandbox view: tags
// (from GetTags) and status (from Poll). A Poll error is tolerated so a single flaky
// sandbox doesn't fail the whole read — the poll loop will re-observe next tick. A TAG
// error is not: see below.
//
// observe is a CHEAP read, and it must be: it runs once per sandbox inside the List
// iteration, on every poll tick. That is why it reports no endpoint (see the tail of
// this function) — an address lookup here would be a per-sandbox round trip on the
// hot path.
func (c *sdkClient) observe(ctx context.Context, sb *modal.Sandbox) (Sandbox, error) {
	out := Sandbox{ID: sb.SandboxID}

	// Tags carry Nebula identity (ClaimTagKey), recovered by toInstance, and
	// probe-ness (ProbeTagKey), read by observeReady below — so this must precede the
	// status block.
	//
	// A read failure is NOT "no tags", and cannot be tolerated: without ClaimTagKey the
	// sandbox has no recoverable identity, and the poll loop reads that as "the instance
	// is gone" → Pod Terminated, which is terminal-sticky and makes the NodeClaim reclaim
	// a live sandbox. A missing ProbeTagKey would likewise report a booting sandbox ready.
	//
	// So the error fails the whole read, which callers already treat as "do not act on a
	// half-known fleet" and retry. A stalled tick is recoverable; a stuck terminal phase
	// is not.
	tags, err := sb.GetTags(ctx, &modal.SandboxGetTagsParams{})
	if err != nil {
		return out, fmt.Errorf("modal: read tags for sandbox %s: %w", sb.SandboxID, err)
	}
	out.Tags = tags

	// Status. Poll says only whether the PROCESS HAS EXITED: a non-nil exit code means
	// gone, nil means still live. It cannot tell "still scheduling" (queued, image pull,
	// GPU attach, boot) from "running and serving" — both read as nil. So liveness comes
	// from Poll, and readiness from the latch via observeReady, which never blocks.
	if code, err := sb.Poll(ctx, &modal.SandboxPollParams{}); err == nil {
		switch {
		case code != nil:
			out.Status = exitStatus(*code)
			c.forgetReady(sb.SandboxID)
		case c.observeReady(sb.SandboxID, out.Tags):
			out.Status = statusRunning
		default:
			out.Status = statusInitializing
		}
	}

	// No endpoint is read back. The sandbox's reachable address is its connect URL,
	// minted at create and already persisted on the Pod's endpoint annotation, so
	// re-deriving it per tick would be a round trip for a value the API server holds.
	//
	// The alternative — a tunnel URL — is worse than nothing: a tunnel is PUBLIC to
	// whoever learns it, so substituting one for an authenticated URL silently downgrades
	// access. A sandbox whose mint failed reports no address at all, and that stays a FACT
	// rather than an error, since observe's errors fail the entire List.
	return out, nil
}

// Exit codes Modal substitutes for a non-exit outcome, since Poll follows the subprocess
// API and has only an int to say it with. Both mean "Modal ended this sandbox" (by our
// Terminate, or on the configured Timeout), not "the workload failed".
//
// Being the conventional signal-derived codes makes them AMBIGUOUS: a workload that truly
// exits 137 is indistinguishable from a Modal termination. See exitStatus.
const (
	sandboxExitTerminated = 137
	sandboxExitTimeout    = 124
)

// exitStatus classifies an exited sandbox from its Poll exit code, splitting "it failed"
// from "it is gone". Poll flattens the two: Modal's control plane has eight result
// statuses and collapses them all into one int before we see it. Treating every exit as
// terminated reported a sandbox that never came up — bad image, no GPU, OOM at init — as a
// clean teardown, hiding the failure.
//
// Conservative, since the collapse cannot be undone: only the two substituted codes plus a
// clean 0 count as terminated, everything else is the workload's own nonzero exit. So a
// workload exiting exactly 137 understates a failure — but it can never invent one, and
// teardown is unaffected either way (both states are terminal for the claim, which
// reclaims by asking the provider what exists).
func exitStatus(code int) string {
	switch code {
	case 0, sandboxExitTerminated, sandboxExitTimeout:
		return statusTerminated
	default:
		return statusFailed
	}
}

// observeReady reports whether a live sandbox may be treated as Running WITHOUT a network
// call: it reads the latch and, on a miss, starts the one background waiter that fills it.
// That keeps observe bounded — a mutex per sandbox per tick, not a ~16s blocking wait — so
// List does not degrade with fleet size. It takes no context because there is nothing to
// cancel, which is what makes it safe to call inside List.
//
// A sandbox with no probe (no ProbeTagKey) is ready by definition: Modal has no readiness
// concept without one, so there is nothing to wait for and asking would error.
//
// A miss reports NOT ready. Not-ready is the safe default because it self-corrects — the
// waiter promotes it within one budget — whereas a wrong Running does not, and flapped the
// Pod while permanently latching its NodeClaim to Bound.
func (c *sdkClient) observeReady(id string, tags map[string]string) bool {
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

// awaitReady runs the one blocking readiness wait for a sandbox and latches the result.
// Being off the poll loop is what lets the wait have a budget big enough to reach an
// answer. It builds its own context and re-resolves the sandbox by id because the caller
// is List, whose context (and *modal.Sandbox) die the moment List returns.
//
// Only a CONFIRMED answer latches: err == nil (the probe passed), or FailedPrecondition,
// which is Modal saying the sandbox has no probe configured — a definitive no-probe
// answer, reachable when the tag and the actual probe disagree, and latching it avoids
// paying a full wait every tick.
//
// Anything else — deadline, transient failure, not-found — latches nothing and just clears
// the in-flight marker, so the next tick retries. No error classification is needed now
// that none of those can promote a sandbox.
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
