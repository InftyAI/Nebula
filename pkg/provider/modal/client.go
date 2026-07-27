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
	"time"

	modal "github.com/modal-labs/modal-client/go"
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
	if p == nil {
		return nil, nil
	}
	// PeriodSeconds maps to the probe interval; zero leaves the SDK default (the
	// SDK constructors reject a zero interval).
	var interval time.Duration
	if p.PeriodSeconds > 0 {
		interval = time.Duration(p.PeriodSeconds) * time.Second
	}
	switch {
	case p.Exec != nil && len(p.Exec.Command) > 0:
		return modal.NewExecProbe(p.Exec.Command, &modal.ExecProbeParams{Interval: interval})
	case p.TCPSocket != nil:
		if port, ok := numericPort(p.TCPSocket.Port); ok {
			return modal.NewTCPProbe(port, &modal.TCPProbeParams{Interval: interval})
		}
		return nil, nil // named port: unsupported here (see doc)
	case p.HTTPGet != nil:
		if port, ok := numericPort(p.HTTPGet.Port); ok {
			return modal.NewTCPProbe(port, &modal.TCPProbeParams{Interval: interval})
		}
		return nil, nil // named port: unsupported here (see doc)
	default:
		return nil, nil
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
// observe is a POINT-IN-TIME read and never blocks: it reports the status as
// currently known and returns immediately. It deliberately does not call
// WaitUntilReady (the only Modal readiness signal), because that blocks until the
// probe first passes and would stall the List for as long as a sandbox takes to
// come up.
func (c *sdkClient) observe(ctx context.Context, sb *modal.Sandbox) Sandbox {
	out := Sandbox{ID: sb.SandboxID}

	// Tags carry Nebula identity (ClaimTagKey), recovered by toInstance.
	if tags, err := sb.GetTags(ctx, &modal.SandboxGetTagsParams{}); err == nil {
		out.Tags = tags
	}

	// Status. Poll (== sandboxWait(0)) is the only cheap point-in-time signal: it
	// reports whether the sandbox PROCESS HAS EXITED — a non-nil exit code means it
	// is gone (terminated), nil means it is still live. Poll cannot tell "still
	// scheduling" (queued, image pull, GPU attach, container boot) apart from
	// "running and serving" — both read as nil — and Modal exposes no cheap
	// readiness readback (WaitUntilReady blocks, so we do not call it here). So a
	// live sandbox is reported "running" as soon as its process exists, folding the
	// brief startup window into running rather than blocking to confirm readiness.
	if code, err := sb.Poll(ctx, &modal.SandboxPollParams{}); err == nil {
		if code != nil {
			out.Status = statusTerminated
		} else {
			out.Status = statusRunning
		}
	}

	// Endpoint is only meaningful once running; look it up best-effort.
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
