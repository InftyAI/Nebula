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

	sb, err := c.mc.Sandboxes.Create(ctx, app, image, &modal.SandboxCreateParams{
		Command:        spec.Command,
		Env:            spec.Env,
		GPU:            gpuReservation(spec.GPU, spec.GPUCount),
		CPU:            spec.CPU,
		MemoryMiB:      spec.MemoryMiB,
		EncryptedPorts: spec.Ports,
		Timeout:        spec.Timeout,
		Tags:           spec.Tags,
	})
	if err != nil {
		return "", err
	}
	return sb.SandboxID, nil
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

// ListSandboxes implements Client. Filters server-side by the Nebula claim tag
// key so only Nebula-owned sandboxes are returned.
func (c *sdkClient) ListSandboxes(ctx context.Context) ([]Sandbox, error) {
	app, err := c.app(ctx)
	if err != nil {
		return nil, fmt.Errorf("modal: resolve app: %w", err)
	}
	seq, err := c.mc.Sandboxes.List(ctx, &modal.SandboxListParams{AppID: app.AppID})
	if err != nil {
		return nil, err
	}
	var out []Sandbox
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
func (c *sdkClient) observe(ctx context.Context, sb *modal.Sandbox) Sandbox {
	out := Sandbox{ID: sb.SandboxID}

	// Status. Poll (== sandboxWait(0)) reports whether the sandbox PROCESS HAS
	// EXITED: a non-nil exit code means it is gone (terminated), nil means it is
	// still live. We treat "still live" as running. This does fold the brief
	// startup window (queued, image pull, GPU attach, container boot) into
	// "running" rather than "pending", because the SDK at this version exposes no
	// lightweight readiness readback: WaitUntilReady only resolves against a
	// readiness probe configured at create time (which we do not set) and requires
	// a live task-command-router connection, so it can never report ready here.
	// Poll is the only reliable signal, and reporting a starting sandbox as running
	// a few seconds early is far better than the alternative — never reporting it
	// ready at all, which wedges the Pod at Pending and the NodeClaim at Provisioning.
	if code, err := sb.Poll(ctx, &modal.SandboxPollParams{}); err == nil {
		if code != nil {
			out.Status = "terminated"
		} else {
			out.Status = "running"
		}
	}

	if tags, err := sb.GetTags(ctx, &modal.SandboxGetTagsParams{}); err == nil {
		out.Tags = tags
	}

	// Endpoint is only meaningful once running; look it up best-effort.
	if out.Status == "running" {
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
