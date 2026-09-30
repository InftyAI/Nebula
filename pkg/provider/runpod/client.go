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

package runpod

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/provider/catalog"
	"github.com/InftyAI/Nebula/pkg/util"
)

// The RunPod REST API v2 (https://docs.runpod.io/api-reference-v2/overview; v1 retires
// 2026-11-15). There is no official Go SDK, so this is plain net/http — which is also why the
// whole surface is one small file: the adapter needs five operations.
const (
	// defaultBaseURL is RunPod's API host; every path carries its own /v2 prefix.
	// Overridable only in tests (see newClient).
	defaultBaseURL = "https://api.runpod.io"
	// apiKeyEnv is where the credential comes from. It is delivered by the per-provider
	// Secret the manager mounts via envFrom; absent means the provider is skipped at
	// registration rather than failing the process.
	apiKeyEnv = "RUNPOD_API_KEY"
	// requestTimeout bounds one HTTP call. Generous because a create allocates a machine
	// server-side, but finite: a hung call would otherwise pin a Provision until the
	// caller's own context expired.
	requestTimeout = 60 * time.Second
	// maxResponseBytes caps how much of a response is read, so a malformed or hostile
	// response cannot exhaust memory. A full Pod list is a few KB per Pod.
	maxResponseBytes = 8 << 20
	// maxErrorBodyChars caps how much of an error response reaches the error string, which
	// is logged and may land on a Pod condition.
	maxErrorBodyChars = 512
	// cloudTypeSecure is the only cloud type Nebula requests; see the package doc for why
	// COMMUNITY is out until the catalog can price it.
	cloudTypeSecure = "SECURE"
	// cpuFlavor is the CPU flavor a CPU-only Pod runs on; v2 requires one. A flavor fixes
	// memory as a multiple of vCPUs, so the Pod's memory request has nowhere to go.
	cpuFlavor = "cpu5c"
	// listPageSize is the largest page GET /v2/pods serves.
	listPageSize = 1000
)

// restClient is the real Client, backed by RunPod's REST API. Every RunPod-specific HTTP
// call lives here so the adapter and its tests stay transport-free.
type restClient struct {
	http    *http.Client
	baseURL string
	apiKey  string
}

// compile-time assertion that restClient satisfies the adapter's Client seam.
var _ Client = (*restClient)(nil)

// NewSDKClient builds a RunPod-backed Provider, reading the API key from RUNPOD_API_KEY.
// An absent key is an ERROR rather than a client that fails on first use, so
// registerProviders can log and skip RunPod the same way it skips Modal and AWS.
//
// The context is accepted for symmetry with the other adapters' constructors (and so a
// future availability probe can use it); nothing here makes a call.
func NewSDKClient(_ context.Context) (*Provider, error) {
	apiKey := strings.TrimSpace(os.Getenv(apiKeyEnv))
	if apiKey == "" {
		return nil, fmt.Errorf("runpod: %s is not set", apiKeyEnv)
	}
	cat, err := catalog.Load()
	if err != nil {
		return nil, fmt.Errorf("runpod: load price catalog: %w", err)
	}
	return New(newClient(defaultBaseURL, apiKey), cat), nil
}

// newClient builds a restClient against baseURL. Separate from NewSDKClient so a test can
// point it at an httptest.Server.
func newClient(baseURL, apiKey string) *restClient {
	return &restClient{
		http:    &http.Client{Timeout: requestTimeout},
		baseURL: strings.TrimSuffix(baseURL, "/"),
		apiKey:  apiKey,
	}
}

// apiError is one non-2xx RunPod response. It carries the status and RunPod's own message
// so the classify helpers can key on both, and so an operator reading a log sees what
// RunPod actually said.
//
// It deliberately holds NOTHING from the REQUEST body: that body carries the workload's
// resolved environment and, on a registry-auth create, a registry password. Only the method
// and path are echoed back.
type apiError struct {
	status  int
	method  string
	path    string
	message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("runpod: %s %s: HTTP %d: %s", e.method, e.path, e.status, e.message)
}

// notFound reports whether err is a 404. Both Get and Terminate treat that as "already
// gone" rather than a failure, which is what makes Terminate idempotent for the finalizer.
func notFound(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.status == http.StatusNotFound
}

// do performs one API call: body is JSON-encoded when non-nil, out is JSON-decoded when
// non-nil, and any non-2xx becomes an *apiError.
func (c *restClient) do(ctx context.Context, method, path string, body, out any) error {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("runpod: encode %s %s request: %w", method, path, err)
		}
		payload = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, payload)
	if err != nil {
		return fmt.Errorf("runpod: build %s %s request: %w", method, path, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		// A transport failure, wrapped WITHOUT a sentinel on purpose: nobody knows whether
		// RunPod acted on the request, so it must stay unattributable and blocklist nothing.
		return fmt.Errorf("runpod: %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("runpod: %s %s: read response: %w", method, path, err)
	}
	if resp.StatusCode >= http.StatusMultipleChoices {
		return &apiError{
			status:  resp.StatusCode,
			method:  method,
			path:    path,
			message: errorMessage(raw),
		}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("runpod: %s %s: decode response: %w", method, path, err)
	}
	return nil
}

// errorMessage pulls the human-readable part out of an error response: v2's RFC 9457 body
// ({title, status, detail, errors}), falling back to the raw text — some gateway errors are
// HTML, and an empty message would leave the classifier nothing to read.
//
// The per-field errors are appended to the detail because a 422 puts the reason ONLY there.
func errorMessage(raw []byte) string {
	var problem struct {
		Title  string   `json:"title"`
		Detail string   `json:"detail"`
		Errors []string `json:"errors"`
	}
	if err := json.Unmarshal(raw, &problem); err == nil {
		msg := problem.Detail
		if msg == "" {
			msg = problem.Title
		}
		if len(problem.Errors) > 0 {
			msg = strings.TrimSpace(msg + ": " + strings.Join(problem.Errors, "; "))
		}
		if msg != "" {
			return truncate(msg)
		}
	}
	return truncate(strings.TrimSpace(string(raw)))
}

// truncate bounds a message so an error string stays loggable.
func truncate(s string) string {
	if len(s) <= maxErrorBodyChars {
		return s
	}
	return s[:maxErrorBodyChars] + "…"
}

// classifyCreate wraps a create failure with the shared sentinel that matches it, which is
// what lets the control plane act on the failure without knowing anything about RunPod (see
// docs/add-a-provider.md, "Wrap the errors your Provision returns").
//
// The status table follows RunPod's own guidance for POST /v2/pods. Its gotcha is 400: it
// means both "this GPU and data center could not be placed" and "the body breaks a
// cross-field rule", with no machine-readable code to tell them apart, so only a detail
// that reads as capacity is treated as one.
func classifyCreate(err error) error {
	var ae *apiError
	if !errors.As(err, &ae) {
		return err // a transport/encode failure: unattributable, and already wrapped
	}
	msg := strings.ToLower(ae.message)

	switch {
	case ae.status >= http.StatusInternalServerError:
		// Left UNWRAPPED, deliberately. A 5xx says RunPod failed to answer, not that it
		// said no, and it may well have created the Pod before falling over.
		return err

	case ae.status == http.StatusUnauthorized:
		// Whole-provider: nothing succeeds until the key is fixed.
		return fmt.Errorf("%w: %w", err, provider.ErrAuth)

	case ae.status == http.StatusForbidden:
		// NOT auth, on create: RunPod documents it as "your account cannot access the
		// requested pool", to be skipped for the next candidate. DenyAll would fence off
		// every other pool the account can use.
		return fmt.Errorf("%w: %w", err, provider.ErrUnsupportedAccelerator)

	// Money, not capacity, but scoped the same way: it is transient, it is not an
	// authentication problem, and ErrQuota is the sentinel for "a limit stopped this".
	case ae.status == http.StatusPaymentRequired, ae.status == http.StatusTooManyRequests,
		util.ContainsAny(msg, "insufficient funds", "insufficient balance", "not enough credit"):
		return fmt.Errorf("%w: %w", err, provider.ErrQuota)

	case util.ContainsAny(msg, "no longer any instances available", "no instances available",
		"no instance available", "out of capacity", "no capacity", "not available",
		"unavailable", "sold out", "could not be placed"):
		return fmt.Errorf("%w: %w", err, provider.ErrNoCapacity)

	case util.ContainsAny(msg, "invalid gpu", "unknown gpu", "gpu type", "unsupported"):
		// A GPU id RunPod does not recognize: durable until runpod.csv is corrected, and
		// accelerator-scoped so the rest of the provider stays usable.
		return fmt.Errorf("%w: %w", err, provider.ErrUnsupportedAccelerator)

	case util.ContainsAny(msg, "registry", "image", "pull", "manifest"):
		// Belongs to the REQUEST, not the candidate, so it must blocklist NOTHING. The phrase
		// is what provider.ClassifyError keys on; left bare, a registry's "unauthorized" would
		// read as OUR auth failing and fence the whole provider.
		return fmt.Errorf("runpod: image pull credential or image rejected: %w", err)

	default:
		// A 422 or an unrecognized 400. Left unwrapped rather than guessed at: every
		// available sentinel is worse — ErrAuth would fence off the whole provider, a
		// capacity wrap would evict a healthy candidate.
		return err
	}
}

// createPodRequest is RunPod's POST /v2/pods body. Only the fields Nebula sets are present;
// everything omitted takes RunPod's own default, which is the point of the omitempty tags —
// a zero we did not mean would override a sane default with 0.
//
// No mounts: a Nebula instance is cattle with nothing to persist, and v2 attaches no
// persistent volume unless asked (v1 defaulted to a billable 20 GiB one).
type createPodRequest struct {
	Name  string      `json:"name"`
	Image string      `json:"image"`
	Cloud string      `json:"cloud"`
	GPU   *gpuRequest `json:"gpu,omitempty"`
	CPU   *cpuRequest `json:"cpu,omitempty"`

	Disk       int               `json:"disk,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	Entrypoint []string          `json:"entrypoint,omitempty"`
	Cmd        []string          `json:"cmd,omitempty"`
	Ports      []string          `json:"ports,omitempty"`

	DataCenterIDs []string `json:"dataCenterIds,omitempty"`
	Registry      string   `json:"registry,omitempty"`
}

// gpuRequest is a GPU Pod's compute. The per-GPU minimums are placement filters, not
// reservations: RunPod may hand out more, never less.
type gpuRequest struct {
	ID                 string `json:"id"`
	Count              int32  `json:"count"`
	MinVCPUCountPerGPU int    `json:"minVcpuCountPerGpu,omitempty"`
	MinRAMPerGPU       int    `json:"minRamPerGpu,omitempty"`
}

// cpuRequest is a CPU-only Pod's compute; v2 takes exactly one of it or gpuRequest.
type cpuRequest struct {
	ID        string `json:"id"`
	VCPUCount int    `json:"vcpuCount"`
}

// podResponse is the subset of RunPod's Pod object this adapter reads. Fields it ignores
// (ssh, cost, template, mounts) are omitted rather than carried, so the struct states exactly
// what the adapter's behaviour depends on.
type podResponse struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Status string   `json:"status"`
	Ports  []string `json:"ports"`
	// DataCenterID is null until the scheduler assigns one, which decodes to "" — Region
	// then stays empty rather than reporting a placement we did not observe.
	DataCenterID string `json:"dataCenterId"`
	// Runtime is null unless the Pod is RUNNING.
	Runtime *struct {
		Ports []struct {
			Private int     `json:"private"`
			Public  *int    `json:"public"`
			IP      *string `json:"ip"`
		} `json:"ports"`
	} `json:"runtime"`
}

// toPod converts the wire shape into the adapter's view. Only a port RunPod has published on
// a public IP becomes a mapping; the rest are reachable through the proxy alone.
func (r podResponse) toPod() Pod {
	pd := Pod{
		ID:           r.ID,
		Name:         r.Name,
		Status:       r.Status,
		Ports:        r.Ports,
		DataCenterID: r.DataCenterID,
	}
	if r.Runtime == nil {
		return pd
	}
	for _, m := range r.Runtime.Ports {
		if m.Public == nil || m.IP == nil || *m.IP == "" {
			continue
		}
		if pd.PortMappings == nil {
			pd.PortMappings = make(map[string]int)
		}
		pd.PortMappings[strconv.Itoa(m.Private)] = *m.Public
		pd.PublicIP = *m.IP
	}
	return pd
}

// CreatePod implements Client.
func (c *restClient) CreatePod(ctx context.Context, spec PodSpec) (string, error) {
	body := createPodRequest{
		Name:          spec.Name,
		Image:         spec.Image,
		Cloud:         cloudTypeSecure,
		Disk:          spec.ContainerDiskGiB,
		Env:           spec.Env,
		Entrypoint:    spec.Entrypoint,
		Cmd:           spec.StartCmd,
		Ports:         spec.Ports,
		DataCenterIDs: spec.DataCenterIDs,
		Registry:      spec.RegistryAuthID,
	}
	if spec.GPUCount > 0 {
		body.GPU = &gpuRequest{
			ID:                 spec.GPUTypeID,
			Count:              spec.GPUCount,
			MinVCPUCountPerGPU: spec.VCPUPerGPU,
			MinRAMPerGPU:       spec.RAMPerGPUGiB,
		}
	} else {
		body.CPU = &cpuRequest{ID: cpuFlavor, VCPUCount: spec.VCPUCount}
	}

	var out podResponse
	if err := c.do(ctx, http.MethodPost, "/v2/pods", body, &out); err != nil {
		return "", classifyCreate(err)
	}
	if out.ID == "" {
		// A 2xx with no id is unusable and, worse, ambiguous: a Pod may exist that we can
		// never name to terminate. Reported as an error with no sentinel, so the Pod retries
		// (Provision is idempotent on the claim name, and the name lookup will find any Pod
		// this call did create).
		return "", fmt.Errorf("runpod: create pod %q: response carried no id", spec.Name)
	}
	return out.ID, nil
}

// TerminatePod implements Client. Idempotent: a 404 means the Pod is already gone, which is
// success for the caller (the NodeClaim finalizer retries against this).
func (c *restClient) TerminatePod(ctx context.Context, id string) error {
	err := c.do(ctx, http.MethodDelete, "/v2/pods/"+url.PathEscape(id), nil, nil)
	if err != nil && !notFound(err) {
		return err
	}
	return nil
}

// GetPod implements Client, returning (nil, nil) for a Pod that no longer exists.
func (c *restClient) GetPod(ctx context.Context, id string) (*Pod, error) {
	var out podResponse
	err := c.do(ctx, http.MethodGet, "/v2/pods/"+url.PathEscape(id), nil, &out)
	if notFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	pd := out.toPod()
	return &pd, nil
}

// ListPods implements Client. Filtering to Nebula's own Pods is the adapter's job, since it
// owns the naming scheme (see Provider.List).
//
// Every page is walked: a Pod missing from List reads as terminated, so stopping at the
// first page would report every Pod past it dead. One call below listPageSize Pods.
func (c *restClient) ListPods(ctx context.Context) ([]Pod, error) {
	var pods []Pod
	cursor := ""
	for {
		q := url.Values{"limit": {strconv.Itoa(listPageSize)}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var out struct {
			Pods       []podResponse `json:"pods"`
			Pagination struct {
				NextCursor  *string `json:"nextCursor"`
				HasNextPage bool    `json:"hasNextPage"`
			} `json:"pagination"`
		}
		if err := c.do(ctx, http.MethodGet, "/v2/pods?"+q.Encode(), nil, &out); err != nil {
			return nil, err
		}
		for _, r := range out.Pods {
			pods = append(pods, r.toPod())
		}
		next := out.Pagination.NextCursor
		if !out.Pagination.HasNextPage || next == nil || *next == "" || *next == cursor {
			return pods, nil
		}
		cursor = *next
	}
}

// registryAuthPath is the collection RunPod stores image-pull credentials in.
const registryAuthPath = "/v2/registries"

// registryAuthResponse is the subset of a registry credential this adapter reads.
// Notably NOT the password: RunPod does not return it, and nothing here needs it back.
type registryAuthResponse struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// EnsureRegistryAuth implements Client. RunPod's create takes a registry credential ID,
// never an inline username/password, so a credential has to become an OBJECT in RunPod's
// account before a Pod can use it.
//
// The object is CONTENT-ADDRESSED — its name is a hash of the credential (see
// registryAuthName) — which is what makes this safe to call on every Provision:
//
//   - Idempotent. The same credential resolves to the same name, so the list-then-create
//     finds the existing object instead of accumulating one object per Pod.
//   - Correct across rotation. A changed password hashes differently, so it becomes a new
//     object rather than silently reusing a stale one that would 401 at pull time.
//
// Objects are never DELETED, and that is deliberate: one object is shared by every Pod using
// that credential, so deleting it on any single teardown would break the others' next pull.
// The population is bounded by the number of distinct credentials, not by the number of Pods.
func (c *restClient) EnsureRegistryAuth(ctx context.Context, auth *provider.RegistryAuth) (string, error) {
	if auth == nil || auth.Basic == nil {
		// The adapter vets the kind before calling (checkRegistryAuth), so this is a
		// programming error rather than a user-facing one — but it must not become a silent
		// anonymous pull.
		return "", auth.Unsupported("runpod")
	}
	name := registryAuthName(auth.Basic.Username, auth.Basic.Password)

	var existing struct {
		Registries []registryAuthResponse `json:"registries"`
	}
	if err := c.do(ctx, http.MethodGet, registryAuthPath, nil, &existing); err != nil {
		return "", err
	}
	for _, e := range existing.Registries {
		if e.Name == name && e.ID != "" {
			return e.ID, nil
		}
	}

	body := struct {
		Name     string `json:"name"`
		Username string `json:"username"`
		Password string `json:"password"`
	}{Name: name, Username: auth.Basic.Username, Password: auth.Basic.Password}

	var created registryAuthResponse
	if err := c.do(ctx, http.MethodPost, registryAuthPath, body, &created); err != nil {
		// Worded as an image-pull failure, which blocklists nothing: a credential RunPod
		// would not store is a fact about this Pod's imagePullSecret, not about the
		// accelerator or region the Pod was headed for.
		return "", fmt.Errorf("runpod: store image pull credential %q: %w", name, err)
	}
	if created.ID == "" {
		return "", fmt.Errorf("runpod: store image pull credential %q: response carried no id", name)
	}
	return created.ID, nil
}

// registryAuthName is the content-addressed name of a stored credential: a fixed prefix
// (so Nebula's objects are recognizable in the RunPod console) plus a hash of the
// credential itself.
//
// The hash is what makes EnsureRegistryAuth idempotent, and it is over BOTH fields so a
// rotated password yields a new object. Hashed rather than named after the registry or the
// claim for two reasons: a RunPod object name is not a secret and appears in its UI, so the
// username must not be in it; and naming it after the claim would create one object per
// NodeClaim for a credential every claim shares.
//
// Truncated to 16 hex characters — 64 bits, which for a per-account population of at most a
// handful of credentials is far past any collision concern, and keeps the name readable.
func registryAuthName(username, password string) string {
	// The NUL separator keeps ("ab", "c") from hashing the same as ("a", "bc").
	sum := sha256.Sum256([]byte(username + "\x00" + password))
	return namePrefix + hex.EncodeToString(sum[:])[:16]
}
