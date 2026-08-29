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
	"strings"
	"time"

	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/provider/catalog"
)

// The RunPod REST API. There is no official Go SDK, so this is plain net/http — which is
// also why the whole surface is one small file: the adapter needs five operations.
const (
	// defaultBaseURL is RunPod's REST v1 root. Overridable only in tests (see newClient).
	defaultBaseURL = "https://rest.runpod.io/v1"
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
		// RunPod acted on the request, and provider.IsRejection reads an unwrapped
		// transport error as unattributable, which keeps the Pod retrying instead of being
		// failed under an instance whose id we never saw.
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

// errorMessage pulls the human-readable part out of an error response, trying RunPod's
// JSON envelope first and falling back to the raw text — some gateway errors are HTML, and
// an empty message would leave the classifier nothing to read.
func errorMessage(raw []byte) string {
	var envelope struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &envelope); err == nil {
		if envelope.Error != "" {
			return truncate(envelope.Error)
		}
		if envelope.Message != "" {
			return truncate(envelope.Message)
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
// docs/add-a-provider.md, "Wrap the errors your Provision returns"). Unwrapped, every one of
// these would land on nebula_provision_failures_total{reason="other"} and be retried
// forever against a provider that has already given its answer.
//
// interruptible is the request's tier, which the error itself never carries; a capacity
// failure on the spot tier also gets ErrSpotCapacity so ClassifyProvisionError can block
// Spot alone and leave OnDemand purchasable.
func classifyCreate(err error, interruptible bool) error {
	var ae *apiError
	if !errors.As(err, &ae) {
		return err // a transport/encode failure: unattributable, and already wrapped
	}
	msg := strings.ToLower(ae.message)

	switch {
	case ae.status == http.StatusUnauthorized, ae.status == http.StatusForbidden:
		// Whole-provider: nothing succeeds until the key is fixed.
		return fmt.Errorf("%w: %w", err, provider.ErrAuth)

	case ae.status >= http.StatusInternalServerError:
		// Left UNWRAPPED, deliberately. A 5xx says RunPod failed to answer, not that it
		// said no, and it may well have created the Pod before falling over. Wrapping a
		// sentinel here would fail the Pod and blocklist a candidate on the strength of a
		// server-side blip — and could reap a Pod out from under a paid instance.
		return err

	case ae.status == http.StatusTooManyRequests:
		return fmt.Errorf("%w: %w", err, provider.ErrQuota)

	// Money, not capacity, but scoped the same way: it is transient, it is not an
	// authentication problem, and ErrQuota is the sentinel for "a limit stopped this". It
	// does block more than the one candidate in practice, which the TTL bounds.
	case ae.status == http.StatusPaymentRequired,
		containsAny(msg, "insufficient funds", "insufficient balance", "not enough credit"):
		return fmt.Errorf("%w: %w", err, provider.ErrQuota)

	case containsAny(msg, "no longer any instances available", "no instances available",
		"no instance available", "out of capacity", "no capacity", "not available",
		"unavailable", "sold out"):
		if interruptible {
			return fmt.Errorf("%w: %w: %w", err, provider.ErrNoCapacity, ErrSpotCapacity)
		}
		return fmt.Errorf("%w: %w", err, provider.ErrNoCapacity)

	case containsAny(msg, "invalid gpu", "unknown gpu", "gpu type", "unsupported"):
		// A GPU id RunPod does not recognize: durable until runpod.csv is corrected, and
		// accelerator-scoped so the rest of the provider stays usable.
		return fmt.Errorf("%w: %w", err, provider.ErrUnsupportedAccelerator)

	case containsAny(msg, "registry", "image", "pull", "manifest"):
		// Belongs to the REQUEST, not the candidate, so ErrImagePull — which blocklists
		// NOTHING. Blocking here would exclude an accelerator that is serving every other
		// Pod fine, because one Pod named an image RunPod could not fetch.
		return fmt.Errorf("%w: %w", err, provider.ErrImagePull)

	default:
		// An unrecognized 4xx. Left unwrapped rather than guessed at: none of the shared
		// sentinels describes "RunPod rejected this and we do not know why", and every
		// available guess is worse than retrying — ErrAuth would fence off the whole
		// provider, a capacity wrap would evict a healthy candidate. A sustained
		// reason="other" rate on this provider's metrics is the signal that a condition
		// belongs in the table above.
		return err
	}
}

// containsAny reports whether s contains any of subs. A local copy because the shared one
// in package provider is unexported, and the alternative — exporting it — would widen that
// package's surface for one adapter's convenience.
func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// createPodRequest is RunPod's POST /pods body. Only the fields Nebula sets are present;
// everything omitted takes RunPod's own default, which is the point of the omitempty tags —
// a zero we did not mean would override a sane default with 0.
type createPodRequest struct {
	Name        string `json:"name"`
	ImageName   string `json:"imageName"`
	CloudType   string `json:"cloudType"`
	ComputeType string `json:"computeType"`

	GPUTypeIDs      []string `json:"gpuTypeIds,omitempty"`
	GPUCount        int32    `json:"gpuCount,omitempty"`
	GPUTypePriority string   `json:"gpuTypePriority,omitempty"`
	MinVCPUPerGPU   int      `json:"minVCPUPerGPU,omitempty"`
	MinRAMPerGPU    int      `json:"minRAMPerGPU,omitempty"`
	VCPUCount       int      `json:"vcpuCount,omitempty"`

	ContainerDiskInGb int `json:"containerDiskInGb,omitempty"`
	// VolumeInGb has NO omitempty: 0 is exactly the value we mean, and it must be sent to
	// override RunPod's default of a 20 GiB persistent volume. A Nebula instance is cattle
	// with nothing to persist, so that volume would be pure cost.
	VolumeInGb int `json:"volumeInGb"`

	Env              map[string]string `json:"env,omitempty"`
	DockerEntrypoint []string          `json:"dockerEntrypoint,omitempty"`
	DockerStartCmd   []string          `json:"dockerStartCmd,omitempty"`
	Ports            []string          `json:"ports,omitempty"`

	DataCenterIDs      []string `json:"dataCenterIds,omitempty"`
	DataCenterPriority string   `json:"dataCenterPriority,omitempty"`
	CountryCodes       []string `json:"countryCodes,omitempty"`

	// Interruptible has no omitempty either: false is the OnDemand tier, and being explicit
	// about the tier that costs money is worth four bytes.
	Interruptible   bool   `json:"interruptible"`
	SupportPublicIP bool   `json:"supportPublicIp,omitempty"`
	RegistryAuthID  string `json:"containerRegistryAuthId,omitempty"`
}

// podResponse is the subset of RunPod's Pod object this adapter reads. Fields it ignores
// (savings plans, template, network volume, cost) are omitted rather than carried, so the
// struct states exactly what the adapter's behaviour depends on.
type podResponse struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	DesiredStatus string         `json:"desiredStatus"`
	LastStartedAt string         `json:"lastStartedAt"`
	Interruptible bool           `json:"interruptible"`
	Ports         []string       `json:"ports"`
	PortMappings  map[string]int `json:"portMappings"`
	PublicIP      string         `json:"publicIp"`
	// Machine carries the data center, and is only populated when the request asks for it
	// (includeMachine=true) — hence the query on every read path. A pointer because RunPod
	// sends null when it is not included, and Region must then stay empty rather than
	// reporting a placement we did not observe.
	Machine *struct {
		DataCenterID string `json:"dataCenterId"`
	} `json:"machine"`
}

// toPod converts the wire shape into the adapter's view.
func (r podResponse) toPod() Pod {
	pd := Pod{
		ID:            r.ID,
		Name:          r.Name,
		DesiredStatus: r.DesiredStatus,
		LastStartedAt: r.LastStartedAt,
		Interruptible: r.Interruptible,
		Ports:         r.Ports,
		PublicIP:      r.PublicIP,
		PortMappings:  r.PortMappings,
	}
	if r.Machine != nil {
		pd.DataCenterID = r.Machine.DataCenterID
	}
	return pd
}

// CreatePod implements Client.
func (c *restClient) CreatePod(ctx context.Context, spec PodSpec) (string, error) {
	body := createPodRequest{
		Name:        spec.Name,
		ImageName:   spec.Image,
		CloudType:   cloudTypeSecure,
		ComputeType: "GPU",

		GPUTypeIDs:    spec.GPUTypeIDs,
		GPUCount:      spec.GPUCount,
		MinVCPUPerGPU: spec.VCPUPerGPU,
		MinRAMPerGPU:  spec.RAMPerGPUGiB,

		ContainerDiskInGb: spec.ContainerDiskGiB,
		VolumeInGb:        0,

		Env:              spec.Env,
		DockerEntrypoint: spec.Entrypoint,
		DockerStartCmd:   spec.StartCmd,
		Ports:            spec.Ports,

		DataCenterIDs: spec.DataCenterIDs,
		CountryCodes:  spec.CountryCodes,

		Interruptible: spec.Interruptible,
		// Ask for a public IP so a /tcp port can be reached directly. Harmless for the
		// /http-only Pods this adapter creates today, and it is what would make a raw-TCP
		// workload addressable without a second change here.
		SupportPublicIP: true,
		RegistryAuthID:  spec.RegistryAuthID,
	}
	if spec.GPUCount == 0 {
		// A CPU-only Pod is a different product on RunPod: the GPU sizing fields are
		// meaningless and vcpuCount replaces them.
		body.ComputeType = "CPU"
		body.VCPUCount = spec.VCPUCount
	} else if len(spec.GPUTypeIDs) > 1 {
		// Only meaningful with alternates: it tells RunPod to satisfy the list by whichever
		// id has capacity rather than insisting on the first. With one id there is nothing
		// to prioritize, and sending it would just be noise in the request.
		body.GPUTypePriority = "availability"
	}
	if len(spec.DataCenterIDs) > 0 {
		// custom, not availability: the pool NAMED these data centers, so honouring them is
		// the point. availability would let RunPod place elsewhere, silently breaking a
		// constraint an operator set for data residency.
		body.DataCenterPriority = "custom"
	}

	var out podResponse
	if err := c.do(ctx, http.MethodPost, "/pods", body, &out); err != nil {
		return "", classifyCreate(err, spec.Interruptible)
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
	err := c.do(ctx, http.MethodDelete, "/pods/"+url.PathEscape(id), nil, nil)
	if err != nil && !notFound(err) {
		return err
	}
	return nil
}

// GetPod implements Client, returning (nil, nil) for a Pod that no longer exists.
func (c *restClient) GetPod(ctx context.Context, id string) (*Pod, error) {
	var out podResponse
	err := c.do(ctx, http.MethodGet, "/pods/"+url.PathEscape(id)+"?includeMachine=true", nil, &out)
	if notFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	pd := out.toPod()
	return &pd, nil
}

// ListPods implements Client: one call for the whole account. Filtering to Nebula's own Pods
// is the adapter's job, since it owns the naming scheme (see Provider.List).
func (c *restClient) ListPods(ctx context.Context) ([]Pod, error) {
	var out []podResponse
	if err := c.do(ctx, http.MethodGet, "/pods?includeMachine=true", nil, &out); err != nil {
		return nil, err
	}
	pods := make([]Pod, 0, len(out))
	for _, r := range out {
		pods = append(pods, r.toPod())
	}
	return pods, nil
}

// registryAuthPath is the collection RunPod stores image-pull credentials in.
const registryAuthPath = "/containerregistryauth"

// registryAuthResponse is the subset of a containerRegistryAuth object this adapter reads.
// Notably NOT the password: RunPod does not return it, and nothing here needs it back.
type registryAuthResponse struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// EnsureRegistryAuth implements Client. RunPod's create takes a containerRegistryAuth ID,
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
		return "", fmt.Errorf("runpod: registry auth %s cannot be expressed as a RunPod credential: %w",
			auth, provider.ErrImagePull)
	}
	name := registryAuthName(auth.Basic.Username, auth.Basic.Password)

	var existing []registryAuthResponse
	if err := c.do(ctx, http.MethodGet, registryAuthPath, nil, &existing); err != nil {
		return "", err
	}
	for _, e := range existing {
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
		// Wrapped as an image-pull failure, which blocklists nothing: a credential RunPod
		// would not store is a fact about this Pod's imagePullSecret, not about the
		// accelerator or region the Pod was headed for.
		return "", fmt.Errorf("%w: %w", err, provider.ErrImagePull)
	}
	if created.ID == "" {
		return "", fmt.Errorf("runpod: create registry auth %q: response carried no id: %w",
			name, provider.ErrImagePull)
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
