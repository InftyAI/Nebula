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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/InftyAI/Nebula/pkg/provider"
)

// recordedRequest is what the fake RunPod saw, so a test can assert on the wire form the
// adapter produced rather than only on what it got back.
type recordedRequest struct {
	method string
	path   string
	query  string
	auth   string
	body   map[string]any
}

// testServer stands in for RunPod's REST API. handler answers each call; every request is
// recorded first. Returns the client under test and a pointer to the log.
func testServer(t *testing.T, handler http.HandlerFunc) (*restClient, *[]recordedRequest) {
	t.Helper()
	var seen []recordedRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := recordedRequest{
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.RawQuery,
			auth:   r.Header.Get("Authorization"),
		}
		if raw, err := io.ReadAll(r.Body); err == nil && len(raw) > 0 {
			_ = json.Unmarshal(raw, &rec.body)
		}
		seen = append(seen, rec)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	return newClient(srv.URL, "test-key"), &seen
}

// jsonReply writes one canned response.
func jsonReply(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

func TestClassifyCreate(t *testing.T) {
	// Every one of these must carry a sentinel or deliberately carry none: unwrapped, a
	// rejection lands on nebula_provision_failures_total{reason="other"} and is retried
	// forever against a provider that has already given its answer.
	cases := []struct {
		name string
		// status and message are what RunPod answered; spot is the request's tier, which the
		// error itself never carries.
		status  int
		message string
		spot    bool

		want     error // the sentinel the error must wrap, nil for "none"
		wantSpot bool  // must also carry the spot-tier marker
	}{{
		name: "401 is auth", status: 401, message: "Unauthorized", want: provider.ErrAuth,
	}, {
		name: "403 is auth", status: 403, message: "Forbidden", want: provider.ErrAuth,
	}, {
		// A 5xx says RunPod failed to ANSWER, not that it said no — and it may have created
		// the Pod before falling over. A sentinel here would fail the Pod and blocklist a
		// candidate on a server-side blip, possibly reaping a paid instance.
		name: "500 stays unwrapped", status: 500, message: "internal error", want: nil,
	}, {
		name: "502 stays unwrapped", status: 502, message: "<html>bad gateway</html>", want: nil,
	}, {
		name: "429 is quota", status: 429, message: "Too Many Requests", want: provider.ErrQuota,
	}, {
		// Money, not capacity, but scoped the same way: transient, and not an auth problem.
		name: "402 is quota", status: 402, message: "Payment Required", want: provider.ErrQuota,
	}, {
		name:    "insufficient funds is quota",
		status:  400,
		message: "Insufficient funds to start this pod",
		want:    provider.ErrQuota,
	}, {
		name:    "no instances available is capacity",
		status:  400,
		message: "There are no longer any instances available with the requested specifications",
		want:    provider.ErrNoCapacity,
	}, {
		// The tier marker is what keeps a Spot shortage from disabling OnDemand capacity
		// that is still purchasable at the higher price.
		name:     "a spot shortage also marks the tier",
		status:   400,
		message:  "no instances available",
		spot:     true,
		want:     provider.ErrNoCapacity,
		wantSpot: true,
	}, {
		name:    "an unknown gpu type is an accelerator problem",
		status:  400,
		message: "invalid gpu type id",
		want:    provider.ErrUnsupportedAccelerator,
	}, {
		// Belongs to the REQUEST, not the candidate: ErrImagePull blocklists nothing, so one
		// Pod's bad credential cannot exclude an accelerator serving every other Pod.
		name:    "a registry failure is an image-pull problem",
		status:  400,
		message: "could not authenticate with registry",
		want:    provider.ErrImagePull,
	}, {
		// None of the shared sentinels describes "rejected, reason unknown", and every guess
		// is worse than retrying: ErrAuth would fence off the provider, a capacity wrap would
		// evict a healthy candidate. A sustained reason="other" rate is the signal to add a
		// row to the table.
		name: "an unrecognized 4xx stays unwrapped", status: 400, message: "something new", want: nil,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := testServer(t, jsonReply(tc.status, fmt.Sprintf(`{"error":%q}`, tc.message)))

			_, err := c.CreatePod(context.Background(), PodSpec{
				Name: "nebula-claim-a", Image: "img", GPUCount: 1,
				GPUTypeIDs: []string{"NVIDIA H100 80GB HBM3"}, Interruptible: tc.spot,
			})
			if err == nil {
				t.Fatal("CreatePod succeeded against an error response")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("error %v does not wrap %v", err, tc.want)
			}
			if tc.want == nil {
				// Assert it wraps NONE of them: the point of leaving it unwrapped is that
				// provider.IsRejection reads it as unattributable.
				for _, s := range []error{
					provider.ErrAuth, provider.ErrQuota, provider.ErrNoCapacity,
					provider.ErrUnsupportedAccelerator, provider.ErrImagePull,
				} {
					if errors.Is(err, s) {
						t.Errorf("error %v wraps %v; it must stay unattributable", err, s)
					}
				}
			}
			if got := errors.Is(err, ErrSpotCapacity); got != tc.wantSpot {
				t.Errorf("errors.Is(err, ErrSpotCapacity) = %t, want %t", got, tc.wantSpot)
			}
			// The status and RunPod's own words survive into the message, which is what an
			// operator reads off a Pod condition.
			if !strings.Contains(err.Error(), tc.message) {
				t.Errorf("error %q dropped RunPod's message %q", err, tc.message)
			}
		})
	}
}

func TestCreatePod_WireForm(t *testing.T) {
	c, seen := testServer(t, jsonReply(200, `{"id":"pod-1"}`))

	id, err := c.CreatePod(context.Background(), PodSpec{
		Name:          "nebula-claim-a",
		Image:         "myimg:latest",
		GPUTypeIDs:    []string{"NVIDIA H100 80GB HBM3", "NVIDIA H100 NVL"},
		GPUCount:      2,
		VCPUPerGPU:    5,
		RAMPerGPUGiB:  50,
		DataCenterIDs: []string{"US-KS-2"},
		Interruptible: true,
	})
	if err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if id != "pod-1" {
		t.Fatalf("id = %q, want pod-1", id)
	}
	req := (*seen)[0]
	if req.method != http.MethodPost || req.path != "/pods" {
		t.Errorf("%s %s, want POST /pods", req.method, req.path)
	}
	if req.auth != "Bearer test-key" {
		t.Errorf("Authorization = %q", req.auth)
	}
	// volumeInGb must be PRESENT and 0: RunPod's default is a billable 20 GiB persistent
	// volume, and a Nebula instance is cattle with nothing to persist. omitempty here would
	// silently restore that default — which is why the field carries no omitempty tag.
	v, ok := req.body["volumeInGb"]
	if !ok {
		t.Error("volumeInGb absent; RunPod would then attach its default 20 GiB billable volume")
	} else if v != float64(0) {
		t.Errorf("volumeInGb = %v, want 0", v)
	}
	// Likewise interruptible: false is the tier that costs money, so it is stated rather
	// than left to a default.
	if got, ok := req.body["interruptible"]; !ok || got != true {
		t.Errorf("interruptible = %v (present=%t), want true", got, ok)
	}
	// SECURE only: COMMUNITY prices the same GPU differently and the catalog has no
	// cloud-type axis to express that, so offering it would make the price a guess.
	if req.body["cloudType"] != cloudTypeSecure {
		t.Errorf("cloudType = %v, want %q", req.body["cloudType"], cloudTypeSecure)
	}
	if req.body["computeType"] != "GPU" {
		t.Errorf("computeType = %v, want GPU", req.body["computeType"])
	}
	// Only meaningful WITH alternates: it tells RunPod to satisfy the list by whichever id
	// has capacity rather than insisting on the first.
	if req.body["gpuTypePriority"] != "availability" {
		t.Errorf("gpuTypePriority = %v, want availability for a multi-id request", req.body["gpuTypePriority"])
	}
	// custom, not availability: the pool NAMED this data center, so letting RunPod place
	// elsewhere would silently break a constraint an operator set for data residency.
	if req.body["dataCenterPriority"] != "custom" {
		t.Errorf("dataCenterPriority = %v, want custom", req.body["dataCenterPriority"])
	}
	if req.body["minVCPUPerGPU"] != float64(5) || req.body["minRAMPerGPU"] != float64(50) {
		t.Errorf("per-GPU sizing = %v/%v, want 5/50",
			req.body["minVCPUPerGPU"], req.body["minRAMPerGPU"])
	}
}

func TestCreatePod_SingleGPUIDAndCPUOnly(t *testing.T) {
	t.Run("one id sends no priority", func(t *testing.T) {
		// With a single id there is nothing to prioritize, so the field would be noise.
		c, seen := testServer(t, jsonReply(200, `{"id":"pod-1"}`))
		if _, err := c.CreatePod(context.Background(), PodSpec{
			Name: "nebula-a", Image: "img", GPUCount: 1, GPUTypeIDs: []string{"NVIDIA L4"},
		}); err != nil {
			t.Fatalf("CreatePod: %v", err)
		}
		if _, ok := (*seen)[0].body["gpuTypePriority"]; ok {
			t.Error("gpuTypePriority sent for a single-id request")
		}
	})

	t.Run("no GPU is a different product", func(t *testing.T) {
		// A CPU-only Pod's per-GPU sizing fields are meaningless; vcpuCount replaces them.
		c, seen := testServer(t, jsonReply(200, `{"id":"pod-cpu"}`))
		if _, err := c.CreatePod(context.Background(), PodSpec{
			Name: "nebula-c", Image: "img", VCPUCount: 3,
		}); err != nil {
			t.Fatalf("CreatePod: %v", err)
		}
		body := (*seen)[0].body
		if body["computeType"] != "CPU" {
			t.Errorf("computeType = %v, want CPU", body["computeType"])
		}
		if body["vcpuCount"] != float64(3) {
			t.Errorf("vcpuCount = %v, want 3", body["vcpuCount"])
		}
		if _, ok := body["gpuCount"]; ok {
			t.Error("gpuCount sent on a CPU-only Pod")
		}
	})
}

func TestCreatePod_SuccessWithNoID(t *testing.T) {
	// A 2xx with no id is worse than an error: a Pod may exist that we can never name to
	// terminate. It must fail WITHOUT a sentinel so the Pod retries — Provision is
	// idempotent on the claim name, and the name lookup will find whatever this call created.
	c, _ := testServer(t, jsonReply(201, `{}`))

	_, err := c.CreatePod(context.Background(), PodSpec{Name: "nebula-a", Image: "img", GPUCount: 1})
	if err == nil {
		t.Fatal("CreatePod accepted a response with no id")
	}
	if provider.IsRejection(err) {
		t.Errorf("error %v reads as a rejection; it must stay retryable", err)
	}
}

func TestTerminatePod_404IsSuccess(t *testing.T) {
	// The NodeClaim finalizer retries Terminate, so an already-gone Pod has to be success —
	// otherwise the finalizer never clears and the Pod is stuck deleting forever.
	c, seen := testServer(t, jsonReply(404, `{"error":"pod not found"}`))
	if err := c.TerminatePod(context.Background(), "pod-gone"); err != nil {
		t.Fatalf("TerminatePod on a missing Pod = %v, want nil", err)
	}
	if req := (*seen)[0]; req.method != http.MethodDelete || req.path != "/pods/pod-gone" {
		t.Errorf("%s %s, want DELETE /pods/pod-gone", req.method, req.path)
	}

	// Any OTHER failure must still surface: swallowing a 500 would drop the teardown
	// obligation and leak a billing instance.
	c2, _ := testServer(t, jsonReply(500, `{"error":"boom"}`))
	if err := c2.TerminatePod(context.Background(), "pod-1"); err == nil {
		t.Error("TerminatePod swallowed a 500; the instance would leak")
	}
}

func TestGetPod_404IsGone(t *testing.T) {
	// Absent means terminated, per the interface contract.
	c, seen := testServer(t, jsonReply(404, `{"error":"not found"}`))
	pd, err := c.GetPod(context.Background(), "pod-gone")
	if err != nil || pd != nil {
		t.Fatalf("GetPod(missing) = %v, %v; want nil, nil", pd, err)
	}
	// includeMachine is what populates the data center, so every read path must ask for it —
	// without it Region would silently stay empty on every instance.
	if req := (*seen)[0]; !strings.Contains(req.query, "includeMachine=true") {
		t.Errorf("GetPod query = %q, want includeMachine=true", req.query)
	}
}

func TestListPods_DecodesMachineAndPorts(t *testing.T) {
	c, seen := testServer(t, jsonReply(200, `[
		{"id":"pod-1","name":"nebula-claim-a","desiredStatus":"RUNNING",
		 "lastStartedAt":"2026-08-29T00:00:00Z","interruptible":true,
		 "ports":["8000/http"],"machine":{"dataCenterId":"EU-RO-1"}},
		{"id":"pod-2","name":"nebula-claim-b","desiredStatus":"EXITED","machine":null}
	]`))

	pods, err := c.ListPods(context.Background())
	if err != nil {
		t.Fatalf("ListPods: %v", err)
	}
	// The list path needs includeMachine for the same reason Get does; it is the poll loop's
	// only source of an instance's region.
	if req := (*seen)[0]; !strings.Contains(req.query, "includeMachine=true") {
		t.Errorf("ListPods query = %q, want includeMachine=true", req.query)
	}
	if len(pods) != 2 {
		t.Fatalf("got %d pods, want 2", len(pods))
	}
	if pods[0].DataCenterID != "EU-RO-1" || !pods[0].Interruptible {
		t.Errorf("pod-1 = %+v", pods[0])
	}
	if len(pods[0].Ports) != 1 || pods[0].Ports[0] != "8000/http" {
		t.Errorf("pod-1 ports = %v; they are what makes an /http-only Pod addressable", pods[0].Ports)
	}
	// A null machine must leave the region EMPTY rather than reporting a placement that was
	// never observed.
	if pods[1].DataCenterID != "" {
		t.Errorf("pod-2 region = %q, want empty for a null machine", pods[1].DataCenterID)
	}
}

func TestEnsureRegistryAuth(t *testing.T) {
	auth := &provider.RegistryAuth{
		Registry: "ghcr.io",
		Basic:    &provider.BasicAuth{Username: "u", Password: "p4ssw0rd"},
	}
	// Content-addressed: the SAME credential always resolves to the same object name, which
	// is what makes calling this on every Provision safe.
	name := registryAuthName("u", "p4ssw0rd")

	t.Run("reuses an existing object", func(t *testing.T) {
		// One object is shared by every Pod using that credential. Creating a second per Pod
		// would accumulate objects without bound, and RunPod never garbage-collects them.
		c, seen := testServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				t.Error("POST issued although a matching object already exists")
			}
			jsonReply(200, fmt.Sprintf(`[{"id":"cra-existing","name":%q},
				{"id":"cra-other","name":"nebula-deadbeefdeadbeef"}]`, name))(w, r)
		})

		id, err := c.EnsureRegistryAuth(context.Background(), auth)
		if err != nil {
			t.Fatalf("EnsureRegistryAuth: %v", err)
		}
		if id != "cra-existing" {
			t.Errorf("id = %q, want cra-existing", id)
		}
		if len(*seen) != 1 {
			t.Errorf("made %d calls, want 1 (the list)", len(*seen))
		}
	})

	t.Run("creates when absent, and never sends the password back on the list", func(t *testing.T) {
		c, seen := testServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				jsonReply(200, `[]`)(w, r)
				return
			}
			jsonReply(201, `{"id":"cra-new","name":"whatever"}`)(w, r)
		})

		id, err := c.EnsureRegistryAuth(context.Background(), auth)
		if err != nil {
			t.Fatalf("EnsureRegistryAuth: %v", err)
		}
		if id != "cra-new" {
			t.Errorf("id = %q, want cra-new", id)
		}
		if len(*seen) != 2 {
			t.Fatalf("made %d calls, want 2 (list then create)", len(*seen))
		}
		post := (*seen)[1]
		if post.method != http.MethodPost || post.path != registryAuthPath {
			t.Errorf("%s %s, want POST %s", post.method, post.path, registryAuthPath)
		}
		// The object NAME must be the hash, not the username or the claim: a RunPod object
		// name is not secret and shows in its UI, and naming it after the claim would create
		// one object per NodeClaim for a credential every claim shares.
		if post.body["name"] != name {
			t.Errorf("name = %v, want the content-addressed %q", post.body["name"], name)
		}
		if post.body["username"] != "u" || post.body["password"] != "p4ssw0rd" {
			t.Errorf("credential did not reach the create body: %v", post.body["username"])
		}
	})

	t.Run("a rotated password becomes a new object", func(t *testing.T) {
		// The hash covers BOTH fields, so a rotation cannot silently reuse a stale object
		// that would 401 at pull time.
		if registryAuthName("u", "old") == registryAuthName("u", "new") {
			t.Error("a rotated password hashes to the same object name")
		}
		// And the NUL separator keeps ("ab","c") from colliding with ("a","bc").
		if registryAuthName("ab", "c") == registryAuthName("a", "bc") {
			t.Error("username/password boundary is not separated in the hash")
		}
		if !strings.HasPrefix(name, namePrefix) {
			t.Errorf("name %q lacks the %q prefix that makes Nebula's objects recognizable",
				name, namePrefix)
		}
	})

	t.Run("a create failure blocklists nothing", func(t *testing.T) {
		// A credential RunPod will not store is a fact about this Pod's imagePullSecret, not
		// about the accelerator or region it was headed for — so ErrImagePull, which the
		// classifier maps to the zero BlockScope.
		c, _ := testServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				jsonReply(200, `[]`)(w, r)
				return
			}
			jsonReply(400, `{"error":"invalid credential"}`)(w, r)
		})

		_, err := c.EnsureRegistryAuth(context.Background(), auth)
		if !errors.Is(err, provider.ErrImagePull) {
			t.Errorf("error = %v, want it to wrap ErrImagePull", err)
		}
	})

	t.Run("a kind RunPod cannot express is refused, not silently dropped", func(t *testing.T) {
		// The adapter vets the kind first, so this is a programming error — but it must never
		// become a silent ANONYMOUS pull, which either 401s opaquely or succeeds against a
		// PUBLIC image of the same name.
		c, seen := testServer(t, jsonReply(200, `[]`))
		_, err := c.EnsureRegistryAuth(context.Background(), &provider.RegistryAuth{
			Registry: "1234.dkr.ecr.us-east-1.amazonaws.com",
			AWSRole:  &provider.AWSRoleAuth{RoleARN: "arn:aws:iam::1234:role/pull", Region: "us-east-1"},
		})
		if !errors.Is(err, provider.ErrImagePull) {
			t.Errorf("error = %v, want it to wrap ErrImagePull", err)
		}
		if len(*seen) != 0 {
			t.Errorf("made %d API calls for a credential it cannot express", len(*seen))
		}
	})
}

func TestNewSDKClient_MissingKeyIsSkippable(t *testing.T) {
	// An absent key must be an ERROR rather than a client that fails on first use, so
	// registerProviders can log and skip RunPod the way it skips Modal and AWS — an operator
	// who configured only Modal must not get a fatal boot.
	t.Setenv(apiKeyEnv, "")
	if _, err := NewSDKClient(context.Background()); err == nil {
		t.Fatal("NewSDKClient succeeded with no API key; registration would silently register a dead provider")
	}
}
