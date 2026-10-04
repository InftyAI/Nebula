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
	"reflect"
	"strings"
	"sync"
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
	var (
		mu   sync.Mutex
		seen []recordedRequest
	)
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
		mu.Lock()
		seen = append(seen, rec)
		mu.Unlock()
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

// problem is a v2 error body (RFC 9457) carrying msg as its detail.
func problem(status int, msg string) string {
	return fmt.Sprintf(`{"title":%q,"status":%d,"detail":%q}`, http.StatusText(status), status, msg)
}

func TestClassifyCreate(t *testing.T) {
	// Every one of these must carry a sentinel or deliberately carry none: unwrapped, a
	// rejection lands on nebula_provision_failures_total{reason="other"}.
	cases := []struct {
		name    string
		status  int
		message string

		want error // the sentinel the error must wrap, nil for "none"
		// blocksNothing: the zero BlockScope, the contract for a REQUEST-scoped failure.
		blocksNothing bool
	}{{
		name: "401 is auth", status: 401, message: "invalid token", want: provider.ErrAuth,
	}, {
		// RunPod: "your account cannot access the requested pool — skip this candidate".
		// DenyAll would fence off every pool the account CAN use.
		name: "403 is scoped to the candidate", status: 403, message: "Forbidden",
		want: provider.ErrUnsupportedAccelerator,
	}, {
		// A 5xx says RunPod failed to ANSWER, not that it said no — and it may have created
		// the Pod before falling over.
		name: "500 stays unwrapped", status: 500, message: "internal error", want: nil,
	}, {
		name: "502 stays unwrapped", status: 502, message: "<html>bad gateway</html>", want: nil,
	}, {
		name: "429 is quota", status: 429, message: "Too Many Requests", want: provider.ErrQuota,
	}, {
		name: "402 is quota", status: 402, message: "Insufficient balance", want: provider.ErrQuota,
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
		name:    "an unknown gpu type is an accelerator problem",
		status:  400,
		message: "invalid gpu type id",
		want:    provider.ErrUnsupportedAccelerator,
	}, {
		// Belongs to the REQUEST, not the candidate: one Pod's bad credential must not exclude
		// an accelerator serving every other Pod — and a registry's "unauthorized" must not
		// read as OUR auth failing.
		name:          "a registry failure blocks nothing",
		status:        400,
		message:       "registry unauthorized: could not pull image",
		blocksNothing: true,
	}, {
		// The capacity and GPU phrases are generic enough to match these too.
		name: "an unavailable image blocks nothing", status: 400, message: "image not available",
		blocksNothing: true,
	}, {
		name: "an unavailable manifest blocks nothing", status: 400, message: "manifest unavailable",
		blocksNothing: true,
	}, {
		name: "an unsupported image blocks nothing", status: 400, message: "unsupported image format",
		blocksNothing: true,
	}, {
		// v2's 400 also covers cross-field rule violations, with no code to tell them from
		// capacity; guessing either way is worse than leaving it unattributable.
		name: "an unrecognized 400 stays unwrapped", status: 400, message: "something new", want: nil,
	}, {
		name: "a 422 stays unwrapped", status: 422, message: "gpu.count: must be >= 1", want: nil,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := testServer(t, jsonReply(tc.status, problem(tc.status, tc.message)))

			_, err := c.CreatePod(context.Background(), PodSpec{
				Name: "nebula-claim-a", Image: "img", GPUCount: 1, GPUTypeID: "NVIDIA H100 80GB HBM3",
			})
			if err == nil {
				t.Fatal("CreatePod succeeded against an error response")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("error %v does not wrap %v", err, tc.want)
			}
			if tc.want == nil {
				for _, s := range []error{
					provider.ErrAuth, provider.ErrQuota, provider.ErrNoCapacity,
					provider.ErrUnsupportedAccelerator,
				} {
					if errors.Is(err, s) {
						t.Errorf("error %v wraps %v; it must stay unattributable", err, s)
					}
				}
			}
			scope := provider.ClassifyError(err, "", "H100:1")
			if tc.blocksNothing && scope != (provider.BlockScope{}) {
				t.Errorf("scope = %+v, want the zero scope", scope)
			}
			// RunPod's own words survive into the message an operator reads off a Pod condition.
			if !strings.Contains(err.Error(), tc.message) {
				t.Errorf("error %q dropped RunPod's message %q", err, tc.message)
			}
		})
	}
}

func TestErrorMessage_KeepsValidationErrors(t *testing.T) {
	// A 422 carries its reason ONLY in errors[]; dropping it leaves "Unprocessable Entity".
	got := errorMessage([]byte(`{"title":"Unprocessable Entity","status":422,
		"detail":"validation failed","errors":["cpu.vcpuCount: must be a power of two"]}`))
	if want := "validation failed: cpu.vcpuCount: must be a power of two"; got != want {
		t.Errorf("errorMessage = %q, want %q", got, want)
	}
	if got := errorMessage([]byte("<html>bad gateway</html>")); got != "<html>bad gateway</html>" {
		t.Errorf("non-JSON body = %q, want it verbatim", got)
	}
}

func TestCreatePod_WireForm(t *testing.T) {
	c, seen := testServer(t, jsonReply(201, `{"id":"pod-1"}`))

	id, err := c.CreatePod(context.Background(), PodSpec{
		Name:          "nebula-claim-a",
		Image:         "myimg:latest",
		Entrypoint:    []string{"python"},
		StartCmd:      []string{"serve.py"},
		Env:           map[string]string{"K": "v"},
		GPUTypeID:     "NVIDIA H100 80GB HBM3",
		GPUCount:      2,
		VCPUPerGPU:    5,
		RAMPerGPUGiB:  50,
		DataCenterIDs: []string{"US-KS-2", "US-TX-3"},
	})
	if err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if id != "pod-1" {
		t.Fatalf("id = %q, want pod-1", id)
	}
	req := (*seen)[0]
	if req.method != http.MethodPost || req.path != "/v2/pods" {
		t.Errorf("%s %s, want POST /v2/pods", req.method, req.path)
	}
	if req.auth != "Bearer test-key" {
		t.Errorf("Authorization = %q", req.auth)
	}
	want := map[string]any{
		"name":  "nebula-claim-a",
		"image": "myimg:latest",
		// SECURE only: COMMUNITY prices the same GPU differently and the catalog has no
		// cloud-type axis to express that.
		"cloud":      cloudTypeSecure,
		"entrypoint": []any{"python"},
		"cmd":        []any{"serve.py"},
		"env":        map[string]any{"K": "v"},
		"gpu": map[string]any{
			"id": "NVIDIA H100 80GB HBM3", "count": float64(2),
			"minVcpuCountPerGpu": float64(5), "minRamPerGpu": float64(50),
		},
		"dataCenterIds": []any{"US-KS-2", "US-TX-3"},
	}
	if !reflect.DeepEqual(req.body, want) {
		t.Errorf("body =\n%v\nwant\n%v", req.body, want)
	}
}

func TestCreatePod_SuccessWithNoID(t *testing.T) {
	// A 2xx with no id is worse than an error: a Pod may exist that we can never name to
	// terminate. It must fail WITHOUT a sentinel so nothing is blocklisted — Provision is
	// idempotent on the claim name, and the name lookup will find whatever this call created.
	c, _ := testServer(t, jsonReply(201, `{}`))

	_, err := c.CreatePod(context.Background(), PodSpec{Name: "nebula-a", Image: "img", GPUCount: 1})
	if err == nil {
		t.Fatal("CreatePod accepted a response with no id")
	}
	if scope := provider.ClassifyError(err, "", "H100:1"); scope != (provider.BlockScope{}) {
		t.Errorf("scope = %+v; a missing id says nothing about the candidate", scope)
	}
}

func TestTerminatePod_404IsSuccess(t *testing.T) {
	// The NodeClaim finalizer retries Terminate, so an already-gone Pod has to be success —
	// otherwise the finalizer never clears and the Pod is stuck deleting forever.
	c, seen := testServer(t, jsonReply(404, problem(404, "pod not found")))
	if err := c.TerminatePod(context.Background(), "pod-gone"); err != nil {
		t.Fatalf("TerminatePod on a missing Pod = %v, want nil", err)
	}
	if req := (*seen)[0]; req.method != http.MethodDelete || req.path != "/v2/pods/pod-gone" {
		t.Errorf("%s %s, want DELETE /v2/pods/pod-gone", req.method, req.path)
	}

	// Any OTHER failure must still surface: swallowing a 500 would drop the teardown
	// obligation and leak a billing instance.
	c2, _ := testServer(t, jsonReply(500, problem(500, "boom")))
	if err := c2.TerminatePod(context.Background(), "pod-1"); err == nil {
		t.Error("TerminatePod swallowed a 500; the instance would leak")
	}
}

func TestGetPod_404IsGone(t *testing.T) {
	// Absent means terminated, per the interface contract.
	c, seen := testServer(t, jsonReply(404, problem(404, "not found")))
	pd, err := c.GetPod(context.Background(), "pod-gone")
	if err != nil || pd != nil {
		t.Fatalf("GetPod(missing) = %v, %v; want nil, nil", pd, err)
	}
	if req := (*seen)[0]; req.path != "/v2/pods/pod-gone" {
		t.Errorf("GET %s, want /v2/pods/pod-gone", req.path)
	}
}

func TestListPods_DecodesPlacementAndPorts(t *testing.T) {
	c, _ := testServer(t, jsonReply(200, `{"pods":[
		{"id":"pod-1","name":"nebula-claim-a","status":"RUNNING","dataCenterId":"EU-RO-1",
		 "ports":["8000/http","22/tcp"],
		 "runtime":{"ports":[{"private":22,"public":34446,"type":"tcp","ip":"195.26.233.3"},
		                     {"private":8000,"public":null,"type":"http","ip":null}]}},
		{"id":"pod-2","name":"nebula-claim-b","status":"PROVISIONING","dataCenterId":null,"runtime":null}
	],"pagination":{"nextCursor":null,"hasNextPage":false}}`))

	pods, err := c.ListPods(context.Background())
	if err != nil {
		t.Fatalf("ListPods: %v", err)
	}
	if len(pods) != 2 {
		t.Fatalf("got %d pods, want 2", len(pods))
	}
	if pods[0].DataCenterID != "EU-RO-1" || pods[0].Status != "RUNNING" {
		t.Errorf("pod-1 = %+v", pods[0])
	}
	if !reflect.DeepEqual(pods[0].Ports, []string{"8000/http", "22/tcp"}) {
		t.Errorf("pod-1 ports = %v", pods[0].Ports)
	}
	// A null data center must leave the region EMPTY rather than reporting a placement that
	// was never observed.
	if pods[1].DataCenterID != "" || pods[1].Ports != nil {
		t.Errorf("pod-2 = %+v, want no region and no ports", pods[1])
	}
}

func TestListPods_WalksEveryPage(t *testing.T) {
	// A Pod missing from List reads as terminated, so stopping at page one would report every
	// Pod past it dead.
	c, seen := testServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") == "" {
			jsonReply(200, `{"pods":[{"id":"pod-1","name":"nebula-a"}],
				"pagination":{"nextCursor":"c2","hasNextPage":true}}`)(w, r)
			return
		}
		jsonReply(200, `{"pods":[{"id":"pod-2","name":"nebula-b"}],
			"pagination":{"nextCursor":null,"hasNextPage":false}}`)(w, r)
	})

	pods, err := c.ListPods(context.Background())
	if err != nil {
		t.Fatalf("ListPods: %v", err)
	}
	if len(pods) != 2 || pods[1].ID != "pod-2" {
		t.Fatalf("pods = %+v, want both pages", pods)
	}
	if q := (*seen)[1].query; !strings.Contains(q, "cursor=c2") {
		t.Errorf("second page query = %q, want the cursor passed through", q)
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
			jsonReply(200, fmt.Sprintf(`{"registries":[{"id":"cra-existing","name":%q},
				{"id":"cra-other","name":"nebula-deadbeefdeadbeef"}]}`, name))(w, r)
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
				jsonReply(200, `{"registries":[]}`)(w, r)
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
		if !strings.HasPrefix(name, registryAuthPrefix) {
			t.Errorf("name %q lacks the %q prefix that makes Nebula's objects recognizable",
				name, registryAuthPrefix)
		}
	})

	t.Run("cached after the first call, evicted when a create using it fails", func(t *testing.T) {
		c, seen := testServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v2/pods" {
				jsonReply(400, problem(400, "registry not found"))(w, r)
				return
			}
			jsonReply(200, fmt.Sprintf(`{"registries":[{"id":"cra-existing","name":%q}]}`, name))(w, r)
		})
		ctx := context.Background()

		for range 2 {
			if _, err := c.EnsureRegistryAuth(ctx, auth); err != nil {
				t.Fatalf("EnsureRegistryAuth: %v", err)
			}
		}
		if len(*seen) != 1 {
			t.Fatalf("made %d calls for two resolves, want 1 (the second is cached)", len(*seen))
		}

		// A deleted credential leaves a stale cached id; the failed create must drop it so
		// the next Provision re-lists rather than failing every Pod until restart.
		if _, err := c.CreatePod(ctx, PodSpec{Name: "claim-a", RegistryAuthID: "cra-existing"}); err == nil {
			t.Fatal("CreatePod succeeded against an error response")
		}
		if _, err := c.EnsureRegistryAuth(ctx, auth); err != nil {
			t.Fatalf("EnsureRegistryAuth: %v", err)
		}
		if last := (*seen)[len(*seen)-1]; last.path != registryAuthPath {
			t.Errorf("last call %s %s, want a re-list after eviction", last.method, last.path)
		}
	})

	t.Run("a create failure blocklists nothing", func(t *testing.T) {
		// A credential RunPod will not store is a fact about this Pod's imagePullSecret, not
		// about the accelerator or region it was headed for — so the zero BlockScope.
		c, _ := testServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				jsonReply(200, `{"registries":[]}`)(w, r)
				return
			}
			jsonReply(400, problem(400, "invalid credential"))(w, r)
		})

		_, err := c.EnsureRegistryAuth(context.Background(), auth)
		if err == nil {
			t.Fatal("EnsureRegistryAuth succeeded against an error response")
		}
		if scope := provider.ClassifyError(err, "", "H100:1"); scope != (provider.BlockScope{}) {
			t.Errorf("scope = %+v, want the zero scope", scope)
		}
	})

	t.Run("a kind RunPod cannot express is refused, not silently dropped", func(t *testing.T) {
		// The adapter vets the kind first, so this is a programming error — but it must never
		// become a silent ANONYMOUS pull, which either 401s opaquely or succeeds against a
		// PUBLIC image of the same name.
		c, seen := testServer(t, jsonReply(200, `{"registries":[]}`))
		_, err := c.EnsureRegistryAuth(context.Background(), &provider.RegistryAuth{
			Registry: "1234.dkr.ecr.us-east-1.amazonaws.com",
			AWSRole:  &provider.AWSRoleAuth{RoleARN: "arn:aws:iam::1234:role/pull", Region: "us-east-1"},
		})
		if scope := provider.ClassifyError(err, "", "H100:1"); err == nil || scope != (provider.BlockScope{}) {
			t.Errorf("error = %v, scope = %+v; want a refusal that blocks nothing", err, scope)
		}
		if len(*seen) != 0 {
			t.Errorf("made %d API calls for a credential it cannot express", len(*seen))
		}
	})
}

// TestEnsureRegistryAuthRace pins that a name clash on RunPod's unique names never fails a Pod.
func TestEnsureRegistryAuthRace(t *testing.T) {
	auth := &provider.RegistryAuth{
		Registry: "ghcr.io",
		Basic:    &provider.BasicAuth{Username: "u", Password: "p4ssw0rd"},
	}
	name := registryAuthName("u", "p4ssw0rd")

	t.Run("concurrent first calls create once", func(t *testing.T) {
		var mu sync.Mutex
		var created bool
		posts := 0
		c, _ := testServer(t, func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			if r.Method == http.MethodPost {
				posts++
				created = true
				jsonReply(201, `{"id":"cra-new","name":"whatever"}`)(w, r)
				return
			}
			if created {
				jsonReply(200, fmt.Sprintf(`{"registries":[{"id":"cra-new","name":%q}]}`, name))(w, r)
				return
			}
			jsonReply(200, `{"registries":[]}`)(w, r)
		})

		const n = 8
		ids := make([]string, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				id, err := c.EnsureRegistryAuth(context.Background(), auth)
				if err != nil {
					t.Errorf("EnsureRegistryAuth: %v", err)
				}
				ids[i] = id
			})
		}
		wg.Wait()
		if posts != 1 {
			t.Errorf("issued %d creates, want 1", posts)
		}
		for i, id := range ids {
			if id != "cra-new" {
				t.Errorf("call %d got id %q, want cra-new", i, id)
			}
		}
	})

	t.Run("a create lost to another process resolves by re-listing", func(t *testing.T) {
		lists := 0
		c, _ := testServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				jsonReply(400, problem(400, "name already exists"))(w, r)
				return
			}
			lists++
			if lists == 1 {
				jsonReply(200, `{"registries":[]}`)(w, r)
				return
			}
			jsonReply(200, fmt.Sprintf(`{"registries":[{"id":"cra-theirs","name":%q}]}`, name))(w, r)
		})

		id, err := c.EnsureRegistryAuth(context.Background(), auth)
		if err != nil {
			t.Fatalf("EnsureRegistryAuth: %v", err)
		}
		if id != "cra-theirs" {
			t.Errorf("id = %q, want cra-theirs", id)
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
