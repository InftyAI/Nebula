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

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPolicyFor(t *testing.T) {
	cases := []struct {
		kind      keyKind
		ok        bool
		reusable  bool
		ephemeral bool
	}{
		// Daemon: single-use (NOT reusable) + ephemeral — a throwaway per-workload
		// credential that auto-reaps on disconnect.
		{kindDaemon, true, false, true},
		// Controller: reusable (survives restarts) + ephemeral (reaped on teardown).
		{kindController, true, true, true},
		{keyKind("bogus"), false, false, false},
		{keyKind(""), false, false, false},
	}
	for _, tc := range cases {
		t.Run(string(tc.kind), func(t *testing.T) {
			p, ok := policyFor(tc.kind)
			if ok != tc.ok {
				t.Fatalf("policyFor(%q) ok = %v, want %v", tc.kind, ok, tc.ok)
			}
			if !ok {
				return
			}
			if p.reusable != tc.reusable {
				t.Errorf("reusable = %v, want %v", p.reusable, tc.reusable)
			}
			if p.ephemeral != tc.ephemeral {
				t.Errorf("ephemeral = %v, want %v", p.ephemeral, tc.ephemeral)
			}
			if p.expiration == "" {
				t.Errorf("expiration must not be empty")
			}
		})
	}
}

// fakeRunner records the policy it was asked to mint and returns a canned key, so
// the handler can be tested without a real headscale.
func fakeRunner(key string, err error) (commandRunner, *keyPolicy) {
	var seen keyPolicy
	run := func(_ config, policy keyPolicy) (string, error) {
		seen = policy
		return key, err
	}
	return run, &seen
}

func TestKeysHandler_Daemon(t *testing.T) {
	run, seen := fakeRunner("tskey-daemon-abc", nil)
	h := keysHandler(config{user: "nebula"}, run)

	req := httptest.NewRequest(http.MethodPost, "/keys?kind=daemon", nil)
	rec := httptest.NewRecorder()
	h(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp keyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp.Key != "tskey-daemon-abc" {
		t.Errorf("key = %q, want tskey-daemon-abc", resp.Key)
	}
	if resp.Reusable {
		t.Errorf("daemon key must not be reusable")
	}
	if !resp.Ephemeral {
		t.Errorf("daemon key must be ephemeral")
	}
	// The runner must have been asked for the single-use ephemeral policy.
	if seen.reusable || !seen.ephemeral {
		t.Errorf("runner policy = %+v, want single-use ephemeral", *seen)
	}
}

func TestKeysHandler_Controller(t *testing.T) {
	run, seen := fakeRunner("tskey-ctrl-xyz", nil)
	h := keysHandler(config{user: "nebula"}, run)

	req := httptest.NewRequest(http.MethodPost, "/keys?kind=controller", nil)
	rec := httptest.NewRecorder()
	h(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !seen.reusable || !seen.ephemeral {
		t.Errorf("controller policy = %+v, want reusable ephemeral", *seen)
	}
}

func TestKeysHandler_UnknownKind(t *testing.T) {
	run, _ := fakeRunner("unused", nil)
	h := keysHandler(config{user: "nebula"}, run)

	for _, q := range []string{"/keys?kind=bogus", "/keys"} {
		req := httptest.NewRequest(http.MethodPost, q, nil)
		rec := httptest.NewRecorder()
		h(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", q, rec.Code)
		}
	}
}

func TestKeysHandler_MethodNotAllowed(t *testing.T) {
	run, _ := fakeRunner("unused", nil)
	h := keysHandler(config{user: "nebula"}, run)

	req := httptest.NewRequest(http.MethodGet, "/keys?kind=daemon", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

func TestEnsureHeadscaleUserWithRetry_SucceedsFirstTry(t *testing.T) {
	calls := 0
	err := ensureHeadscaleUserWithRetry(config{user: "nebula"}, func(config) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("ensure called %d times, want 1", calls)
	}
}

func TestEnsureHeadscaleUserWithRetry_RecoversAfterSocketRace(t *testing.T) {
	// Simulate headscale's socket not being ready on the first few attempts, then
	// coming up — the broker must keep trying rather than fatal on the first miss.
	calls := 0
	err := ensureHeadscaleUserWithRetry(config{user: "nebula"}, func(config) error {
		calls++
		if calls < 3 {
			return fmt.Errorf("socket not ready")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil after recovery", err)
	}
	if calls != 3 {
		t.Errorf("ensure called %d times, want 3", calls)
	}
}

func TestEnsureHeadscaleUserWithRetry_ExhaustsAndReturnsLastError(t *testing.T) {
	// Shrink the delay so exhausting every attempt doesn't wait the real ~18s.
	orig := ensureUserRetryDelay
	ensureUserRetryDelay = 0
	defer func() { ensureUserRetryDelay = orig }()

	calls := 0
	err := ensureHeadscaleUserWithRetry(config{user: "nebula"}, func(config) error {
		calls++
		return fmt.Errorf("still down")
	})
	if err == nil {
		t.Fatal("err = nil, want the last failure")
	}
	if !strings.Contains(err.Error(), "still down") {
		t.Errorf("err = %v, want it to carry the last failure", err)
	}
	if calls != ensureUserRetries {
		t.Errorf("ensure called %d times, want %d", calls, ensureUserRetries)
	}
}

func TestKeysHandler_MintError(t *testing.T) {
	run, _ := fakeRunner("", fmt.Errorf("headscale down"))
	h := keysHandler(config{user: "nebula"}, run)

	req := httptest.NewRequest(http.MethodPost, "/keys?kind=daemon", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", rec.Code)
	}
	// The error body must NOT leak internal detail beyond a generic message.
	if strings.Contains(rec.Body.String(), "headscale down") {
		t.Errorf("response body leaked internal error: %s", rec.Body.String())
	}
}
