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

package vnode

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// testEndpoint stands in for the operator-supplied SANDD_EXTERNAL_HOST, already
// normalized by cmd's sanddEndpoint. It is an EXTERNAL address on purpose: that is
// the whole point of the field (see SandD.Endpoint), and a test using an in-cluster
// .svc name here would quietly bless the one address shape that can never be dialed.
const testEndpoint = "wss://sandd.example.com/ws"

// stubMinter records what the handler asked it to sign and returns a canned token
// or error. It stands in for pkg/sandd.Signer so these tests assert the WIRING
// (who is named in the token, where it is delivered) without pulling in key
// loading — the signing itself is covered by pkg/sandd's own tests.
type stubMinter struct {
	token  string
	err    error
	calls  int
	daemon string
	ctrl   string
	tenant string
	now    time.Time
}

func (s *stubMinter) MintDaemonToken(daemonID, controllerID, tenant string, now time.Time) (string, error) {
	s.calls++
	s.daemon, s.ctrl, s.tenant, s.now = daemonID, controllerID, tenant, now
	return s.token, s.err
}

// sanddCfg is the feature switched ON: an endpoint plus a minter, the pair that
// travels together (see the SandD type doc — either half alone is useless).
func sanddCfg(m SandDMinter) *SandD {
	return &SandD{Endpoint: testEndpoint, Minter: m}
}

// TestCreatePod_MintsCredentialsIntoProvisionRequest is the core of the wiring:
// the credentials must reach the provider through the ProvisionRequest, bound to
// THIS instance (sub) and addressed at the one controller (aud + endpoint).
func TestCreatePod_MintsCredentialsIntoProvisionRequest(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	m := &stubMinter{token: "signed.jwt.value"}
	h := NewHandler(fp, nil, nil, sanddCfg(m))

	if err := h.CreatePod(context.Background(), testPod("team-ml", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	if m.calls != 1 {
		t.Fatalf("expected exactly 1 mint per Provision, got %d", m.calls)
	}
	// sub is the claim name — the same identity the provider tags the instance with and
	// the NodeClaim carries. The controller registers the daemon under this verified
	// value and ignores the id the daemon claims, so `sub` is the ONLY thing deciding
	// which registry entry a connection becomes: get it wrong and exec addresses the
	// wrong instance, or none. A security assertion, not a formatting one.
	if m.daemon != "team-ml-p1" {
		t.Errorf("expected sub=claim name team-ml-p1, got %q", m.daemon)
	}
	// aud is the cluster-wide constant, NOT derived from the workload: there is one
	// controller, so a workload-scoped audience could not refuse a token that
	// controller is itself the audience for.
	if m.ctrl != nebulav1alpha1.SanddControllerAudience {
		t.Errorf("expected aud=%q, got %q", nebulav1alpha1.SanddControllerAudience, m.ctrl)
	}
	if m.tenant != "" {
		t.Errorf("expected empty tenant (single-tenant today), got %q", m.tenant)
	}

	if fp.lastReq.SanddAuth.Token != "signed.jwt.value" {
		t.Errorf("minted token did not reach the ProvisionRequest, got %q", fp.lastReq.SanddAuth.Token)
	}
	if fp.lastReq.SanddAuth.Endpoint != testEndpoint {
		t.Errorf("endpoint = %q, want the configured %q", fp.lastReq.SanddAuth.Endpoint, testEndpoint)
	}
}

// TestCreatePod_EndpointIsConfigurationNotDerived is the regression guard for the
// reachability bug this design replaced. The endpoint used to be BUILT from the
// Pod's namespace plus a per-workload controller id, producing an in-cluster
// Service name (<id>.<ns>.svc) that a NeoCloud instance can neither resolve nor
// route to — so every daemon dialed an address that could not answer while the
// instance billed on.
//
// Two Pods in different namespaces must therefore get the SAME configured address,
// verbatim: nothing about the Pod may leak into it.
func TestCreatePod_EndpointIsConfigurationNotDerived(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil, sanddCfg(&stubMinter{token: "t"}))

	for _, pod := range []*corev1.Pod{
		testPod("team-ml", "p1"),
		testPod("team-infra", "p2"),
	} {
		if err := h.CreatePod(context.Background(), pod); err != nil {
			t.Fatalf("CreatePod(%s/%s): %v", pod.Namespace, pod.Name, err)
		}
		got := fp.lastReq.SanddAuth.Endpoint
		if got != testEndpoint {
			t.Errorf("%s/%s: endpoint = %q, want the configured %q verbatim",
				pod.Namespace, pod.Name, got, testEndpoint)
		}
		// Spelled out as the shape that must never come back: an in-cluster address is
		// unreachable from the provider network, which is the failure the
		// operator-supplied endpoint exists to prevent.
		if strings.Contains(got, ".svc") {
			t.Errorf("%s/%s: endpoint is an in-cluster address daemons cannot reach: %q",
				pod.Namespace, pod.Name, got)
		}
		if strings.Contains(got, pod.Namespace) {
			t.Errorf("%s/%s: endpoint embeds the Pod namespace, so it was derived: %q",
				pod.Namespace, pod.Name, got)
		}
	}
}

// TestCreatePod_TokenNeverLandsOnThePod: the token is a bearer secret, so it must
// exist only in the in-memory ProvisionRequest. Anything written back onto the Pod
// is in etcd and readable by every actor with pod-read in the namespace — including
// the workload's own service account, i.e. the very thing the token authenticates.
func TestCreatePod_TokenNeverLandsOnThePod(t *testing.T) {
	const token = "super.secret.jwt"
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil, sanddCfg(&stubMinter{token: token}))
	pod := testPod("team-ml", "p1")

	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	// Neither the Pod we were handed nor the tracked copy VK reports may carry it.
	tracked, err := h.GetPod(context.Background(), "team-ml", "p1")
	if err != nil {
		t.Fatalf("GetPod: %v", err)
	}
	for _, p := range []*corev1.Pod{pod, tracked} {
		for k, v := range p.Annotations {
			if strings.Contains(v, token) {
				t.Errorf("token leaked into annotation %q", k)
			}
		}
		if strings.Contains(p.Status.Message, token) || strings.Contains(p.Status.Reason, token) {
			t.Errorf("token leaked into Pod status: %+v", p.Status)
		}
	}
}

// TestCreatePod_MintFailureDoesNotProvision: a signing failure must abort BEFORE
// the provider call. Launching an instance whose daemon can never authenticate
// bills indefinitely for something unreachable — strictly worse than not launching.
func TestCreatePod_MintFailureDoesNotProvision(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	bl := &recordingBlocklist{}
	h := NewHandler(fp, nil, bl, sanddCfg(&stubMinter{err: errors.New("signing key unreadable")}))

	err := h.CreatePod(context.Background(), testPod("team-ml", "p1"))
	if err == nil {
		t.Fatal("expected CreatePod to fail when minting fails")
	}
	if fp.provisionCnt != 0 {
		t.Errorf("expected no Provision after a mint failure, got %d calls", fp.provisionCnt)
	}
	// No blocklist entry: a signing failure is OUR misconfiguration, identical on
	// every provider and region, so failing over would only spread the failure while
	// poisoning candidates that are actually healthy.
	if bl.calls != 0 {
		t.Errorf("expected no blocklist entry for a local signing failure, got %d", bl.calls)
	}

	got, err := h.GetPod(context.Background(), "team-ml", "p1")
	if err != nil {
		t.Fatalf("GetPod: %v", err)
	}
	if got.Status.Phase != corev1.PodFailed {
		t.Errorf("expected the Pod marked Failed so the failure is observable, got %q", got.Status.Phase)
	}
}

// TestCreatePod_MintErrorDoesNotEmbedTheToken guards the one place a token could
// escape through an error string: the wrap in mintSanddAuth. Provision errors are
// logged AND written to Pod status, so a token in the message would be persisted
// to etcd.
func TestCreatePod_MintErrorDoesNotEmbedTheToken(t *testing.T) {
	const token = "leaky.jwt.value"
	// A minter that returns BOTH a token and an error — the shape most likely to get
	// the token spliced into an error message by a careless wrap.
	m := &stubMinter{token: token, err: errors.New("expired key")}
	h := NewHandler(&fakeProvider{}, nil, nil, sanddCfg(m))

	_, err := h.mintSanddAuth(testPod("team-ml", "p1"), "team-ml-p1")
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), token) {
		t.Errorf("token leaked into the error string: %v", err)
	}
	// The non-secret identifiers SHOULD be there — that is what makes it debuggable.
	for _, want := range []string{"team-ml-p1", "expired key"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should name %q for debuggability, got: %v", want, err)
		}
	}
}

// TestCreatePod_SandDDisabledInjectsNothing: the feature has exactly ONE switch (a
// nil *SandD) and it must be non-fatal, so a cluster running without SandD
// provisions exactly as it did before the feature existed.
func TestCreatePod_SandDDisabledInjectsNothing(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil, nil)

	if err := h.CreatePod(context.Background(), testPod("team-ml", "p1")); err != nil {
		t.Fatalf("CreatePod must still succeed with SandD off: %v", err)
	}
	if fp.provisionCnt != 1 {
		t.Fatalf("expected the instance provisioned anyway, got %d calls", fp.provisionCnt)
	}
	if fp.lastReq.SanddAuth != (provider.SanddAuth{}) {
		t.Errorf("expected zero credentials with SandD off, got %+v", fp.lastReq.SanddAuth)
	}
}

// TestCreatePod_MintUsesTheHandlerClock pins the mint to the handler's clock seam
// rather than time.Now, so a test (and a future replay/backdating bug) sees the
// same instant the rest of the handler stamps statuses with.
func TestCreatePod_MintUsesTheHandlerClock(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	m := &stubMinter{token: "t"}
	h := NewHandler(fp, nil, nil, sanddCfg(m))
	pinned := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	h.nowFn = func() metav1.Time { return metav1.Time{Time: pinned} }

	if err := h.CreatePod(context.Background(), testPod("team-ml", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if !m.now.Equal(pinned) {
		t.Errorf("expected the mint to use the handler clock %v, got %v", pinned, m.now)
	}
}
