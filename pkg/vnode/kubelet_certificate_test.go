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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/yaml"
)

func TestKubeletServingCertificateBootstrapperInstallsIssuedCertificate(t *testing.T) {
	client := fake.NewSimpleClientset()
	server, err := NewKubeletServer("10.20.18.154", ":10250", "")
	if err != nil {
		t.Fatalf("NewKubeletServer: %v", err)
	}
	tlsConfig, err := server.tlsConfig()
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}
	fallback, err := tlsConfig.GetCertificate(nil)
	if err != nil {
		t.Fatalf("get fallback certificate: %v", err)
	}
	fallbackLeaf, err := x509.ParseCertificate(fallback.Certificate[0])
	if err != nil {
		t.Fatalf("parse fallback certificate: %v", err)
	}

	bootstrapper, err := NewKubeletServingCertificateBootstrapper(
		client,
		client,
		server,
		"10.20.18.154",
		"nebula-modal",
		"nebula-system",
		"nebula-controller-manager-abc",
	)
	if err != nil {
		t.Fatalf("NewKubeletServingCertificateBootstrapper: %v", err)
	}
	bootstrapper.pollInterval = 5 * time.Millisecond
	bootstrapper.retryInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- bootstrapper.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("bootstrapper Start: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("bootstrapper did not stop")
		}
	})

	// Waits for the approval rather than merely for the object, so the assertion below cannot
	// race the UpdateApproval that follows the create.
	var csr *certificatesv1.CertificateSigningRequest
	waitFor(t, func() bool {
		csr, err = client.CertificatesV1().CertificateSigningRequests().Get(
			context.Background(), bootstrapper.csrName, metav1.GetOptions{},
		)
		return err == nil && isApproved(csr)
	}, "self-approved kubelet-serving CSR")

	if csr.Spec.SignerName != certificatesv1.KubeletServingSignerName {
		t.Fatalf("signer = %q, want %q", csr.Spec.SignerName, certificatesv1.KubeletServingSignerName)
	}
	request := parseCertificateRequest(t, csr.Spec.Request)
	// Must be the node identity the client impersonates, not the Pod: the signer compares the
	// two and ignores a mismatch without any condition to notice (see NodeIdentity).
	if request.Subject.CommonName != "system:node:nebula-modal" {
		t.Fatalf("common name = %q", request.Subject.CommonName)
	}
	if len(request.Subject.Organization) != 1 || request.Subject.Organization[0] != "system:nodes" {
		t.Fatalf("organization = %v, want [system:nodes]", request.Subject.Organization)
	}
	if len(request.IPAddresses) != 1 || !request.IPAddresses[0].Equal(net.ParseIP("10.20.18.154")) {
		t.Fatalf("IP SANs = %v, want [10.20.18.154]", request.IPAddresses)
	}

	csr.Status.Conditions = append(csr.Status.Conditions, certificatesv1.CertificateSigningRequestCondition{
		Type:   certificatesv1.CertificateApproved,
		Status: "True",
		Reason: "TestApproved",
	})
	csr.Status.Certificate = issueTestServingCertificate(t, request)
	if _, err := client.CertificatesV1().CertificateSigningRequests().UpdateStatus(
		context.Background(), csr, metav1.UpdateOptions{},
	); err != nil {
		t.Fatalf("issue certificate: %v", err)
	}

	waitFor(t, func() bool {
		current, getErr := tlsConfig.GetCertificate(nil)
		return getErr == nil && current.Leaf != nil && current.Leaf.SerialNumber.Cmp(fallbackLeaf.SerialNumber) != 0
	}, "issued certificate installation")
	current, err := tlsConfig.GetCertificate(nil)
	if err != nil {
		t.Fatalf("get installed certificate: %v", err)
	}
	if err := current.Leaf.VerifyHostname("10.20.18.154"); err != nil {
		t.Fatalf("installed certificate does not cover advertised IP: %v", err)
	}
}

// TestKubeletServingCertificateBootstrapperRoutesVerbsByIdentity pins each call to the client
// allowed to make it, by denying what a real cluster denies. The bug it encodes: everything
// went through the impersonating client, and the delete runs first — so it failed Forbidden
// before creating anything, leaving no CSR at all.
func TestKubeletServingCertificateBootstrapperRoutesVerbsByIdentity(t *testing.T) {
	nodeFake := fake.NewSimpleClientset()
	ownFake := fake.NewSimpleClientset()
	// Two clients over ONE store, so which client issued a call is observable. Delegating to the
	// tracker rather than the clientset deliberately bypasses nodeFake's denials below.
	ownFake.PrependReactor("*", "certificatesigningrequests", k8stesting.ObjectReaction(nodeFake.Tracker()))

	deny := func(verb string) k8stesting.ReactionFunc {
		return func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Group: "certificates.k8s.io", Resource: "certificatesigningrequests"},
				"", fmt.Errorf("%s is not granted to this identity", verb))
		}
	}
	nodeFake.PrependReactor("delete", "certificatesigningrequests", deny("delete"))
	nodeFake.PrependReactor("update", "certificatesigningrequests", deny("update"))
	ownFake.PrependReactor("create", "certificatesigningrequests", deny("create"))

	server, err := NewKubeletServer("10.20.18.154", ":10250", "")
	if err != nil {
		t.Fatalf("NewKubeletServer: %v", err)
	}
	bootstrapper, err := NewKubeletServingCertificateBootstrapper(
		nodeFake, ownFake, server,
		"10.20.18.154", "nebula-modal", "nebula-system",
		"nebula-controller-manager-abc",
	)
	if err != nil {
		t.Fatalf("NewKubeletServingCertificateBootstrapper: %v", err)
	}
	bootstrapper.pollInterval = 5 * time.Millisecond
	bootstrapper.retryInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = bootstrapper.Start(ctx) }()

	// Reaching Approved proves the routing: delete and approval went out as the manager, the
	// create as the node.
	waitFor(t, func() bool {
		csr, getErr := nodeFake.CertificatesV1().CertificateSigningRequests().Get(
			ctx, bootstrapper.csrName, metav1.GetOptions{},
		)
		return getErr == nil && isApproved(csr)
	}, "CSR created as the node and approved as the manager")
}

// TestKubeletServingCertificateBootstrapperRetriesApproval covers a transient UpdateApproval.
// Approving from outside the poll left nothing to try again, so the loop watched a CSR that
// could not be signed until the cleaner removed it a day later.
func TestKubeletServingCertificateBootstrapperRetriesApproval(t *testing.T) {
	client := fake.NewSimpleClientset()
	var mu sync.Mutex
	var attempts int
	client.PrependReactor("update", "certificatesigningrequests",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			mu.Lock()
			defer mu.Unlock()
			if attempts++; attempts == 1 {
				return true, nil, apierrors.NewServiceUnavailable("etcd leader election in progress")
			}
			return false, nil, nil
		})

	server, err := NewKubeletServer("10.20.18.154", ":10250", "")
	if err != nil {
		t.Fatalf("NewKubeletServer: %v", err)
	}
	bootstrapper, err := NewKubeletServingCertificateBootstrapper(
		client, client, server,
		"10.20.18.154", "nebula-modal", "nebula-system",
		"nebula-controller-manager-abc",
	)
	if err != nil {
		t.Fatalf("NewKubeletServingCertificateBootstrapper: %v", err)
	}
	bootstrapper.pollInterval = 5 * time.Millisecond
	// Long enough that a recreate-from-scratch cannot masquerade as a retry.
	bootstrapper.retryInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = bootstrapper.Start(ctx) }()

	waitFor(t, func() bool {
		csr, getErr := client.CertificatesV1().CertificateSigningRequests().Get(
			ctx, bootstrapper.csrName, metav1.GetOptions{},
		)
		return getErr == nil && isApproved(csr)
	}, "approval retried after a transient failure")

	// The loop polls for a certificate that never arrives, so without the isApproved guard it
	// would keep approving an approved CSR — which the real API rejects as a duplicate
	// condition. Settling for many intervals is what makes the count meaningful.
	time.Sleep(20 * bootstrapper.pollInterval)
	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 {
		t.Fatalf("UpdateApproval calls = %d, want 2 (one transient failure, one success)", attempts)
	}
}

// TestServingCSRNameIsScopedByRBAC guards the coupling the narrow grant rests on: the name is
// computed in Go, the resourceNames list is written by hand, and drift between them is silent
// in CI and surfaces only as a Forbidden on a real cluster.
func TestServingCSRNameIsScopedByRBAC(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "role.yaml"))
	if err != nil {
		t.Fatalf("read role.yaml: %v", err)
	}
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal(raw, &role); err != nil {
		t.Fatalf("parse role.yaml: %v", err)
	}

	scoped := map[string]map[string]bool{}
	for _, rule := range role.Rules {
		if !slices.Contains(rule.APIGroups, certificatesv1.GroupName) {
			continue
		}
		for _, resource := range rule.Resources {
			// An unscoped rule may only create: that verb cannot be scoped by name, while
			// deleting or approving someone else's CSR is what the scoping exists to prevent.
			if len(rule.ResourceNames) == 0 {
				if !slices.Equal(rule.Verbs, []string{"create"}) {
					t.Errorf("cluster-wide rule on %s grants %v, want [create] alone", resource, rule.Verbs)
				}
				continue
			}
			if scoped[resource] == nil {
				scoped[resource] = map[string]bool{}
			}
			for _, name := range rule.ResourceNames {
				scoped[resource][name] = true
			}
		}
	}

	for _, resource := range []string{"certificatesigningrequests", "certificatesigningrequests/approval"} {
		for _, provider := range []string{"aws", "modal", "fake"} {
			if want := ServingCSRName(NodeName(provider)); !scoped[resource][want] {
				t.Errorf("role.yaml does not scope %s to %q; run `make manifests` after changing "+
					"ServingCSRName or the markers in cmd/main.go", resource, want)
			}
		}
	}
}

func TestServingCertificateRejectsWrongIP(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	requestPEM, keyPEM, err := servingCertificateRequest("10.20.18.155", "manager", key)
	if err != nil {
		t.Fatalf("servingCertificateRequest: %v", err)
	}
	certificatePEM := issueTestServingCertificate(t, parseCertificateRequest(t, requestPEM))
	if _, _, err := servingCertificate(certificatePEM, keyPEM, "10.20.18.154"); err == nil {
		t.Fatal("expected the certificate with the wrong IP SAN to be rejected")
	}
}

func parseCertificateRequest(t *testing.T, requestPEM []byte) *x509.CertificateRequest {
	t.Helper()
	block, _ := pem.Decode(requestPEM)
	if block == nil {
		t.Fatal("CSR is not PEM")
	}
	request, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		t.Fatalf("parse CSR: %v", err)
	}
	if err := request.CheckSignature(); err != nil {
		t.Fatalf("CSR signature: %v", err)
	}
	return request
}

func issueTestServingCertificate(t *testing.T, request *x509.CertificateRequest) []byte {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	now := time.Now()
	ca := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test kubelet CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(72 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      request.Subject,
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(48 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  request.IPAddresses,
		DNSNames:     request.DNSNames,
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, request.PublicKey, caKey)
	if err != nil {
		t.Fatalf("issue serving certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
