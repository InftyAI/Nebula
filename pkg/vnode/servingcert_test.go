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
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	testNodeIP   = "10.244.1.7"
	testNodeName = "nebula-modal"
	testCASubj   = "test-cluster-signing-ca"
)

// testCA stands in for the cluster's kubelet-serving signer, so tests can assert on a
// certificate the fake control plane actually issued rather than on a canned blob.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: testCASubj},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	return &testCA{cert: cert, key: key}
}

// sign issues a leaf for a CSR, carrying its SANs over the way a real signer does.
func (ca *testCA) sign(t *testing.T, csrPEM []byte, lifetime time.Duration) []byte {
	t.Helper()
	csr := parseCSR(t, csrPEM)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      csr.Subject,
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(lifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  csr.IPAddresses,
		DNSNames:     csr.DNSNames,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, csr.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func parseCSR(t *testing.T, csrPEM []byte) *x509.CertificateRequest {
	t.Helper()
	block, _ := pem.Decode(csrPEM)
	if block == nil {
		t.Fatal("CSR request is not PEM")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		t.Fatalf("parse CSR: %v", err)
	}
	return csr
}

// issuingClient is a control plane whose kubelet-serving signer works: it signs on
// approval, which is the ordering the real one enforces.
func issuingClient(t *testing.T, ca *testCA, lifetime time.Duration) *fake.Clientset {
	t.Helper()
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("update", "certificatesigningrequests",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			csr := approvedCSR(action)
			if csr == nil {
				return false, nil, nil
			}
			// Mutate and fall through: the default reactor then persists it, so the next Get
			// sees the issued certificate.
			csr.Status.Certificate = ca.sign(t, csr.Spec.Request, lifetime)
			return false, nil, nil
		})
	return cs
}

// approvedCSR returns the CSR an approval update carries, or nil for any other action.
// The fake control planes react on approval because the real signer only acts after it.
func approvedCSR(action k8stesting.Action) *certificatesv1.CertificateSigningRequest {
	if action.GetSubresource() != "approval" {
		return nil
	}
	csr, ok := action.(k8stesting.UpdateAction).GetObject().(*certificatesv1.CertificateSigningRequest)
	if !ok {
		return nil
	}
	return csr
}

func serverWithCSR(t *testing.T, cs *fake.Clientset, timeout time.Duration) *KubeletServer {
	t.Helper()
	s, err := NewKubeletServer(testNodeIP, freeAddr(t), "")
	if err != nil {
		t.Fatalf("NewKubeletServer: %v", err)
	}
	s.EnableServingCSR(cs, testNodeName)
	s.csrTimeout = timeout
	return s
}

func storedCSR(t *testing.T, cs *fake.Clientset, name string) *certificatesv1.CertificateSigningRequest {
	t.Helper()
	csr, err := cs.CertificatesV1().CertificateSigningRequests().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CSR %s: %v", name, err)
	}
	return csr
}

// The signer rejects anything but this shape, and the API server verifies the SAN rather
// than the subject — so a wrong CN is rejected at issue time and a missing IP SAN only
// fails later, when logs are dialed. Both are pinned here.
func TestRequestServingCert_RequestShape(t *testing.T) {
	ca := newTestCA(t)
	cs := issuingClient(t, ca, time.Hour)
	s := serverWithCSR(t, cs, 5*time.Second)

	if _, err := s.requestServingCert(context.Background()); err != nil {
		t.Fatalf("requestServingCert: %v", err)
	}

	stored := storedCSR(t, cs, "nebula-kubelet-serving-"+testNodeName)
	if got, want := stored.Spec.SignerName, certificatesv1.KubeletServingSignerName; got != want {
		t.Errorf("SignerName = %q, want %q", got, want)
	}
	wantUsages := []certificatesv1.KeyUsage{certificatesv1.UsageDigitalSignature, certificatesv1.UsageServerAuth}
	if got := stored.Spec.Usages; len(got) != len(wantUsages) || got[0] != wantUsages[0] || got[1] != wantUsages[1] {
		t.Errorf("Usages = %v, want %v", got, wantUsages)
	}

	csr := parseCSR(t, stored.Spec.Request)
	if got, want := csr.Subject.CommonName, "system:node:"+testNodeName; got != want {
		t.Errorf("CommonName = %q, want %q", got, want)
	}
	if got := csr.Subject.Organization; len(got) != 1 || got[0] != "system:nodes" {
		t.Errorf("Organization = %v, want [system:nodes]", got)
	}
	if !hasIP(csr.IPAddresses, testNodeIP) {
		t.Errorf("IPAddresses = %v, want the advertised IP %s (what the API server dials)", csr.IPAddresses, testNodeIP)
	}
	if !hasIP(csr.IPAddresses, "127.0.0.1") {
		t.Errorf("IPAddresses = %v, want loopback", csr.IPAddresses)
	}
}

func hasIP(ips []net.IP, want string) bool {
	for _, ip := range ips {
		if ip.Equal(net.ParseIP(want)) {
			return true
		}
	}
	return false
}

// Nothing else will approve a serving CSR — kube-controller-manager approves node client
// certs only — so without this the request would sit pending until the timeout.
func TestRequestServingCert_SelfApproves(t *testing.T) {
	ca := newTestCA(t)
	cs := issuingClient(t, ca, time.Hour)
	s := serverWithCSR(t, cs, 5*time.Second)

	if _, err := s.requestServingCert(context.Background()); err != nil {
		t.Fatalf("requestServingCert: %v", err)
	}

	stored := storedCSR(t, cs, "nebula-kubelet-serving-"+testNodeName)
	var approved bool
	for _, c := range stored.Status.Conditions {
		if c.Type == certificatesv1.CertificateApproved && c.Status == corev1.ConditionTrue {
			approved = true
		}
	}
	if !approved {
		t.Fatalf("conditions = %v, want an Approved=True condition", stored.Status.Conditions)
	}
}

// The point of the whole path: the endpoint must present the CA-issued cert, since that is
// the only thing an API server with --kubelet-certificate-authority accepts.
func TestTLSConfig_ServesIssuedCert(t *testing.T) {
	ca := newTestCA(t)
	cs := issuingClient(t, ca, time.Hour)
	s := serverWithCSR(t, cs, 5*time.Second)

	if _, err := s.tlsConfig(context.Background()); err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}
	if !s.signerIssued() {
		t.Fatal("signerIssued = false, want true when the signer issued")
	}
	cert := s.currentCert()
	if cert == nil || cert.Leaf == nil {
		t.Fatal("no leaf on the served cert; rotation reads NotAfter from it")
	}
	if got := cert.Leaf.Issuer.CommonName; got != testCASubj {
		t.Errorf("issuer = %q, want the signing CA %q", got, testCASubj)
	}
	if !hasIP(cert.Leaf.IPAddresses, testNodeIP) {
		t.Errorf("served cert SANs = %v, want %s", cert.Leaf.IPAddresses, testNodeIP)
	}
}

// A control plane whose kubelet-serving signer is disabled leaves the CSR approved and
// unsigned forever. That must degrade to self-signed, not fail startup: the node still
// runs workloads, and self-signed is what every non-verifying cluster accepts.
func TestTLSConfig_FallsBackWhenNeverIssued(t *testing.T) {
	cs := fake.NewSimpleClientset() // no signing reactor
	s := serverWithCSR(t, cs, 300*time.Millisecond)

	if _, err := s.tlsConfig(context.Background()); err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}
	if s.signerIssued() {
		t.Fatal("signerIssued = true, want false on the fallback")
	}
	cert := s.currentCert()
	if cert == nil {
		t.Fatal("no cert served")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := leaf.Subject.CommonName; got != "nebula-virtual-kubelet" {
		t.Errorf("subject = %q, want the self-signed fallback", got)
	}
}

// Denial is terminal, so it must return at once instead of polling out the timeout — the
// difference between a fast degrade and a stalled startup.
func TestRequestServingCert_DeniedReturnsImmediately(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("update", "certificatesigningrequests",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			csr := approvedCSR(action)
			if csr == nil {
				return false, nil, nil
			}
			csr.Status.Conditions = append(csr.Status.Conditions, certificatesv1.CertificateSigningRequestCondition{
				Type:    certificatesv1.CertificateDenied,
				Status:  corev1.ConditionTrue,
				Reason:  "TestDenied",
				Message: "denied by the test",
			})
			return false, nil, nil
		})
	s := serverWithCSR(t, cs, time.Minute)

	start := time.Now()
	_, err := s.requestServingCert(context.Background())
	if err == nil {
		t.Fatal("expected an error for a denied CSR")
	}
	if !strings.Contains(err.Error(), "denied") {
		t.Errorf("error = %v, want it to name the denial", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("took %v; a denied CSR must not be polled until the timeout", elapsed)
	}
}

// The CSR name is deterministic, so a restart finds its own request from the previous pod
// IP. The API rejects reusing a name with different content, so the stale one is deleted.
func TestRequestServingCert_ReplacesStaleCSR(t *testing.T) {
	ca := newTestCA(t)
	cs := issuingClient(t, ca, time.Hour)
	name := "nebula-kubelet-serving-" + testNodeName
	if _, err := cs.CertificatesV1().CertificateSigningRequests().Create(context.Background(),
		&certificatesv1.CertificateSigningRequest{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: certificatesv1.CertificateSigningRequestSpec{
				Request:    []byte("stale, for an IP this pod no longer has"),
				SignerName: certificatesv1.KubeletServingSignerName,
			},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed stale CSR: %v", err)
	}

	s := serverWithCSR(t, cs, 5*time.Second)
	if _, err := s.requestServingCert(context.Background()); err != nil {
		t.Fatalf("requestServingCert: %v", err)
	}

	// The stored request must be ours, not the seeded junk.
	csr := parseCSR(t, storedCSR(t, cs, name).Spec.Request)
	if !hasIP(csr.IPAddresses, testNodeIP) {
		t.Errorf("stored CSR SANs = %v, want the current pod IP %s", csr.IPAddresses, testNodeIP)
	}
	var deleted bool
	for _, a := range cs.Actions() {
		if a.GetVerb() == "delete" && a.GetResource().Resource == "certificatesigningrequests" {
			deleted = true
		}
	}
	if !deleted {
		t.Error("no delete recorded; a stale CSR would make Create fail with AlreadyExists")
	}
}

// Renewal timing is the difference between rotating quietly and losing logs mid-run on a
// cluster with a short --cluster-signing-duration.
func TestRenewAfter(t *testing.T) {
	s := &KubeletServer{}

	t.Run("fallback retries soon", func(t *testing.T) {
		s.setCert(mustSelfSigned(t), false)
		if got := s.renewAfter(); got != csrRetryInterval {
			t.Errorf("renewAfter = %v, want the retry interval %v while self-signed", got, csrRetryInterval)
		}
	})

	t.Run("two thirds of the remaining life", func(t *testing.T) {
		cert := mustSelfSigned(t)
		cert.Leaf = &x509.Certificate{NotAfter: time.Now().Add(3 * time.Hour)}
		s.setCert(cert, true)
		if got := s.renewAfter(); got < 110*time.Minute || got > 2*time.Hour {
			t.Errorf("renewAfter = %v, want ~2h for a 3h cert", got)
		}
	})

	t.Run("expired does not spin", func(t *testing.T) {
		cert := mustSelfSigned(t)
		cert.Leaf = &x509.Certificate{NotAfter: time.Now().Add(-time.Hour)}
		s.setCert(cert, true)
		if got := s.renewAfter(); got != csrRenewFloor {
			t.Errorf("renewAfter = %v, want the floor %v", got, csrRenewFloor)
		}
	})
}

func mustSelfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	cert, err := selfSignedCert(testNodeIP)
	if err != nil {
		t.Fatalf("selfSignedCert: %v", err)
	}
	return cert
}
