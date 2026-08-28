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
	"math/big"
	"net"
	"testing"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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
		server,
		"10.20.18.154",
		"nebula-system",
		"nebula-controller-manager-abc",
		"3d18b85e-43aa-4ed6-b5e0-38fd04d93241",
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

	var csr *certificatesv1.CertificateSigningRequest
	waitFor(t, func() bool {
		csr, err = client.CertificatesV1().CertificateSigningRequests().Get(
			context.Background(), bootstrapper.csrName, metav1.GetOptions{},
		)
		return err == nil
	}, "kubelet-serving CSR")

	if csr.Spec.SignerName != certificatesv1.KubeletServingSignerName {
		t.Fatalf("signer = %q, want %q", csr.Spec.SignerName, certificatesv1.KubeletServingSignerName)
	}
	request := parseCertificateRequest(t, csr.Spec.Request)
	if request.Subject.CommonName != "system:node:nebula-controller-manager-abc" {
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
