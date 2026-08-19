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
	"fmt"
	"net"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// csrIssueTimeout bounds the whole request → approve → issue round trip. Short on
	// purpose: an approved CSR that is never signed is the normal outcome on a control
	// plane whose kubelet-serving signer is off, and waiting longer only delays the
	// fallback.
	csrIssueTimeout = 30 * time.Second
	// csrPollInterval is how often the pending CSR is re-read while waiting.
	csrPollInterval = time.Second
	// csrRetryInterval is how long to wait before asking again after a failed request,
	// i.e. while the fallback cert is being served. Long enough not to hammer the API
	// server, short enough that granting the missing RBAC takes effect without a restart.
	csrRetryInterval = 10 * time.Minute
	// csrRenewFloor keeps the rotation loop from spinning on a cert that is already close
	// to expiry, or expired.
	csrRenewFloor = time.Minute
)

// EnableServingCSR makes the endpoint ask the cluster's kubelet-serving signer for its
// serving cert instead of self-signing it. Needed on an API server started with
// --kubelet-certificate-authority, which rejects a self-signed kubelet cert; pointless
// elsewhere, and the approval it needs is privileged, hence opt-in.
//
// nodeName only has to be one of our virtual node names: the signer requires the subject
// to be system:node:<name>, while the API server verifies the SAN and not the CN, so one
// cert serves every node this process hosts. Call before Start.
func (s *KubeletServer) EnableServingCSR(cs kubernetes.Interface, nodeName string) {
	s.csrClient = cs
	s.csrNodeName = nodeName
}

// servingCert is what the endpoint presents: signer-issued when that is enabled and works,
// self-signed otherwise. The bool reports which, since it decides how soon the rotation
// loop tries again. An error means even self-signing failed.
func (s *KubeletServer) servingCert(ctx context.Context) (tls.Certificate, bool, error) {
	if s.csrClient == nil {
		cert, err := selfSignedCert(s.nodeIP)
		return cert, false, err
	}
	cert, err := s.requestServingCert(ctx)
	if err == nil {
		return cert, true, nil
	}
	// Degrade, never fail: a node that serves no logs still runs workloads, and self-signed
	// is the working configuration on every cluster that does not verify.
	logf.FromContext(ctx).Error(err, "no signer-issued serving cert; falling back to self-signed, "+
		"so `kubectl logs` and `kubectl exec` will fail if the API server sets "+
		"--kubelet-certificate-authority")
	cert, err = selfSignedCert(s.nodeIP)
	return cert, false, err
}

// requestServingCert obtains a serving cert from the cluster's kubelet-serving signer, so
// the API server accepts this endpoint the way it accepts a real kubelet's. There is no
// alternative for a verifying API server: Node has no caBundle field, unlike APIService
// and the webhook configurations, so our own CA cannot be published anywhere it would be
// trusted.
//
// The private key is generated here and never leaves this process; only the CSR is written
// to the API.
func (s *KubeletServer) requestServingCert(ctx context.Context) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet serving cert: generate key: %w", err)
	}
	// The signer enforces this subject. The SANs mirror selfSignedCert: the advertised IP
	// because that is what the API server dials, loopback for curl'ing inside the pod.
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   "system:node:" + s.csrNodeName,
			Organization: []string{"system:nodes"},
		},
		IPAddresses: []net.IP{net.ParseIP(s.nodeIP), net.IPv4(127, 0, 0, 1)},
		DNSNames:    []string{"localhost"},
	}, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet serving cert: create request: %w", err)
	}

	csrs := s.csrClient.CertificatesV1().CertificateSigningRequests()
	name := s.csrName()
	// Delete first: the name is deterministic, so a restart finds the CSR of the PREVIOUS
	// pod IP, and the API rejects reusing a name with different content.
	if err := csrs.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return tls.Certificate{}, fmt.Errorf("kubelet serving cert: delete stale CSR %s: %w", name, err)
	}

	created, err := csrs.Create(ctx, &certificatesv1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: certificatesv1.CertificateSigningRequestSpec{
			Request:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}),
			SignerName: certificatesv1.KubeletServingSignerName,
			// No key encipherment: it means nothing for an ECDSA key, and the signer accepts
			// this pair.
			Usages: []certificatesv1.KeyUsage{
				certificatesv1.UsageDigitalSignature,
				certificatesv1.UsageServerAuth,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet serving cert: create CSR %s: %w", name, err)
	}

	// Self-approve. kube-controller-manager auto-approves node CLIENT certs only, never
	// serving ones, because an approver cannot verify that a requester owns the SANs it
	// asks for — which is exactly what we are asserting about ourselves here, and why the
	// RBAC for it ships separately.
	created.Status.Conditions = append(created.Status.Conditions, certificatesv1.CertificateSigningRequestCondition{
		Type:    certificatesv1.CertificateApproved,
		Status:  corev1.ConditionTrue,
		Reason:  "NebulaSelfApproved",
		Message: "requested by the Nebula manager for its own kubelet endpoint",
	})
	if _, err := csrs.UpdateApproval(ctx, name, created, metav1.UpdateOptions{}); err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet serving cert: approve CSR %s: %w", name, err)
	}

	// Polling, not a watch: one object, seconds of waiting, and a watch here would need its
	// own reconnect handling to be no more reliable.
	var issued []byte
	err = wait.PollUntilContextTimeout(ctx, csrPollInterval, s.issueTimeout(), true,
		func(ctx context.Context) (bool, error) {
			cur, err := csrs.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			for _, c := range cur.Status.Conditions {
				if c.Status != corev1.ConditionTrue {
					continue
				}
				switch c.Type {
				case certificatesv1.CertificateDenied:
					return false, fmt.Errorf("denied: %s", c.Message)
				case certificatesv1.CertificateFailed:
					return false, fmt.Errorf("failed: %s", c.Message)
				}
			}
			issued = cur.Status.Certificate
			return len(issued) > 0, nil
		})
	if err != nil {
		// Approved but never signed is the signature of a control plane whose
		// kubelet-serving signer is disabled — common on managed offerings.
		return tls.Certificate{}, fmt.Errorf("kubelet serving cert: CSR %s not issued: %w", name, err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet serving cert: marshal key: %w", err)
	}
	cert, err := tls.X509KeyPair(issued, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet serving cert: load issued cert: %w", err)
	}
	// Leaf is what the rotation loop reads NotAfter from: the signer decides the lifetime
	// (--cluster-signing-duration), not us.
	if cert.Leaf == nil {
		if cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0]); err != nil {
			return tls.Certificate{}, fmt.Errorf("kubelet serving cert: parse issued cert: %w", err)
		}
	}
	return cert, nil
}

// rotateServingCert replaces the cert before it expires, and keeps trying while the
// fallback is in use. Runs until ctx is cancelled.
//
// It exists because the signer's lifetime is the cluster's to choose: a control plane with
// a short --cluster-signing-duration would otherwise lose logs mid-run, silently, hours
// after a start that looked fine.
func (s *KubeletServer) rotateServingCert(ctx context.Context) {
	log := logf.FromContext(ctx)
	for {
		delay := s.renewAfter()
		log.V(1).Info("kubelet serving cert renewal scheduled", "in", delay.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		cert, issued, err := s.servingCert(ctx)
		if err != nil {
			// Keep the old cert: an expiring cert still serves, a missing one serves nothing.
			log.Error(err, "kubelet serving cert renewal failed; keeping the current cert")
			continue
		}
		s.setCert(cert, issued)
		log.Info("kubelet serving cert renewed", "signerIssued", issued)
	}
}

// renewAfter is how long to wait before asking again: two thirds of the current cert's
// remaining life when the signer issued it, a fixed retry while on the fallback.
func (s *KubeletServer) renewAfter() time.Duration {
	s.certMu.RLock()
	cert, issued := s.cert, s.certIssued
	s.certMu.RUnlock()

	if !issued || cert == nil || cert.Leaf == nil {
		return csrRetryInterval
	}
	if d := time.Until(cert.Leaf.NotAfter) * 2 / 3; d > csrRenewFloor {
		return d
	}
	return csrRenewFloor
}

// csrName is deterministic, so a restart replaces its own CSR rather than leaving one
// behind per pod IP. One name for the whole deployment is safe because Start is
// leader-scoped: at most one replica requests, and a new leader replacing the outgoing
// one's CSR is the intended outcome.
func (s *KubeletServer) csrName() string {
	return "nebula-kubelet-serving-" + s.csrNodeName
}

func (s *KubeletServer) issueTimeout() time.Duration {
	if s.csrTimeout > 0 {
		return s.csrTimeout
	}
	return csrIssueTimeout
}

// setCert swaps in the cert served to new connections. Established ones keep the old one,
// which is correct: renewal must not sever a `kubectl logs -f`.
func (s *KubeletServer) setCert(cert tls.Certificate, issued bool) {
	s.certMu.Lock()
	defer s.certMu.Unlock()
	s.cert, s.certIssued = &cert, issued
}

func (s *KubeletServer) currentCert() *tls.Certificate {
	s.certMu.RLock()
	defer s.certMu.RUnlock()
	return s.cert
}

// signerIssued reports whether the served cert came from the cluster signer.
func (s *KubeletServer) signerIssued() bool {
	s.certMu.RLock()
	defer s.certMu.RUnlock()
	return s.certIssued
}
