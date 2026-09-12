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
	"errors"
	"fmt"
	"net"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	certificatesclientv1 "k8s.io/client-go/kubernetes/typed/certificates/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	kubeletServingCertificateLifetime = 30 * 24 * time.Hour
	kubeletServingRenewBefore         = 24 * time.Hour
	kubeletServingRetryInterval       = 30 * time.Second
	kubeletServingPollInterval        = 2 * time.Second
)

// NodeIdentity is the username the kubernetes.io/kubelet-serving signer expects on a request
// for a node's serving certificate.
//
// One function because two places must agree: the request's CN, and the identity the client
// impersonates to submit it. The signer compares them and ignores a mismatch in silence — the
// CSR stays Approved and unsigned, with no condition to notice.
func NodeIdentity(nodeName string) string { return "system:node:" + nodeName }

// ServingCSRName is the CSR one virtual node reuses for the life of the cluster.
//
// Derived from the node name and nothing per-process, so RBAC can scope delete, get and
// approval to exactly these names (see the markers in cmd/main.go). Changing the format means
// changing that list too, or the manager loses access to its own CSR.
func ServingCSRName(nodeName string) string { return "nebula-kubelet-serving-" + nodeName }

type KubeletServingCertificateBootstrapper struct {
	// nodeClient impersonates the virtual node and CREATES the request; ownClient is the
	// manager's own identity and does the rest. See the constructor.
	nodeClient    certificatesclientv1.CertificateSigningRequestInterface
	ownClient     certificatesclientv1.CertificateSigningRequestInterface
	server        *KubeletServer
	nodeIP        string
	nodeName      string
	podName       string
	podNamespace  string
	csrName       string
	pollInterval  time.Duration
	retryInterval time.Duration
}

var _ manager.Runnable = (*KubeletServingCertificateBootstrapper)(nil)

// NewKubeletServingCertificateBootstrapper builds the CSR loop for one virtual node, over TWO
// clients because no single identity can do the whole job:
//
//   - nodeClient impersonates NodeIdentity(nodeName) and creates the request; the signer
//     refuses one submitted by anything else.
//   - ownClient is the manager's ServiceAccount and does the rest. A node may create and get
//     its own CSRs and nothing more — on EKS it `cannot delete resource
//     "certificatesigningrequests"`, and approving is an approver's job anyway.
//
// Requester and approver differing is the ordinary arrangement: the signer checks who ASKED.
func NewKubeletServingCertificateBootstrapper(
	nodeClient, ownClient kubernetes.Interface,
	server *KubeletServer,
	nodeIP, nodeName, podNamespace, podName string,
) (*KubeletServingCertificateBootstrapper, error) {
	if nodeClient == nil || ownClient == nil {
		return nil, errors.New("kubelet serving certificate: both the node and manager clients are required")
	}
	if server == nil {
		return nil, errors.New("kubelet serving certificate: kubelet server is required")
	}
	if net.ParseIP(nodeIP) == nil {
		return nil, fmt.Errorf("kubelet serving certificate: node IP %q is invalid", nodeIP)
	}
	if nodeName == "" {
		return nil, errors.New("kubelet serving certificate: a virtual node name is required")
	}
	if podName == "" || podNamespace == "" {
		return nil, errors.New("kubelet serving certificate: POD_NAME and POD_NAMESPACE are required")
	}

	return &KubeletServingCertificateBootstrapper{
		nodeClient:    nodeClient.CertificatesV1().CertificateSigningRequests(),
		ownClient:     ownClient.CertificatesV1().CertificateSigningRequests(),
		server:        server,
		nodeIP:        nodeIP,
		nodeName:      nodeName,
		podName:       podName,
		podNamespace:  podNamespace,
		csrName:       ServingCSRName(nodeName),
		pollInterval:  kubeletServingPollInterval,
		retryInterval: kubeletServingRetryInterval,
	}, nil
}

func (b *KubeletServingCertificateBootstrapper) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("kubelet-serving-certificate")
	for {
		notAfter, err := b.requestAndWait(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Error(err, "serving certificate bootstrap failed; retaining the current certificate",
				"retryAfter", b.retryInterval)
			if !waitForContext(ctx, b.retryInterval) {
				return nil
			}
			continue
		}

		renewIn := time.Until(notAfter.Add(-kubeletServingRenewBefore))
		if renewIn < time.Minute {
			renewIn = time.Minute
		}
		log.Info("installed trusted kubelet serving certificate", "expires", notAfter, "renewIn", renewIn)
		if !waitForContext(ctx, renewIn) {
			return nil
		}
	}
}

func (b *KubeletServingCertificateBootstrapper) requestAndWait(ctx context.Context) (time.Time, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return time.Time{}, fmt.Errorf("generate private key: %w", err)
	}
	requestPEM, keyPEM, err := servingCertificateRequest(b.nodeIP, NodeIdentity(b.nodeName), key)
	if err != nil {
		return time.Time{}, err
	}

	// A CSR left by an earlier attempt is unusable: its certificate would be for a key we no
	// longer hold. Usually a no-op — the cleaner drops an issued CSR an hour after approval.
	if err := b.ownClient.Delete(ctx, b.csrName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return time.Time{}, fmt.Errorf("delete stale CSR %s: %w", b.csrName, err)
	}
	expirationSeconds := int32(kubeletServingCertificateLifetime / time.Second)
	// The one call whose IDENTITY matters (see the constructor).
	csr, err := b.nodeClient.Create(ctx, &certificatesv1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: b.csrName,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "nebula",
				"app.kubernetes.io/component": "kubelet-serving-certificate",
			},
			Annotations: map[string]string{
				"nebula.inftyai.com/pod-name":      b.podName,
				"nebula.inftyai.com/pod-namespace": b.podNamespace,
			},
		},
		Spec: certificatesv1.CertificateSigningRequestSpec{
			Request:           requestPEM,
			SignerName:        certificatesv1.KubeletServingSignerName,
			ExpirationSeconds: &expirationSeconds,
			Usages: []certificatesv1.KeyUsage{
				certificatesv1.UsageDigitalSignature,
				certificatesv1.UsageServerAuth,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return time.Time{}, fmt.Errorf("create CSR %s: %w", b.csrName, err)
	}

	log := logf.FromContext(ctx).WithName("kubelet-serving-certificate")
	log.Info("waiting for kubelet serving certificate approval",
		"csr", csr.Name, "identity", NodeIdentity(b.nodeName), "podIP", b.nodeIP)

	ticker := time.NewTicker(b.pollInterval)
	defer ticker.Stop()
	for {
		current, err := b.ownClient.Get(ctx, b.csrName, metav1.GetOptions{})
		if err != nil {
			return time.Time{}, fmt.Errorf("get CSR %s: %w", b.csrName, err)
		}
		for _, condition := range current.Status.Conditions {
			if condition.Type == certificatesv1.CertificateDenied || condition.Type == certificatesv1.CertificateFailed {
				return time.Time{}, fmt.Errorf("CSR %s ended with %s: %s", b.csrName, condition.Type, condition.Message)
			}
		}
		if len(current.Status.Certificate) > 0 {
			cert, notAfter, err := servingCertificate(current.Status.Certificate, keyPEM, b.nodeIP)
			if err != nil {
				return time.Time{}, fmt.Errorf("load certificate from CSR %s: %w", b.csrName, err)
			}
			b.server.SetServingCertificate(cert)
			return notAfter, nil
		}

		// Retrying in here recovers a transient UpdateApproval in one tick, where recreating the
		// CSR would cost the 24h the cleaner takes to remove it AND keep yanking it out from
		// under a human approving by hand. `current` for a fresh resourceVersion.
		if !isApproved(current) {
			if err := b.approve(ctx, current); err != nil {
				log.Error(err, "could not self-approve the serving certificate request; "+
					"approve it by hand or the endpoint keeps its self-signed certificate",
					"csr", b.csrName, "approveCommand", "kubectl certificate approve "+b.csrName)
			}
		}

		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func isApproved(csr *certificatesv1.CertificateSigningRequest) bool {
	for _, condition := range csr.Status.Conditions {
		if condition.Type == certificatesv1.CertificateApproved {
			return true
		}
	}
	return false
}

// approve approves our own request, as the MANAGER — a node cannot approve its own
// certificate, and the built-in approver only handles real kubelets. Without this the endpoint
// keeps its self-signed certificate until a human runs `kubectl certificate approve`, at every
// renewal.
//
// Caller must check isApproved first: a second Approved condition is rejected by validation.
func (b *KubeletServingCertificateBootstrapper) approve(
	ctx context.Context, csr *certificatesv1.CertificateSigningRequest,
) error {
	csr.Status.Conditions = append(csr.Status.Conditions, certificatesv1.CertificateSigningRequestCondition{
		Type:           certificatesv1.CertificateApproved,
		Status:         corev1.ConditionTrue,
		Reason:         "NebulaKubeletServing",
		Message:        "approved by the Nebula manager for its own kubelet serving endpoint",
		LastUpdateTime: metav1.Now(),
	})
	_, err := b.ownClient.UpdateApproval(ctx, b.csrName, csr, metav1.UpdateOptions{})
	return err
}

func servingCertificateRequest(nodeIP, identity string, key *ecdsa.PrivateKey) ([]byte, []byte, error) {
	template := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   identity,
			Organization: []string{"system:nodes"},
		},
		IPAddresses: []net.IP{net.ParseIP(nodeIP)},
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, template, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create serving certificate request: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal serving certificate key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}

func servingCertificate(certPEM, keyPEM []byte, nodeIP string) (tls.Certificate, time.Time, error) {
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, time.Time{}, err
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return tls.Certificate{}, time.Time{}, fmt.Errorf("parse leaf certificate: %w", err)
	}
	if err := leaf.VerifyHostname(nodeIP); err != nil {
		return tls.Certificate{}, time.Time{}, fmt.Errorf("certificate does not cover advertised IP %s: %w", nodeIP, err)
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return tls.Certificate{}, time.Time{}, fmt.Errorf("certificate validity is %s to %s", leaf.NotBefore, leaf.NotAfter)
	}
	pair.Leaf = leaf
	return pair, leaf.NotAfter, nil
}

func waitForContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
