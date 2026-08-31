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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
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

type KubeletServingCertificateBootstrapper struct {
	client        certificatesclientv1.CertificateSigningRequestInterface
	server        *KubeletServer
	nodeIP        string
	podName       string
	podNamespace  string
	csrName       string
	pollInterval  time.Duration
	retryInterval time.Duration
}

var _ manager.Runnable = (*KubeletServingCertificateBootstrapper)(nil)

func NewKubeletServingCertificateBootstrapper(
	client kubernetes.Interface,
	server *KubeletServer,
	nodeIP, podNamespace, podName, podUID string,
) (*KubeletServingCertificateBootstrapper, error) {
	if client == nil {
		return nil, errors.New("kubelet serving certificate: Kubernetes client is required")
	}
	if server == nil {
		return nil, errors.New("kubelet serving certificate: kubelet server is required")
	}
	if net.ParseIP(nodeIP) == nil {
		return nil, fmt.Errorf("kubelet serving certificate: node IP %q is invalid", nodeIP)
	}
	if podName == "" || podNamespace == "" || podUID == "" {
		return nil, errors.New("kubelet serving certificate: POD_NAME, POD_NAMESPACE and POD_UID are required")
	}

	sum := sha256.Sum256([]byte(podUID))
	return &KubeletServingCertificateBootstrapper{
		client:        client.CertificatesV1().CertificateSigningRequests(),
		server:        server,
		nodeIP:        nodeIP,
		podName:       podName,
		podNamespace:  podNamespace,
		csrName:       "nebula-kubelet-serving-" + hex.EncodeToString(sum[:12]),
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
	requestPEM, keyPEM, err := servingCertificateRequest(b.nodeIP, b.podName, key)
	if err != nil {
		return time.Time{}, err
	}

	if err := b.client.Delete(ctx, b.csrName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return time.Time{}, fmt.Errorf("delete stale CSR %s: %w", b.csrName, err)
	}
	expirationSeconds := int32(kubeletServingCertificateLifetime / time.Second)
	csr, err := b.client.Create(ctx, &certificatesv1.CertificateSigningRequest{
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
		"csr", csr.Name,
		"approveCommand", "kubectl certificate approve "+csr.Name,
		"podIP", b.nodeIP)

	ticker := time.NewTicker(b.pollInterval)
	defer ticker.Stop()
	for {
		current, err := b.client.Get(ctx, b.csrName, metav1.GetOptions{})
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

		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func servingCertificateRequest(nodeIP, podName string, key *ecdsa.PrivateKey) ([]byte, []byte, error) {
	template := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   "system:node:" + podName,
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
