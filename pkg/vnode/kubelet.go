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
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"time"

	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// The kubelet API endpoint that makes `kubectl exec` reach a workload. exec.go was only
// half the path: kubectl asks the API SERVER, which dials the node's advertised kubelet
// endpoint and proxies the stream — so without this, RunInContainer is never called.
//
// Not nodeutil, which would provide this whole stack: its authn/authz dependency on
// k8s.io/apiserver does not compile against the pinned k8s 0.33 line (same reason node.go
// wires the low-level `node` package by hand).

const (
	// An interactive shell someone leaves open is idle by definition, so this is
	// generous — the API-server leg must not be the tightest of the three idle timeouts
	// (the others: exec.go's read loop, ingress.yaml's edge).
	kubeletStreamIdleTimeout = 4 * time.Hour

	// Bounds stream SETUP only, a local handshake. Matches the reference kubelet.
	kubeletStreamCreationTimeout = 30 * time.Second

	// Minted per process start, in memory only, so a restart replaces it.
	certValidity = 365 * 24 * time.Hour
)

// serveKubeletAPI serves the kubelet pod routes for one virtual node and returns the port
// it bound. It runs until ctx is cancelled.
//
// The port is ephemeral because one manager process hosts a virtual node per provider: a
// fixed port would collide on the second one. The node object advertises whatever the
// kernel assigned (see node.go), so nothing needs to predict it.
//
// SECURITY: these routes are UNAUTHENTICATED. A real kubelet verifies the API server's
// client cert and runs a SubjectAccessReview, which needs the k8s.io/apiserver stack that
// rules out nodeutil. So anything that can reach this port can exec into any Pod this node
// tracks, with no audit trail. What contains it is the network — it is on the manager's pod
// IP and no Service or Ingress exposes it. Keep it that way; authenticating is a
// prerequisite for any cluster where pod-network access isn't already trusted.
func serveKubeletAPI(ctx context.Context, h *Handler, nodeName string) (int, error) {
	log := logf.FromContext(ctx).WithValues("virtualNode", nodeName)

	cert, err := newServingCert(nodeName)
	if err != nil {
		return 0, fmt.Errorf("mint kubelet serving cert: %w", err)
	}

	// Only RunInContainer is wired; the nil fields make VK answer those routes 501, which
	// says "this node cannot" rather than the handler stubs' "no such pod".
	mux := http.NewServeMux()
	vkapi.AttachPodRoutes(vkapi.PodHandlerConfig{
		RunInContainer:        h.RunInContainer,
		StreamIdleTimeout:     kubeletStreamIdleTimeout,
		StreamCreationTimeout: kubeletStreamCreationTimeout,
	}, mux, false)

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, fmt.Errorf("listen for the kubelet API: %w", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	srv := &http.Server{
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		// No Read/WriteTimeout: an exec stream is long-lived and either would sever a
		// working shell. Idleness is bounded inside the stream handler instead.
		ReadHeaderTimeout: kubeletStreamCreationTimeout,
	}

	go func() {
		<-ctx.Done()
		// Close, not Shutdown: an interactive shell never drains, so Shutdown would
		// block until the idle timeout.
		_ = srv.Close()
	}()

	go func() {
		// Empty paths: the cert comes from TLSConfig, nothing touches the filesystem.
		if err := srv.ServeTLS(ln, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Logged, not fatal: losing a debugging surface beats taking down every
			// workload on this provider.
			log.Error(err, "kubelet API server stopped; exec into this node's pods will fail")
		}
	}()

	log.Info("serving the kubelet API for exec", "port", port)
	return port, nil
}

// newServingCert mints the self-signed serving cert for the kubelet endpoint.
//
// Self-signed because the API server verifies it only when started with
// --kubelet-certificate-authority, and skips verification for kubelet connections
// otherwise — the same reason a real kubelet's self-signed cert works out of the box. A
// CA-backed rotating cert would add a Secret and a trust-distribution problem to satisfy a
// check that is usually off. The consequence: that leg is encrypted but, by default, not
// authenticated in either direction.
func newServingCert(nodeName string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate serial: %w", err)
	}

	// IP SANs, not just a DNS name: the API server dials the node's advertised IP, so a
	// CN-only cert fails in exactly the clusters that verify.
	ips, err := hostIPs()
	if err != nil {
		return tls.Certificate{}, err
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: nodeName, Organization: []string{"nebula"}},
		// Backdated so a lagging API server clock doesn't reject a fresh cert.
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(certValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           ips,
		DNSNames:              []string{nodeName},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create certificate: %w", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

// hostIPs returns the cert's IP SANs. Enumerated rather than read from POD_IP so this works
// outside a Pod too; an extra SAN nobody dials is inert.
func hostIPs() ([]net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("enumerate interface addresses: %w", err)
	}
	ips := []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() || ipNet.IP.IsLinkLocalUnicast() {
			continue
		}
		ips = append(ips, ipNet.IP)
	}
	return ips, nil
}
