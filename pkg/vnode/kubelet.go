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
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// DefaultKubeletAddr is where the kubelet API listens. Nothing requires 10250 (the
// API server dials whatever the Node advertises), but matching a real kubelet keeps
// existing NetworkPolicies and operator intuition intact.
const DefaultKubeletAddr = ":10250"

// certValidity is the lifetime of the self-signed serving cert. Rotation is a process
// restart — the key never leaves memory, so there is no Secret to keep in step.
const certValidity = 365 * 24 * time.Hour

// kubeletShutdownGrace is how long a `kubectl logs -f` stream may finish after shutdown
// starts. Short on purpose: it avoids cutting a response mid-write, it is not a drain.
const kubeletShutdownGrace = 5 * time.Second

const (
	// execIdleTimeout is how long an exec survives without a sign of life FROM THE CLIENT.
	// Not a limit on silence: client-go pings every 5s and any frame resets the timer, so a
	// shell parked at a prompt stays up while kubectl is attached, and only a client that
	// vanished (laptop closed, network gone) expires. Short, because expiry is what frees
	// our streams and the connection behind them.
	//
	// Expiry does NOT stop the command — no provider can kill one — so a disconnected
	// exec keeps running until the instance goes away.
	execIdleTimeout = 5 * time.Minute
	// execCreationTimeout bounds setting the streams up, before any command runs.
	execCreationTimeout = 30 * time.Second
)

// KubeletServer serves the kubelet API routes Nebula implements — container logs and exec
// — for every provider's virtual node.
//
// Why it exists: `kubectl logs` and `kubectl exec` are not control-plane reads. The API
// server proxies them to the kubelet of the Pod's node, at that Node's addresses and
// daemonEndpoints. A virtual node has no kubelet, so without a listener both fail whatever
// the provider can serve.
//
// One listener for all nodes: the routes carry only namespace/pod/container, so a request
// is resolved by asking each registered Handler whether it tracks that Pod — at most one
// can. Cheaper than a port per provider, and than reading the Pod to learn its node.
//
// TLS uses a self-signed in-memory cert, which is what the API server expects: it does
// not verify a kubelet's serving cert unless --kubelet-certificate-authority is set. The
// webhook cert rotator cannot help, since it mints for a Service DNS name and this
// endpoint is dialed by Pod IP.
//
// Client certs are verified only when ClientCAPath is set. Off by default because which CA
// signs the API server's kubelet client cert is not portable (kubeadm uses the cluster CA,
// EKS/GKE their own), so requiring it would break `kubectl logs` on managed control
// planes. The cost is now larger than logs: anything that can reach this port can also RUN
// COMMANDS in these Pods' instances, without passing through the API server's RBAC. Set the
// CA if you can name it, else close the port with a NetworkPolicy.
type KubeletServer struct {
	// addr is the listen address, e.g. ":10250".
	addr string

	// nodeIP is advertised as the nodes' InternalIP, and is what the API server dials. It
	// must be THIS replica's Pod IP, not a Service: tracked Pods live in one process's
	// memory, so a Service could route to a replica that answers NotFound.
	nodeIP string

	// clientCAPath, when set, is a PEM bundle of CAs whose client certs are accepted;
	// others are refused at the TLS layer. Empty disables verification — see above.
	clientCAPath string

	mu       sync.RWMutex
	handlers map[string]*Handler
}

var _ manager.Runnable = (*KubeletServer)(nil)

// NewKubeletServer builds the shared kubelet endpoint. nodeIP is the manager's Pod IP,
// addr defaults to DefaultKubeletAddr, clientCAPath may be empty.
//
// A bad address or IP is an error here, so a misconfiguration fails at startup instead
// of producing nodes that advertise an endpoint nothing serves.
func NewKubeletServer(nodeIP, addr, clientCAPath string) (*KubeletServer, error) {
	if addr == "" {
		addr = DefaultKubeletAddr
	}
	if nodeIP == "" {
		return nil, errors.New("kubelet api: node IP is required (the manager's POD_IP)")
	}
	if net.ParseIP(nodeIP) == nil {
		return nil, fmt.Errorf("kubelet api: node IP %q is not an IP address", nodeIP)
	}
	if _, err := portOf(addr); err != nil {
		return nil, err
	}
	return &KubeletServer{
		addr:         addr,
		nodeIP:       nodeIP,
		clientCAPath: clientCAPath,
		handlers:     map[string]*Handler{},
	}, nil
}

// PodIPFromEnv reads the manager's own Pod IP, projected by config/manager via a
// fieldRef. Empty means no endpoint can be advertised, so the caller degrades to
// logs-unsupported rather than publishing an unreachable address.
func PodIPFromEnv() string { return os.Getenv("POD_IP") }

// Register wires one provider's Handler in. Called by Runner before it starts; the
// node name is only a key, since lookup is by Pod.
func (s *KubeletServer) Register(nodeName string, h *Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[nodeName] = h
}

// nodeAddress is what a node advertises so the API server can find this endpoint.
// InternalIP ONLY, which is load-bearing: --kubelet-preferred-address-types tries
// Hostname first, so also advertising one would have the API server try to resolve
// "nebula-modal" in DNS and fail before reaching the IP.
func (s *KubeletServer) nodeAddress() []corev1.NodeAddress {
	return []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: s.nodeIP}}
}

// daemonEndpoints is the port half of the same advertisement.
func (s *KubeletServer) daemonEndpoints() corev1.NodeDaemonEndpoints {
	port, _ := portOf(s.addr) // validated in NewKubeletServer
	return corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: port}}
}

// Start serves until ctx is cancelled. As a manager.Runnable it is leader-scoped like
// the Runners, which is correct: only the leader holds tracked Pods, and only its IP
// is advertised on the Nodes.
func (s *KubeletServer) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("kubelet-api")

	tlsCfg, err := s.tlsConfig()
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	// Logs and exec are wired; the nil funcs make VK answer NotImplemented on
	// attach/portForward, which is the honest answer — see handler.go.
	vkapi.AttachPodRoutes(vkapi.PodHandlerConfig{
		GetContainerLogs: s.getContainerLogs,
		RunInContainer:   s.runInContainer,
		// A real kubelet's --streaming-connection-idle-timeout, which an interactive shell
		// needs: VK's own default is 30s, so `kubectl exec -it` would be cut off half a
		// minute after the user stopped typing.
		StreamIdleTimeout:     execIdleTimeout,
		StreamCreationTimeout: execCreationTimeout,
	}, mux, false)

	srv := &http.Server{
		Handler:   mux,
		TLSConfig: tlsCfg,
		// No write timeout on purpose: `kubectl logs -f` holds a response open for as long
		// as the client wants, and a write deadline would sever it on a schedule.
		ReadHeaderTimeout: 30 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("kubelet api: listen on %s: %w", s.addr, err)
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), kubeletShutdownGrace)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.V(1).Info("kubelet api shutdown", "error", err.Error())
		}
	}()

	log.Info("serving kubelet api (container logs, exec)",
		"addr", s.addr, "advertisedIP", s.nodeIP, "clientCertRequired", s.clientCAPath != "")
	// The cert and key are already in TLSConfig, hence the empty paths.
	if err := srv.ServeTLS(ln, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("kubelet api: serve: %w", err)
	}
	log.Info("kubelet api stopped")
	return nil
}

// handlerFor finds the virtual node running this Pod. The routes carry only
// namespace/pod/container, so the owner is found by asking each registered Handler — at
// most one tracks a given Pod. nil means no node here runs it (the Pod is elsewhere, or
// leadership moved and the Runners have not re-adopted it yet).
func (s *KubeletServer) handlerFor(namespace, podName string) *Handler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, h := range s.handlers {
		if h.Tracks(namespace, podName) {
			return h
		}
	}
	return nil
}

// getContainerLogs serves the containerLogs route from the Handler that owns the Pod. Its
// error is returned as-is: a provider that cannot read logs is a real failure, not an
// unknown Pod.
func (s *KubeletServer) getContainerLogs(
	ctx context.Context, namespace, podName, containerName string, opts vkapi.ContainerLogOpts,
) (io.ReadCloser, error) {
	h := s.handlerFor(namespace, podName)
	if h == nil {
		return nil, errdefs.NotFoundf("no Nebula virtual node is running pod %q", key(namespace, podName))
	}
	return h.GetContainerLogs(ctx, namespace, podName, containerName, opts)
}

// runInContainer serves the exec route, the same way.
func (s *KubeletServer) runInContainer(
	ctx context.Context, namespace, podName, containerName string, cmd []string, attach vkapi.AttachIO,
) error {
	h := s.handlerFor(namespace, podName)
	if h == nil {
		return errdefs.NotFoundf("no Nebula virtual node is running pod %q", key(namespace, podName))
	}
	return h.RunInContainer(ctx, namespace, podName, containerName, cmd, attach)
}

// tlsConfig: a fresh self-signed keypair, plus client verification if a CA is set.
func (s *KubeletServer) tlsConfig() (*tls.Config, error) {
	cert, err := selfSignedCert(s.nodeIP)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		// http/1.1 only, like a real kubelet: logs need nothing HTTP/2 offers, and this is
		// the streaming path every kubelet client already exercises.
		NextProtos: []string{"http/1.1"},
	}
	if s.clientCAPath == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(s.clientCAPath)
	if err != nil {
		return nil, fmt.Errorf("kubelet api: read client CA %s: %w", s.clientCAPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("kubelet api: client CA %s contains no certificates", s.clientCAPath)
	}
	cfg.ClientCAs = pool
	cfg.ClientAuth = tls.RequireAndVerifyClientCert
	return cfg, nil
}

// selfSignedCert mints an in-memory serving cert for the advertised IP. The IP is a SAN
// because the API server dials an IP, and a CN-only cert is rejected by any client that
// does verify; loopback is for curl'ing from inside the pod.
func selfSignedCert(nodeIP string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet api: generate key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet api: serial: %w", err)
	}
	now := time.Now()
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "nebula-virtual-kubelet"},
		// Backdated, so minutes of clock skew cannot make a fresh cert look not-yet-valid.
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(certValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP(nodeIP), net.IPv4(127, 0, 0, 1)},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet api: create certificate: %w", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

// portOf pulls the port out of a listen address, as the int32 Node status wants.
func portOf(addr string) (int32, error) {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, fmt.Errorf("kubelet api: parse address %q: %w", addr, err)
	}
	port, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil || port <= 0 || port > 65535 {
		return 0, fmt.Errorf("kubelet api: address %q has no valid port", addr)
	}
	return int32(port), nil
}
