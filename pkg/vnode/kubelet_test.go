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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	utilexec "k8s.io/utils/exec"
)

func TestNewKubeletServer_Validation(t *testing.T) {
	cases := []struct {
		name   string
		nodeIP string
		addr   string
		wantOK bool
	}{
		// The empty addr is the common case: cmd passes the flag default through.
		{name: "defaults the address", nodeIP: "10.0.0.5", addr: "", wantOK: true},
		{name: "explicit host and port", nodeIP: "10.0.0.5", addr: "127.0.0.1:10250", wantOK: true},
		// No POD_IP means nothing to advertise; a node claiming an endpoint at "" would
		// fail every `kubectl logs` with a confusing dial error.
		{name: "missing node IP", nodeIP: "", addr: ":10250", wantOK: false},
		// A hostname is unusable: the cert's SANs and the InternalIP both need a literal.
		{name: "node IP is a hostname", nodeIP: "nebula.local", addr: ":10250", wantOK: false},
		// Port problems must fail at startup, since the port is published on the Node.
		{name: "no port", nodeIP: "10.0.0.5", addr: "10.0.0.5", wantOK: false},
		{name: "non-numeric port", nodeIP: "10.0.0.5", addr: ":kubelet", wantOK: false},
		{name: "port out of range", nodeIP: "10.0.0.5", addr: ":70000", wantOK: false},
		{name: "port zero", nodeIP: "10.0.0.5", addr: ":0", wantOK: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewKubeletServer(tc.nodeIP, tc.addr, "")
			if tc.wantOK != (err == nil) {
				t.Fatalf("NewKubeletServer(%q, %q) err = %v, wantOK %v", tc.nodeIP, tc.addr, err, tc.wantOK)
			}
			if tc.wantOK && s.addr == "" {
				t.Fatal("addr is empty; the default should have been applied")
			}
		})
	}
}

// The subtlest failure here: --kubelet-preferred-address-types defaults to Hostname
// FIRST, so advertising a Hostname too would have the API server resolve
// "nebula-modal" in DNS and fail before trying the IP. InternalIP alone is reachable.
func TestKubeletServer_AdvertisesInternalIPOnly(t *testing.T) {
	s, err := NewKubeletServer("10.244.1.7", ":10250", "")
	if err != nil {
		t.Fatalf("NewKubeletServer: %v", err)
	}

	addrs := s.nodeAddress()
	if len(addrs) != 1 {
		t.Fatalf("addresses = %+v, want exactly one", addrs)
	}
	if addrs[0].Type != corev1.NodeInternalIP {
		t.Fatalf("address type = %q, want InternalIP only", addrs[0].Type)
	}
	if addrs[0].Address != "10.244.1.7" {
		t.Fatalf("address = %q, want the manager Pod IP", addrs[0].Address)
	}
	if got := s.daemonEndpoints().KubeletEndpoint.Port; got != 10250 {
		t.Fatalf("kubelet endpoint port = %d, want the listen port 10250", got)
	}
}

// The end-to-end shape of a `kubectl logs`: an HTTPS GET on the containerLogs route.
// Covers what unit tests cannot — the route is attached, TLS serves, and the reply is
// the provider's bytes.
func TestKubeletServer_ServesLogsOverTLS(t *testing.T) {
	lp := newLoggingProvider(&fakeProvider{provisionID: "inst-1"}, "hello over tls\n")
	h := NewHandler(lp, nil, nil, openCluster())
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	s, base := startTestKubeletServer(t, map[string]*Handler{"nebula-fake": h})

	body, code := getLogs(t, base, "default", "p1", "main")
	if code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", code, body)
	}
	if body != "hello over tls\n" {
		t.Fatalf("body = %q", body)
	}
	if lp.askedFor != "inst-1" {
		t.Fatalf("provider asked for %q, want inst-1", lp.askedFor)
	}

	// A Pod nobody tracks: 404, not a 500 the user is invited to retry.
	if body, code := getLogs(t, base, "default", "ghost", "main"); code != http.StatusNotFound {
		t.Fatalf("unknown pod: status = %d, body = %q, want 404", code, body)
	}

	// No handlers at all (leadership lost, Runners not started): still a clean 404.
	s.mu.Lock()
	s.handlers = map[string]*Handler{}
	s.mu.Unlock()
	if body, code := getLogs(t, base, "default", "p1", "main"); code != http.StatusNotFound {
		t.Fatalf("no handlers: status = %d, body = %q, want 404", code, body)
	}
}

// The end-to-end shape of a `kubectl exec`: the API server's own SPDY client against the
// exec route. Covers what unit tests cannot — the route is attached, the streams are
// negotiated, and the command's output and exit code both survive the wire.
func TestKubeletServer_ServesExecOverTLS(t *testing.T) {
	ep := newExecProvider(&fakeProvider{provisionID: "inst-1"}, newExecProcess("hi from exec\n", "", 0))
	h := NewHandler(ep, nil, nil, openCluster())
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	_, base := startTestKubeletServer(t, map[string]*Handler{"nebula-fake": h})

	stdout, err := execCommand(t, base, "default", "p1", []string{"echo", "hi"})
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if stdout != "hi from exec\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	if ep.askedFor != "inst-1" {
		t.Fatalf("provider asked for %q, want inst-1", ep.askedFor)
	}

	// A Pod nobody tracks fails, rather than exec'ing into someone else's instance.
	if _, err := execCommand(t, base, "default", "ghost", []string{"echo", "hi"}); err == nil {
		t.Fatal("unknown pod: expected an error")
	}
}

// The exit code is the one thing an exec must not lose: a failed command has to look
// failed to the client, with its own status, not like a broken kubelet.
func TestKubeletServer_ExecReportsExitCode(t *testing.T) {
	ep := newExecProvider(&fakeProvider{provisionID: "inst-1"}, newExecProcess("", "boom\n", 3))
	h := NewHandler(ep, nil, nil, openCluster())
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	_, base := startTestKubeletServer(t, map[string]*Handler{"nebula-fake": h})

	_, err := execCommand(t, base, "default", "p1", []string{"false"})
	var exitErr utilexec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("err = %v (%T), want an exit error the client can read a code from", err, err)
	}
	if exitErr.ExitStatus() != 3 {
		t.Fatalf("exit status = %d, want 3", exitErr.ExitStatus())
	}
}

// execCommand runs one command through the exec route the way the API server does,
// returning what the command wrote to stdout.
func execCommand(t *testing.T, base, namespace, pod string, cmd []string) (string, error) {
	t.Helper()
	u, err := url.Parse(fmt.Sprintf("%s/exec/%s/%s/main", base, url.PathEscape(namespace), url.PathEscape(pod)))
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	q := url.Values{"command": cmd, "output": {"1"}, "error": {"1"}}
	u.RawQuery = q.Encode()

	// Self-signed, like a real kubelet's; the API server does not verify it either.
	cfg := &rest.Config{Host: base, TLSClientConfig: rest.TLSClientConfig{Insecure: true}}
	exec, err := remotecommand.NewSPDYExecutor(cfg, http.MethodPost, u)
	if err != nil {
		t.Fatalf("NewSPDYExecutor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	return stdout.String(), err
}

// One listener serves every provider and the route carries no node name, so a request
// must find the ONE Handler tracking that Pod.
func TestKubeletServer_ResolvesAcrossProviders(t *testing.T) {
	modalish := newLoggingProvider(&fakeProvider{provisionID: "sb-1"}, "from modal\n")
	awsish := newLoggingProvider(&fakeProvider{provisionID: "i-1"}, "from aws\n")
	hModal, hAWS := NewHandler(modalish, nil, nil, openCluster()), NewHandler(awsish, nil, nil, openCluster())
	if err := hModal.CreatePod(context.Background(), testPod("default", "on-modal")); err != nil {
		t.Fatalf("CreatePod(modal): %v", err)
	}
	if err := hAWS.CreatePod(context.Background(), testPod("default", "on-aws")); err != nil {
		t.Fatalf("CreatePod(aws): %v", err)
	}

	_, base := startTestKubeletServer(t, map[string]*Handler{"nebula-modal": hModal, "nebula-aws": hAWS})

	for _, tc := range []struct{ pod, want string }{
		{pod: "on-modal", want: "from modal\n"},
		{pod: "on-aws", want: "from aws\n"},
	} {
		body, code := getLogs(t, base, "default", tc.pod, "main")
		if code != http.StatusOK || body != tc.want {
			t.Fatalf("pod %q: status = %d, body = %q, want %q", tc.pod, code, body, tc.want)
		}
	}
}

// The walk treats NotFound as "not my pod", so a real error must break out — otherwise
// a broken provider reads as an unknown pod and the cause is hidden.
func TestKubeletServer_ProviderErrorIsNotSwallowed(t *testing.T) {
	broken := newLoggingProvider(&fakeProvider{provisionID: "inst-1"}, "")
	broken.logsErr = fmt.Errorf("provider API unreachable")
	h := NewHandler(broken, nil, nil, openCluster())
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	_, base := startTestKubeletServer(t, map[string]*Handler{"nebula-fake": h})

	body, code := getLogs(t, base, "default", "p1", "main")
	if code == http.StatusNotFound || code == http.StatusOK {
		t.Fatalf("status = %d, body = %q, want a server error rather than 404/200", code, body)
	}
}

// As a manager.Runnable, Start must return on shutdown instead of holding the process
// open.
func TestKubeletServer_StopsWithContext(t *testing.T) {
	s, err := NewKubeletServer("127.0.0.1", freeAddr(t), "")
	if err != nil {
		t.Fatalf("NewKubeletServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Start(ctx) }()
	waitForListener(t, s.addr)

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start returned %v, want nil on a clean shutdown", err)
		}
	case <-time.After(kubeletShutdownGrace + 5*time.Second):
		t.Fatal("Start did not return after the context was cancelled")
	}
}

// An addr in use must fail loudly: continuing would leave nodes advertising a port
// nothing serves.
func TestKubeletServer_ListenFailureSurfaces(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = taken.Close() }()

	s, err := NewKubeletServer("127.0.0.1", taken.Addr().String(), "")
	if err != nil {
		t.Fatalf("NewKubeletServer: %v", err)
	}
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("Start: expected an error when the address is already in use")
	}
}

// --kubelet-client-ca is the opt-in to mTLS, so a bad path or an empty bundle must stop
// startup — serving without it would turn a request to lock the port down into an open
// one.
func TestKubeletServer_ClientCAErrorsAreFatal(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		s, err := NewKubeletServer("127.0.0.1", freeAddr(t), t.TempDir()+"/absent.pem")
		if err != nil {
			t.Fatalf("NewKubeletServer: %v", err)
		}
		if _, err := s.tlsConfig(); err == nil {
			t.Fatal("expected an error for a client CA path that does not exist")
		}
	})

	t.Run("file without certificates", func(t *testing.T) {
		path := t.TempDir() + "/junk.pem"
		if err := os.WriteFile(path, []byte("not a certificate\n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		s, err := NewKubeletServer("127.0.0.1", freeAddr(t), path)
		if err != nil {
			t.Fatalf("NewKubeletServer: %v", err)
		}
		if _, err := s.tlsConfig(); err == nil {
			t.Fatal("expected an error for a client CA file containing no certificates")
		}
	})

	// No CA means verification OFF: the documented default that keeps logs working on
	// managed control planes.
	t.Run("no CA leaves client auth off", func(t *testing.T) {
		s, err := NewKubeletServer("127.0.0.1", freeAddr(t), "")
		if err != nil {
			t.Fatalf("NewKubeletServer: %v", err)
		}
		cfg, err := s.tlsConfig()
		if err != nil {
			t.Fatalf("tlsConfig: %v", err)
		}
		if cfg.ClientAuth != tls.NoClientCert {
			t.Fatalf("ClientAuth = %v, want NoClientCert by default", cfg.ClientAuth)
		}
	})
}

// The API server dials an IP, so a cert missing that IP SAN is rejected by any client
// that verifies (--kubelet-certificate-authority).
func TestSelfSignedCert_HasIPSAN(t *testing.T) {
	cert, err := selfSignedCert("10.244.1.7")
	if err != nil {
		t.Fatalf("selfSignedCert: %v", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := leaf.VerifyHostname("10.244.1.7"); err != nil {
		t.Fatalf("cert does not cover the advertised IP: %v", err)
	}
	// Loopback is there for curl'ing from inside the pod.
	if err := leaf.VerifyHostname("127.0.0.1"); err != nil {
		t.Fatalf("cert does not cover loopback: %v", err)
	}
	// Backdated, so clock skew cannot make a fresh cert look not-yet-valid.
	if !leaf.NotBefore.Before(time.Now()) {
		t.Fatalf("NotBefore = %v, want it backdated", leaf.NotBefore)
	}
}

// startTestKubeletServer runs a server on a loopback port, returning it and the base
// URL to dial.
func startTestKubeletServer(t *testing.T, handlers map[string]*Handler) (*KubeletServer, string) {
	t.Helper()
	s, err := NewKubeletServer("127.0.0.1", freeAddr(t), "")
	if err != nil {
		t.Fatalf("NewKubeletServer: %v", err)
	}
	for name, h := range handlers {
		s.Register(name, h)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("Start: %v", err)
			}
		case <-time.After(kubeletShutdownGrace + 5*time.Second):
			t.Error("kubelet server did not stop")
		}
	})

	waitForListener(t, s.addr)
	return s, "https://" + s.addr
}

// getLogs performs the GET the API server would, returning body and status.
func getLogs(t *testing.T, base, namespace, pod, container string) (string, int) {
	t.Helper()
	client := &http.Client{
		// Self-signed, like a real kubelet's; the API server does not verify it either.
		// TestSelfSignedCert_HasIPSAN covers the cert's contents.
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec
		Timeout:   10 * time.Second,
	}
	u := fmt.Sprintf("%s/containerLogs/%s/%s/%s",
		base, url.PathEscape(namespace), url.PathEscape(pod), url.PathEscape(container))
	resp, err := client.Get(u)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(body), resp.StatusCode
}

// freeAddr picks a loopback port no other test in the package is using.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

func waitForListener(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("nothing listening on %s", addr)
}
