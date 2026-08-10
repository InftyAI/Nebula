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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/InftyAI/SandD/go/controller"
)

// execServer starts the kubelet API for a handler wired to se and returns its base URL plus
// a client trusting the self-signed cert. These tests go through the real HTTP path because
// the leg between the API server and RunInContainer is what a handler-only test can't cover.
func execServer(t *testing.T, se Execer) (string, *http.Client) {
	t.Helper()

	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	h.exec = se

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	port, err := serveKubeletAPI(ctx, h, "nebula-test")
	if err != nil {
		t.Fatalf("serveKubeletAPI: %v", err)
	}
	trackPod(h, "team-a", "trainer", "team-a-trainer")

	// gosec: self-signed per process, nothing to pin; TestNewServingCert checks the cert.
	tlsCfg := &tls.Config{InsecureSkipVerify: true} //nolint:gosec
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
		Timeout:   10 * time.Second,
	}

	base := fmt.Sprintf("https://127.0.0.1:%d", port)
	waitForServer(t, base, client)
	return base, client
}

// waitForServer polls until the listener answers: serveKubeletAPI returns once the port is
// bound but serves from a goroutine.
func waitForServer(t *testing.T, base string, client *http.Client) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := client.Get(base + "/")
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("kubelet API never came up: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// execStream drives the exec route the way the API server does: a SPDY upgrade, then
// streams. A plain POST would be answered 400 by ServeExec before reaching the handler, so
// it could only prove the router exists. SPDY specifically because that is the only upgrade
// VK 1.11's stream layer implements.
func execStream(t *testing.T, base, path string, opts remotecommand.StreamOptions) error {
	t.Helper()
	u, err := url.Parse(base + path)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	exec, err := remotecommand.NewSPDYExecutor(
		&rest.Config{Host: base, TLSClientConfig: rest.TLSClientConfig{Insecure: true}}, "POST", u)
	if err != nil {
		t.Fatalf("NewSPDYExecutor: %v", err)
	}
	return exec.StreamWithContext(context.Background(), opts)
}

// TestServeKubeletAPI_ExecRouteReachesTheHandler is the point of the file: if this passes,
// `kubectl exec` works end to end given a connected daemon; if it fails, exec is
// unreachable no matter how correct exec.go is.
func TestServeKubeletAPI_ExecRouteReachesTheHandler(t *testing.T) {
	se := &stubExecer{result: &controller.ExecResult{Stdout: "hi\n"}}
	base, _ := execServer(t, se)

	// The API server's URL shape, spelled out literally so a route change shows up here.
	var stdout bytes.Buffer
	err := execStream(t, base, "/exec/team-a/trainer/main?command=echo&command=hi&output=1&error=1",
		remotecommand.StreamOptions{Stdout: &stdout, Stderr: io.Discard})
	if err != nil {
		t.Fatalf("exec stream: %v", err)
	}

	if se.daemonID != "team-a-trainer" {
		t.Errorf("exec did not reach the handler: daemonID = %q, want team-a-trainer", se.daemonID)
	}
	if se.command != "echo hi" {
		t.Errorf("command = %q, want %q — argv was not reassembled from the query", se.command, "echo hi")
	}
	// The output must come back over the stream, not merely be produced.
	if got := stdout.String(); got != "hi\n" {
		t.Errorf("stdout = %q, want %q streamed back to the client", got, "hi\n")
	}
}

// TestServeKubeletAPI_UnknownPodFailsWithAnExplanation asserts the MESSAGE, not a 404 or
// errdefs.IsNotFound: the stream is upgraded (101) before the handler runs, and VK's
// ServeExec then wraps every non-ExitError as a generic InternalError, discarding the
// errdefs class exec.go sets. Prose is the only diagnostic that reaches an operator, which
// is why it must stay specific.
func TestServeKubeletAPI_UnknownPodFailsWithAnExplanation(t *testing.T) {
	base, _ := execServer(t, &stubExecer{result: &controller.ExecResult{}})

	err := execStream(t, base, "/exec/team-a/ghost/main?command=ls&output=1",
		remotecommand.StreamOptions{Stdout: io.Discard})
	if err == nil {
		t.Fatal("expected an error for a pod this node does not track")
	}
	// Naming the pod is what makes the failure actionable.
	for _, want := range []string{"team-a/ghost", "not tracked"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q for debuggability, got: %v", want, err)
		}
	}
}

// TestServeKubeletAPI_InteractiveTTYRoundTrips covers `kubectl exec -it`: a PTY with stdin
// and stdout live at once, a different path from the case above, and the one where a relay
// bug shows up as a hung terminal rather than a failed request.
func TestServeKubeletAPI_InteractiveTTYRoundTrips(t *testing.T) {
	// waitFor holds the shell open until the keystrokes land: the relay may return as soon as
	// output ends, so without it stdin might legitimately not have been copied yet.
	sess := &stubSession{output: []byte("root@box:~# "), waitFor: "whoami"}
	se := &stubExecer{session: sess}
	base, _ := execServer(t, se)

	var stdout bytes.Buffer
	err := execStream(t, base, "/exec/team-a/trainer/main?command=sh&input=1&output=1&tty=1",
		remotecommand.StreamOptions{
			Stdin:  strings.NewReader("whoami\n"),
			Stdout: &stdout,
			Tty:    true,
		})
	if err != nil {
		t.Fatalf("interactive exec: %v", err)
	}

	if got := stdout.String(); !strings.Contains(got, "root@box") {
		t.Errorf("stdout = %q, want the session's output relayed back", got)
	}
	if got := sess.stdin(); !strings.Contains(got, "whoami") {
		t.Errorf("session stdin = %q, want the client's keystrokes relayed in", got)
	}
	// Not closing leaks a PTY on the instance per exec.
	if !sess.wasClosed() {
		t.Error("session was not closed after the stream ended")
	}
}

// TestServeKubeletAPI_UnwiredRoutesAreNotImplemented: nil PodHandlerConfig fields must give
// 501, which says "this node cannot" rather than "your pod does not exist".
func TestServeKubeletAPI_UnwiredRoutesAreNotImplemented(t *testing.T) {
	base, client := execServer(t, &stubExecer{result: &controller.ExecResult{}})

	for _, path := range []string{
		"/containerLogs/team-a/trainer/main",
		"/attach/team-a/trainer/main",
	} {
		resp, err := client.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotImplemented {
			t.Errorf("%s: status = %d, want 501", path, resp.StatusCode)
		}
	}
}

// TestServeKubeletAPI_StopsWithTheContext: a manager that lost leader election must not keep
// an unauthenticated exec port open while another replica serves the same nodes.
func TestServeKubeletAPI_StopsWithTheContext(t *testing.T) {
	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	h.exec = &stubExecer{result: &controller.ExecResult{}}

	ctx, cancel := context.WithCancel(context.Background())
	port, err := serveKubeletAPI(ctx, h, "nebula-test")
	if err != nil {
		t.Fatalf("serveKubeletAPI: %v", err)
	}
	cancel()

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(5 * time.Second)
	for {
		// Retried: Close races the cancel goroutine by a hair.
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			return
		}
		_ = conn.Close()
		if time.Now().After(deadline) {
			t.Fatal("listener still accepting connections after the context was cancelled")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestServeKubeletAPI_PortsAreDistinctPerNode guards why the port is ephemeral: one process
// hosts a node per provider, and on a fixed port the second would fail to bind.
func TestServeKubeletAPI_PortsAreDistinctPerNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seen := map[int]bool{}
	for _, name := range []string{"nebula-aws", "nebula-modal"} {
		h := NewHandler(&fakeProvider{}, nil, nil, nil)
		h.exec = &stubExecer{result: &controller.ExecResult{}}
		port, err := serveKubeletAPI(ctx, h, name)
		if err != nil {
			t.Fatalf("serveKubeletAPI(%s): %v", name, err)
		}
		if port == 0 {
			t.Fatalf("%s: port 0 was returned, which would advertise no endpoint", name)
		}
		if seen[port] {
			t.Fatalf("%s: port %d collides with another node's", name, port)
		}
		seen[port] = true
	}
}

// TestNewServingCert_CoversTheDialedAddress: the API server dials the advertised IP, so a
// CN-only cert fails in any cluster using --kubelet-certificate-authority.
func TestNewServingCert_CoversTheDialedAddress(t *testing.T) {
	cert, err := newServingCert("nebula-test")
	if err != nil {
		t.Fatalf("newServingCert: %v", err)
	}
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if err := parsed.VerifyHostname("127.0.0.1"); err != nil {
		t.Errorf("cert does not cover the loopback address: %v", err)
	}
	if len(parsed.IPAddresses) < 2 {
		t.Errorf("expected the host's routable IPs in the SAN, got %v", parsed.IPAddresses)
	}
	// A wrong EKU is rejected even when the name matches.
	var serverAuth bool
	for _, u := range parsed.ExtKeyUsage {
		if u == x509.ExtKeyUsageServerAuth {
			serverAuth = true
		}
	}
	if !serverAuth {
		t.Error("cert is missing the ServerAuth EKU")
	}
	// Backdated, or a client with a lagging clock rejects it.
	if !parsed.NotBefore.Before(time.Now()) {
		t.Errorf("NotBefore %v is not in the past", parsed.NotBefore)
	}
}

// TestNodeSpec_AdvertisesTheKubeletEndpoint: the two fields that tell the API server where
// to proxy an exec.
func TestNodeSpec_AdvertisesTheKubeletEndpoint(t *testing.T) {
	t.Setenv("POD_IP", "10.1.2.3")

	n := nodeSpec("nebula-aws", "aws", 45678)

	if got := n.Status.DaemonEndpoints.KubeletEndpoint.Port; got != 45678 {
		t.Errorf("kubelet endpoint port = %d, want the port actually bound (45678)", got)
	}
	// InternalIP and nothing else: a Hostname entry would win over the IP (the API server
	// walks address TYPES, Hostname first) and "nebula-aws" resolves nowhere.
	want := []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.1.2.3"}}
	if len(n.Status.Addresses) != 1 || n.Status.Addresses[0] != want[0] {
		t.Errorf("addresses = %+v, want exactly %+v", n.Status.Addresses, want)
	}
}

// TestNodeSpec_WithoutPodIPAdvertisesNoRoutableAddress covers `make run` from a laptop: the
// node must not invent an IP, or a clear failure becomes a hang.
func TestNodeSpec_WithoutPodIPAdvertisesNoRoutableAddress(t *testing.T) {
	t.Setenv("POD_IP", "")

	n := nodeSpec("nebula-aws", "aws", 45678)

	for _, a := range n.Status.Addresses {
		if a.Type == corev1.NodeInternalIP {
			t.Errorf("advertised an InternalIP %q with POD_IP unset", a.Address)
		}
	}
}
