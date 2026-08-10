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
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	utilexec "k8s.io/utils/exec"

	"github.com/InftyAI/SandD/go/controller"
)

// stubExecer stands in for the SandD controller. It records what the relay asked for so a
// test can assert the WIRING — which daemon id was resolved, what command was sent —
// without a controller, a daemon, or a network.
type stubExecer struct {
	mu sync.Mutex

	// result/err are what Exec returns.
	result *controller.ExecResult
	err    error

	// session/sessionErr are what OpenSession returns. The interface, not *stubSession, so
	// a test can script a different PTY shape (see slowSession).
	session    Session
	sessionErr error

	// Recorded inputs.
	daemonID string
	command  string
	rows     uint16
	cols     uint16
}

func (s *stubExecer) Exec(daemonID, command string, _ time.Duration) (*controller.ExecResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.daemonID, s.command = daemonID, command
	if s.err != nil {
		return nil, s.err
	}
	return s.result, nil
}

func (s *stubExecer) OpenSession(daemonID string, rows, cols uint16, _ string) (Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.daemonID, s.rows, s.cols = daemonID, rows, cols
	if s.sessionErr != nil {
		return nil, s.sessionErr
	}
	return s.session, nil
}

// stubSession is a scripted PTY: it yields `output` once, then reports the session ended,
// which is what a shell exiting looks like to the relay.
type stubSession struct {
	mu      sync.Mutex
	output  []byte
	served  bool
	written bytes.Buffer
	// waitFor keeps the shell alive until the relay has delivered these keystrokes, for a
	// test that asserts on stdin. Without it the session ends as soon as its output has been
	// served — which is the correct relay behaviour (stdin EOF must not truncate output) but
	// means the relay may legitimately return before io.Copy has moved a byte, so a stdin
	// assertion would be racy rather than wrong. Empty means end immediately.
	waitFor  string
	idle     int
	resizes  [][2]uint16
	closed   bool
	closeErr error
}

// stubSessionIdleLimit bounds the wait for waitFor so a relay that never delivers stdin
// fails the assertion instead of hanging the package until the test binary's timeout.
const stubSessionIdleLimit = 200

func (s *stubSession) Read(p []byte, _ time.Duration) (int, error) {
	s.mu.Lock()
	if !s.served {
		s.served = true
		n := copy(p, s.output)
		s.mu.Unlock()
		return n, nil
	}
	waiting := s.waitFor != "" &&
		!strings.Contains(s.written.String(), s.waitFor) &&
		s.idle < stubSessionIdleLimit
	if waiting {
		s.idle++
	}
	s.mu.Unlock()

	if waiting {
		// An idle timeout, which the relay treats as a quiet terminal and loops on. Slept
		// outside the lock so the stdin goroutine can take it.
		time.Sleep(time.Millisecond)
		return 0, nil
	}
	return 0, controller.ErrSessionClosed
}

func (s *stubSession) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.written.Write(p)
}

func (s *stubSession) Resize(rows, cols uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resizes = append(s.resizes, [2]uint16{rows, cols})
	return nil
}

func (s *stubSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return s.closeErr
}

func (s *stubSession) wasClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// stdin returns what the relay wrote into the session. An accessor, not a direct read of
// the buffer, because the relay writes from its own goroutine — reading the field would be
// a data race that -race fails on intermittently.
func (s *stubSession) stdin() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.written.String()
}

// stubAttachIO is the caller's end of a kubectl exec stream.
type stubAttachIO struct {
	stdin  io.Reader
	stdout bytes.Buffer
	stderr bytes.Buffer
	tty    bool
	resize chan vkapi.TermSize
}

func newAttachIO(tty bool, stdin string) *stubAttachIO {
	return &stubAttachIO{
		stdin:  bytes.NewReader([]byte(stdin)),
		tty:    tty,
		resize: make(chan vkapi.TermSize, 1),
	}
}

func (a *stubAttachIO) Stdin() io.Reader              { return a.stdin }
func (a *stubAttachIO) Stdout() io.WriteCloser        { return nopWriteCloser{&a.stdout} }
func (a *stubAttachIO) Stderr() io.WriteCloser        { return nopWriteCloser{&a.stderr} }
func (a *stubAttachIO) TTY() bool                     { return a.tty }
func (a *stubAttachIO) Resize() <-chan vkapi.TermSize { return a.resize }

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// trackPod puts a Pod in the handler's tracking map as if CreatePod had provisioned it,
// which is the state every exec depends on.
func trackPod(h *Handler, ns, name, claim string) {
	h.store(testPod(ns, name), claim, "inst-1")
}

func TestRunInContainer_DisabledWithoutExecer(t *testing.T) {
	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	trackPod(h, "default", "p1", "default-p1")

	err := h.RunInContainer(context.Background(), "default", "p1", "main",
		[]string{"ls"}, newAttachIO(false, ""))

	// NotFound, not an internal error: a cluster without the fleet installed is not
	// broken, so kubectl must say the feature is absent rather than that the server failed.
	if !errdefs.IsNotFound(err) {
		t.Fatalf("expected NotFound when no Execer is wired, got %v", err)
	}
}

func TestRunInContainer_UntrackedPodIsNotFound(t *testing.T) {
	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	h.exec = &stubExecer{result: &controller.ExecResult{}}

	err := h.RunInContainer(context.Background(), "default", "ghost", "main",
		[]string{"ls"}, newAttachIO(false, ""))

	if !errdefs.IsNotFound(err) {
		t.Fatalf("expected NotFound for an untracked pod, got %v", err)
	}
}

// The daemon id must be the CLAIM name, because that is the `sub` the manager minted the
// token with (see mintSanddAuth) and therefore the id the controller registered. Resolving
// anything else — the pod name, say — would look correct here and find no daemon in
// production.
func TestRunInContainer_ResolvesClaimNameAsDaemonID(t *testing.T) {
	se := &stubExecer{result: &controller.ExecResult{Stdout: "hi\n"}}
	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	h.exec = se
	trackPod(h, "team-a", "trainer", "team-a-trainer")

	attach := newAttachIO(false, "")
	if err := h.RunInContainer(context.Background(), "team-a", "trainer", "main",
		[]string{"echo", "hi"}, attach); err != nil {
		t.Fatalf("RunInContainer: %v", err)
	}

	if se.daemonID != "team-a-trainer" {
		t.Fatalf("expected daemon id to be the claim name team-a-trainer, got %q", se.daemonID)
	}
	if se.command != "echo hi" {
		t.Fatalf("expected command %q, got %q", "echo hi", se.command)
	}
	if got := attach.stdout.String(); got != "hi\n" {
		t.Fatalf("expected stdout %q, got %q", "hi\n", got)
	}
}

// A non-zero exit must arrive as a utilexec.ExitError carrying the CODE. VK's
// remotecommand layer type-asserts exactly that interface to put the status on the wire;
// any other error shape makes kubectl exit 1 regardless of what the command returned,
// which silently breaks `kubectl exec ... && next`.
func TestRunInContainer_NonZeroExitCarriesCode(t *testing.T) {
	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	h.exec = &stubExecer{result: &controller.ExecResult{Stderr: "nope\n", ExitCode: 42}}
	trackPod(h, "default", "p1", "default-p1")

	attach := newAttachIO(false, "")
	err := h.RunInContainer(context.Background(), "default", "p1", "main",
		[]string{"false"}, attach)
	if err == nil {
		t.Fatal("expected an error for a non-zero exit")
	}

	var exitErr utilexec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("error must implement utilexec.ExitError or kubectl loses the code; got %T", err)
	}
	if exitErr.ExitStatus() != 42 {
		t.Fatalf("expected exit status 42, got %d", exitErr.ExitStatus())
	}
	// stderr must still reach the caller: the command's own diagnostics are the whole
	// reason a user runs it, and a non-zero exit is the case where they matter most.
	if got := attach.stderr.String(); got != "nope\n" {
		t.Fatalf("expected stderr %q, got %q", "nope\n", got)
	}
}

func TestRunInContainer_ZeroExitIsSuccess(t *testing.T) {
	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	h.exec = &stubExecer{result: &controller.ExecResult{Stdout: "ok\n"}}
	trackPod(h, "default", "p1", "default-p1")

	if err := h.RunInContainer(context.Background(), "default", "p1", "main",
		[]string{"true"}, newAttachIO(false, "")); err != nil {
		t.Fatalf("expected success for exit code 0, got %v", err)
	}
}

// A disconnected daemon is the common case (the instance is still booting), so it must map
// to NotFound rather than an internal error — and it must do so via errors.Is, not by
// matching message text.
func TestRunInContainer_NoDaemonIsNotFound(t *testing.T) {
	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	h.exec = &stubExecer{err: fmt.Errorf("exec on default-p1: %w", controller.ErrNoDaemon)}
	trackPod(h, "default", "p1", "default-p1")

	err := h.RunInContainer(context.Background(), "default", "p1", "main",
		[]string{"ls"}, newAttachIO(false, ""))

	if !errdefs.IsNotFound(err) {
		t.Fatalf("expected a wrapped controller.ErrNoDaemon to become NotFound, got %v", err)
	}
}

// A transport failure that is NOT controller.ErrNoDaemon must stay an internal error: reporting a
// broken controller as NotFound would tell the user their pod is missing when the cluster
// is at fault.
func TestRunInContainer_OtherErrorIsNotNotFound(t *testing.T) {
	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	h.exec = &stubExecer{err: errors.New("connection reset by peer")}
	trackPod(h, "default", "p1", "default-p1")

	err := h.RunInContainer(context.Background(), "default", "p1", "main",
		[]string{"ls"}, newAttachIO(false, ""))
	if err == nil {
		t.Fatal("expected an error")
	}
	if errdefs.IsNotFound(err) {
		t.Fatalf("a transport failure must not be reported as NotFound: %v", err)
	}
}

// The interactive path: output reaches the caller, stdin reaches the PTY, and the session
// is closed. Closing matters most — a leaked session leaves a shell running on a billing
// instance.
func TestRunInContainer_TTYRelaysAndClosesSession(t *testing.T) {
	sess := &stubSession{output: []byte("root@box:~# ")}
	se := &stubExecer{session: sess}
	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	h.exec = se
	trackPod(h, "default", "p1", "default-p1")

	attach := newAttachIO(true, "")
	if err := h.RunInContainer(context.Background(), "default", "p1", "main",
		[]string{"bash"}, attach); err != nil {
		t.Fatalf("RunInContainer: %v", err)
	}

	if got := attach.stdout.String(); got != "root@box:~# " {
		t.Fatalf("expected PTY output relayed to stdout, got %q", got)
	}
	if !sess.wasClosed() {
		t.Fatal("session must be closed, or a shell keeps running on a paid instance")
	}
	sess.mu.Lock()
	written := sess.written.String()
	sess.mu.Unlock()
	if written != "bash\n" {
		t.Fatalf("expected the command written into the PTY, got %q", written)
	}
}

// An initial terminal size already on the channel must be used when opening the session,
// so the shell does not start at 24x80 and visibly reflow.
func TestRunInContainer_TTYUsesInitialTerminalSize(t *testing.T) {
	sess := &stubSession{}
	se := &stubExecer{session: sess}
	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	h.exec = se
	trackPod(h, "default", "p1", "default-p1")

	attach := newAttachIO(true, "")
	attach.resize <- vkapi.TermSize{Height: 50, Width: 200}

	if err := h.RunInContainer(context.Background(), "default", "p1", "main",
		nil, attach); err != nil {
		t.Fatalf("RunInContainer: %v", err)
	}

	if se.rows != 50 || se.cols != 200 {
		t.Fatalf("expected session opened at 50x200, got %dx%d", se.rows, se.cols)
	}
}

// Stdin reaching EOF must NOT end the session before output is relayed. `echo hi |
// kubectl exec -i pod -- cat` closes stdin at once, so ending on that would truncate the
// very output the user asked for. This is a regression test: the relay originally raced
// stdin against output and lost.
func TestRunInContainer_StdinEOFDoesNotTruncateOutput(t *testing.T) {
	// A session that only produces output on its SECOND read, so a relay that returns as
	// soon as stdin is exhausted cannot pass by accident.
	sess := &slowSession{out: []byte("hi\n"), delayReads: 1}
	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	h.exec = &stubExecer{session: sess}
	trackPod(h, "default", "p1", "default-p1")

	// Non-empty stdin that ends immediately — the pipe case.
	attach := newAttachIO(true, "hi\n")
	if err := h.RunInContainer(context.Background(), "default", "p1", "main",
		[]string{"cat"}, attach); err != nil {
		t.Fatalf("RunInContainer: %v", err)
	}

	if got := attach.stdout.String(); got != "hi\n" {
		t.Fatalf("output produced after stdin EOF must still be relayed; got %q", got)
	}
}

// slowSession withholds output for the first delayReads calls, then yields it once and
// reports the session ended.
type slowSession struct {
	mu         sync.Mutex
	out        []byte
	delayReads int
	reads      int
	served     bool
}

func (s *slowSession) Read(p []byte, _ time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reads < s.delayReads {
		s.reads++
		return 0, nil // idle timeout: a quiet terminal, not an error
	}
	if !s.served {
		s.served = true
		return copy(p, s.out), nil
	}
	return 0, controller.ErrSessionClosed
}

func (s *slowSession) Write(p []byte) (int, error) { return len(p), nil }
func (s *slowSession) Resize(_, _ uint16) error    { return nil }
func (s *slowSession) Close() error                { return nil }

func TestRunInContainer_TTYOpenFailureMapsNoDaemon(t *testing.T) {
	h := NewHandler(&fakeProvider{}, nil, nil, nil)
	h.exec = &stubExecer{sessionErr: fmt.Errorf("open session: %w", controller.ErrNoDaemon)}
	trackPod(h, "default", "p1", "default-p1")

	err := h.RunInContainer(context.Background(), "default", "p1", "main",
		[]string{"bash"}, newAttachIO(true, ""))

	if !errdefs.IsNotFound(err) {
		t.Fatalf("expected NotFound when the daemon is not connected, got %v", err)
	}
}
