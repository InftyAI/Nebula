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
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	utilexec "k8s.io/utils/exec"

	"github.com/InftyAI/Nebula/pkg/provider"
)

// --- fakes ------------------------------------------------------------------

// syncBuffer is a writable sink the test reads while copy goroutines write it.
type syncBuffer struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	closed chan struct{}
	once   sync.Once
}

func newSyncBuffer() *syncBuffer { return &syncBuffer{closed: make(chan struct{})} }

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// execProcess is a fake provider.Process. wait, when set, is what the command waits on
// before exiting, so a test decides when it ends.
type execProcess struct {
	stdin   *syncBuffer
	stdout  io.Reader
	stderr  io.Reader
	code    int
	waitErr error
	wait    <-chan struct{}

	waitBounded bool

	closed chan struct{}
	once   sync.Once
}

func newExecProcess(stdout, stderr string, code int) *execProcess {
	p := &execProcess{stdin: newSyncBuffer(), code: code, closed: make(chan struct{})}
	if stdout != "" {
		p.stdout = strings.NewReader(stdout)
	}
	if stderr != "" {
		p.stderr = strings.NewReader(stderr)
	}
	return p
}

func (p *execProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *execProcess) Stdout() io.Reader     { return p.stdout }
func (p *execProcess) Stderr() io.Reader     { return p.stderr }

func (p *execProcess) Wait(ctx context.Context) (int, error) {
	_, p.waitBounded = ctx.Deadline()
	if p.wait != nil {
		select {
		case <-p.wait:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return p.code, p.waitErr
}

func (p *execProcess) Close() error {
	p.once.Do(func() { close(p.closed) })
	return nil
}

// execProvider is a fakeProvider that also serves exec, and records what it was asked.
type execProvider struct {
	*fakeProvider
	proc     *execProcess
	execErr  error
	askedFor string
	askedCmd []string
	askedTTY bool

	startDeadline time.Time
	startBounded  bool
}

func newExecProvider(fp *fakeProvider, proc *execProcess) *execProvider {
	return &execProvider{fakeProvider: fp, proc: proc}
}

func (p *execProvider) Exec(
	ctx context.Context, instanceID string, cmd []string, opts provider.ExecOptions,
) (provider.Process, error) {
	p.askedFor, p.askedCmd, p.askedTTY = instanceID, cmd, opts.TTY
	p.startDeadline, p.startBounded = ctx.Deadline()
	if p.execErr != nil {
		return nil, p.execErr
	}
	return p.proc, nil
}

// fakeAttach is the client side of an exec: the streams kubectl would have opened. A nil
// stream means the client did not ask for it.
type fakeAttach struct {
	stdin  io.Reader
	stdout io.WriteCloser
	stderr io.WriteCloser
	tty    bool
	resize chan vkapi.TermSize
}

func (a *fakeAttach) Stdin() io.Reader              { return a.stdin }
func (a *fakeAttach) Stdout() io.WriteCloser        { return a.stdout }
func (a *fakeAttach) Stderr() io.WriteCloser        { return a.stderr }
func (a *fakeAttach) TTY() bool                     { return a.tty }
func (a *fakeAttach) Resize() <-chan vkapi.TermSize { return a.resize }

// --- runExec ----------------------------------------------------------------

// The ordinary case: both output streams reach the client and a clean exit is no error.
func TestRunExec_CopiesOutput(t *testing.T) {
	proc := newExecProcess("on stdout\n", "on stderr\n", 0)
	out, errOut := newSyncBuffer(), newSyncBuffer()

	if err := runExec(context.Background(), proc, &fakeAttach{stdout: out, stderr: errOut}); err != nil {
		t.Fatalf("runExec: %v", err)
	}
	if out.String() != "on stdout\n" {
		t.Errorf("stdout = %q", out.String())
	}
	if errOut.String() != "on stderr\n" {
		t.Errorf("stderr = %q", errOut.String())
	}
	// The provider transport must be released, or a disconnected client leaks a stream.
	select {
	case <-proc.closed:
	case <-time.After(2 * time.Second):
		t.Error("process was not closed after the exec finished")
	}
}

// A non-zero exit is the command's own answer, and has to reach kubectl AS an exit code:
// only a typed exit error becomes "command terminated with exit code N" instead of a 500.
func TestRunExec_NonZeroExitIsAnExitError(t *testing.T) {
	proc := newExecProcess("", "no such file\n", 2)
	out := newSyncBuffer()

	err := runExec(context.Background(), proc, &fakeAttach{stdout: out, stderr: newSyncBuffer()})
	if err == nil {
		t.Fatal("runExec: expected an error for a non-zero exit")
	}
	var exitErr utilexec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("err = %v (%T), want a utilexec.ExitError", err, err)
	}
	if !exitErr.Exited() || exitErr.ExitStatus() != 2 {
		t.Fatalf("exit status = %d, want 2", exitErr.ExitStatus())
	}
}

// Not knowing how the command ended is a different failure from it ending badly, and must
// NOT be reported as an exit code the command never returned.
func TestRunExec_WaitErrorIsNotAnExitError(t *testing.T) {
	proc := newExecProcess("", "", 0)
	proc.waitErr = errors.New("command router unreachable")

	err := runExec(context.Background(), proc, &fakeAttach{stdout: newSyncBuffer()})
	if err == nil {
		t.Fatal("runExec: expected the wait failure to surface")
	}
	var exitErr utilexec.ExitError
	if errors.As(err, &exitErr) {
		t.Fatalf("err = %v, want a plain error rather than an exit status", err)
	}
}

// Stdin is forwarded, and CLOSED at EOF — that close is what makes
// `kubectl exec -i -- cat < file` end instead of hanging forever.
func TestRunExec_ForwardsStdinAndClosesIt(t *testing.T) {
	proc := newExecProcess("", "", 0)
	// The command exits when its stdin closes, like `cat`.
	proc.wait = proc.stdin.closed

	err := runExec(context.Background(), proc, &fakeAttach{
		stdin:  strings.NewReader("hello\n"),
		stdout: newSyncBuffer(),
	})
	if err != nil {
		t.Fatalf("runExec: %v", err)
	}
	if proc.stdin.String() != "hello\n" {
		t.Fatalf("stdin = %q, want it forwarded verbatim", proc.stdin.String())
	}
}

// `kubectl exec -it`: the terminal carries one stream, so the provider reports no stderr
// and the client asked for none. Neither may be treated as a missing stream to copy.
func TestRunExec_TTYWithoutStderr(t *testing.T) {
	proc := newExecProcess("prompt$ ", "", 0)
	out := newSyncBuffer()

	attach := &fakeAttach{stdout: out, tty: true, resize: make(chan vkapi.TermSize, 1)}
	// kubectl sends the window size immediately; nothing consumes it downstream, but a
	// dropped resize must not stall the exec.
	attach.resize <- vkapi.TermSize{Width: 80, Height: 24}

	if err := runExec(context.Background(), proc, attach); err != nil {
		t.Fatalf("runExec: %v", err)
	}
	if out.String() != "prompt$ " {
		t.Fatalf("stdout = %q", out.String())
	}
}

// A client that disconnects mid-command (ctx cancelled) must release the provider stream
// rather than leave it running for the rest of the manager's life.
func TestRunExec_CancelReleasesTheProcess(t *testing.T) {
	proc := newExecProcess("", "", 0)
	proc.wait = make(chan struct{}) // never exits on its own

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runExec(ctx, proc, &fakeAttach{stdout: newSyncBuffer()}) }()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runExec did not return after the context was cancelled")
	}
	select {
	case <-proc.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("process was not closed after cancellation")
	}
}

// --- RunInContainer ---------------------------------------------------------

// The whole path: Pod → the instance it provisioned → a command run there. The instance
// id is the assertion that matters; a wrong one would shell into another tenant's box.
func TestRunInContainer_RunsInTrackedInstance(t *testing.T) {
	proc := newExecProcess("root@sandbox:/#\n", "", 0)
	ep := newExecProvider(&fakeProvider{provisionID: "inst-1"}, proc)
	h := NewHandler(ep, nil, nil, openCluster())
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	out := newSyncBuffer()
	attach := &fakeAttach{stdout: out, tty: true}
	err := h.RunInContainer(context.Background(), "default", "p1", "main", []string{"bash"}, attach)
	if err != nil {
		t.Fatalf("RunInContainer: %v", err)
	}
	if ep.askedFor != "inst-1" {
		t.Errorf("provider asked for instance %q, want inst-1", ep.askedFor)
	}
	if len(ep.askedCmd) != 1 || ep.askedCmd[0] != "bash" {
		t.Errorf("command = %v, want it passed through", ep.askedCmd)
	}
	if !ep.askedTTY {
		t.Error("TTY was not passed to the provider")
	}
	if out.String() != "root@sandbox:/#\n" {
		t.Errorf("stdout = %q", out.String())
	}
}

// One instance, one container: `-c whatever` must not fail on a name with nothing behind
// it, exactly as for logs.
func TestRunInContainer_IgnoresContainerName(t *testing.T) {
	for _, container := range []string{"", "main", "not-a-container"} {
		ep := newExecProvider(&fakeProvider{provisionID: "inst-1"}, newExecProcess("ok\n", "", 0))
		h := NewHandler(ep, nil, nil, openCluster())
		if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
			t.Fatalf("CreatePod: %v", err)
		}
		err := h.RunInContainer(context.Background(), "default", "p1", container,
			[]string{"sh"}, &fakeAttach{stdout: newSyncBuffer()})
		if err != nil {
			t.Fatalf("RunInContainer(container=%q): %v", container, err)
		}
	}
}

// Every miss must read as NotFound, which kubectl reports as such rather than as a 500
// the user is invited to retry.
func TestRunInContainer_NotFoundCases(t *testing.T) {
	// No exec support at all — a legitimate configuration (no agent, no key), not a bug.
	t.Run("provider does not support exec", func(t *testing.T) {
		h := NewHandler(&fakeProvider{provisionID: "inst-1"}, nil, nil, openCluster())
		if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
			t.Fatalf("CreatePod: %v", err)
		}
		err := h.RunInContainer(context.Background(), "default", "p1", "main",
			[]string{"sh"}, &fakeAttach{stdout: newSyncBuffer()})
		assertNotFound(t, err)
	})

	// Another node's pod, or one this process never adopted.
	t.Run("pod not tracked", func(t *testing.T) {
		ep := newExecProvider(&fakeProvider{}, newExecProcess("", "", 0))
		h := NewHandler(ep, nil, nil, openCluster())
		err := h.RunInContainer(context.Background(), "default", "ghost", "main",
			[]string{"sh"}, &fakeAttach{stdout: newSyncBuffer()})
		assertNotFound(t, err)
		if ep.askedFor != "" {
			t.Fatalf("provider was asked for instance %q; it must not be called at all", ep.askedFor)
		}
	})

	// Tracked with no instance id: what a rejected Provision leaves behind. There is
	// nothing to run in.
	t.Run("tracked without an instance", func(t *testing.T) {
		ep := newExecProvider(&fakeProvider{provisionErr: errors.New("no capacity")}, newExecProcess("", "", 0))
		h := NewHandler(ep, nil, nil, openCluster())
		if err := h.CreatePod(context.Background(), testPod("default", "p1")); err == nil {
			t.Fatal("CreatePod: expected the provision rejection to surface")
		}
		err := h.RunInContainer(context.Background(), "default", "p1", "main",
			[]string{"sh"}, &fakeAttach{stdout: newSyncBuffer()})
		assertNotFound(t, err)
	})
}

// Starting is bounded, running is not. Without the cap, a provider that waits for a
// queued instance (Modal polls for five minutes) leaves the user at a blank terminal; with
// it applied too widely, an idle shell would be cut off after 30s.
func TestRunInContainer_OnlyTheStartIsBounded(t *testing.T) {
	proc := newExecProcess("ok\n", "", 0)
	ep := newExecProvider(&fakeProvider{provisionID: "inst-1"}, proc)
	h := NewHandler(ep, nil, nil, openCluster())
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	err := h.RunInContainer(context.Background(), "default", "p1", "main",
		[]string{"sh"}, &fakeAttach{stdout: newSyncBuffer()})
	if err != nil {
		t.Fatalf("RunInContainer: %v", err)
	}
	if !ep.startBounded {
		t.Fatal("the provider was given an unbounded context to start the command in")
	}
	if left := time.Until(ep.startDeadline); left > execStartTimeout {
		t.Fatalf("start budget = %v, want at most %v", left, execStartTimeout)
	}
	if proc.waitBounded {
		t.Fatal("Wait inherited the start deadline; a long-running command would be killed")
	}
}

// A provider that cannot start the command (sandbox still queued, API down) is a real
// error: NotFound would claim the Pod cannot be exec'd into at all.
func TestRunInContainer_StartErrorIsNotNotFound(t *testing.T) {
	ep := newExecProvider(&fakeProvider{provisionID: "inst-1"}, newExecProcess("", "", 0))
	ep.execErr = errors.New("timed out waiting for task id")
	h := NewHandler(ep, nil, nil, openCluster())
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	err := h.RunInContainer(context.Background(), "default", "p1", "main",
		[]string{"sh"}, &fakeAttach{stdout: newSyncBuffer()})
	if err == nil {
		t.Fatal("expected an error when the provider cannot start the command")
	}
	if errdefs.IsNotFound(err) {
		t.Fatalf("err = %v, want a plain error rather than NotFound", err)
	}
	if !strings.Contains(err.Error(), "inst-1") {
		t.Fatalf("err = %v, want the instance id in the message", err)
	}
}

// An empty command is a malformed request, not a missing Pod, and must never reach the
// provider — a provider that ran a default shell for it would be a surprise.
func TestRunInContainer_EmptyCommandRejected(t *testing.T) {
	ep := newExecProvider(&fakeProvider{provisionID: "inst-1"}, newExecProcess("", "", 0))
	h := NewHandler(ep, nil, nil, openCluster())
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	err := h.RunInContainer(context.Background(), "default", "p1", "main",
		nil, &fakeAttach{stdout: newSyncBuffer()})
	if err == nil {
		t.Fatal("expected an error for an empty command")
	}
	if ep.askedFor != "" {
		t.Fatalf("provider was asked for instance %q; an empty command must not reach it", ep.askedFor)
	}
}

// compile-time check that the fake matches the seam the handler asserts on.
var _ provider.Executor = (*execProvider)(nil)
