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
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	utilexec "k8s.io/utils/exec"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/InftyAI/SandD/go/controller"
)

// Execer and Session are SEAMS FOR TESTS, not a second definition of the controller's
// contract.
//
// Everything with meaning — the error sentinels, the read-buffer floor, the shape of an
// exec result — is imported from github.com/InftyAI/SandD/go/controller, which lives
// beside the Rust it wraps so an ABI change breaks one build instead of drifting. These
// two interfaces exist ONLY so a test can substitute a scripted PTY for a real daemon;
// *controller.Server and *controller.Session satisfy them as they are, and adding a method
// there needs no change here.
//
// The rule this file follows: Nebula owns the mapping from the kubelet API onto a session
// (vkapi.AttachIO, errdefs, exit codes, Pod → daemon id) and NOTHING about the protocol
// underneath it.
type Execer interface {
	// Exec runs one command and returns after it completes. Non-interactive.
	Exec(daemonID, command string, timeout time.Duration) (*controller.ExecResult, error)
	// OpenSession allocates a PTY on the daemon. term may be empty for a default.
	OpenSession(daemonID string, rows, cols uint16, term string) (Session, error)
}

// Session is one interactive PTY. Read returns (0, nil) on an idle timeout, which is the
// normal state of a quiet terminal and not an error; only controller.ErrSessionClosed is
// terminal.
type Session interface {
	Read(p []byte, timeout time.Duration) (int, error)
	Write(p []byte) (int, error)
	Resize(rows, cols uint16) error
	Close() error
}

const (
	// sessionReadTimeout is how long a read parks before looping. It bounds how quickly
	// the relay notices ctx cancellation, NOT how long a session may idle — a quiet
	// terminal simply loops. Short enough that closing a terminal is not perceptibly
	// delayed, long enough not to spin.
	sessionReadTimeout = 500 * time.Millisecond

	// execTimeout caps a non-interactive command. The kubelet API has no way to express
	// "no limit", and an unbounded call would pin a relay goroutine to a wedged daemon
	// forever.
	execTimeout = 4 * time.Hour
)

// EmbeddedExecer adapts the embedded controller to the Execer seam.
//
// It exists for ONE reason: controller.Server.OpenSession returns *controller.Session (a
// concrete type), while Execer returns the Session interface so tests can script a PTY. Go
// has no covariant return, so the two cannot be reconciled without this one-line widening.
// Everything else passes straight through.
type EmbeddedExecer struct{ Server *controller.Server }

func (e EmbeddedExecer) Exec(
	daemonID, command string, timeout time.Duration,
) (*controller.ExecResult, error) {
	return e.Server.Exec(daemonID, command, timeout)
}

func (e EmbeddedExecer) OpenSession(
	daemonID string, rows, cols uint16, term string,
) (Session, error) {
	sess, err := e.Server.OpenSession(daemonID, rows, cols, term)
	if err != nil {
		// Return a nil INTERFACE, not a (*controller.Session)(nil) inside one: a typed nil
		// in an interface is non-nil when compared to nil, so passing the concrete pointer
		// through on the error path would defeat every `if sess == nil` guard downstream.
		return nil, err
	}
	return sess, nil
}

var _ Execer = EmbeddedExecer{}

// RunInContainer runs a command inside a provisioned workload by relaying it through the
// SandD daemon that dialed in from the instance.
//
// This is the ONLY path into a Nebula workload. The Pod runs on someone else's cloud, so
// the API server cannot reach it; what makes this work is that the instance's daemon
// already holds an outbound connection to the controller in THIS process (setupSandD), and
// the daemon id is the claim name we minted its token for (see mintSanddAuth).
//
// NOT YET REACHABLE from kubectl: serving this verb also needs the kubelet API mounted on
// an HTTPS server the API server trusts, plus the node advertising an address and port.
// None of that exists yet (see node.go), so nothing CALLS this outside tests even though
// h.exec is now wired whenever SandD is enabled. The relay is written first because it is
// the part that has a contract to get right; the serving stack is plumbing on top of it.
//
// namespace/podName identify the workload; containerName and the exec'd Pod's container
// topology are IGNORED, because a Nebula instance is one VM running one image — there is
// no second container to select. A caller naming a container that does not exist gets the
// same session as one naming nothing, which is the honest behaviour for this shape rather
// than a fabricated error.
func (h *Handler) RunInContainer(
	ctx context.Context,
	namespace, podName, containerName string,
	cmd []string,
	attach vkapi.AttachIO,
) error {
	log := logf.FromContext(ctx).WithValues("pod", key(namespace, podName), "container", containerName)

	if h.exec == nil {
		// No transport wired. NotFound rather than a server error: nothing is broken, the
		// feature is simply not enabled.
		return errdefs.NotFound("exec requires SandD to be enabled on this cluster")
	}

	daemonID, err := h.daemonIDFor(namespace, podName)
	if err != nil {
		return err
	}

	if attach.TTY() {
		return h.execInteractive(ctx, log, daemonID, cmd, attach)
	}
	return h.execOneShot(ctx, daemonID, cmd, attach)
}

// daemonIDFor resolves a Pod to the daemon that serves it. The daemon id is the claim
// name, which is what mintSanddAuth used as the token's `sub`, so this is the same
// identity the controller authenticated at dial-in.
func (h *Handler) daemonIDFor(namespace, podName string) (string, error) {
	h.mu.Lock()
	tp, ok := h.tracked[key(namespace, podName)]
	h.mu.Unlock()

	if !ok {
		return "", errdefs.NotFoundf("pod %s is not tracked by this virtual node", key(namespace, podName))
	}
	if tp.claimName == "" {
		// Tracked but pre-provision: there is no instance and therefore no daemon yet.
		return "", errdefs.NotFoundf("pod %s has no instance yet", key(namespace, podName))
	}
	return tp.claimName, nil
}

// execOneShot runs a non-interactive command and copies its output to the caller.
//
// The command is joined into a single shell string because that is what the daemon's
// protocol accepts — it executes through a shell rather than taking an argv. That means
// `kubectl exec -- sh -c 'a && b'` works, and so does a bare `kubectl exec -- ls`, but an
// argument containing shell metacharacters is interpreted by the remote shell rather than
// passed through literally. Quoting each element would break the common case (the shell
// would see 'ls' as a literal), so the daemon's contract wins.
func (h *Handler) execOneShot(
	ctx context.Context, daemonID string, cmd []string, attach vkapi.AttachIO,
) error {
	if len(cmd) == 0 {
		return errdefs.InvalidInput("exec requires a command")
	}

	// A cancelled context is checked before the call, not during: Exec blocks in the
	// controller with its own timeout and cannot be interrupted, so honouring
	// cancellation mid-flight would mean abandoning a goroutine that still holds a
	// pending request. Failing fast up front is the honest bound.
	if err := ctx.Err(); err != nil {
		return err
	}

	res, err := h.exec.Exec(daemonID, strings.Join(cmd, " "), execTimeout)
	if err != nil {
		return translateExecError(err, daemonID)
	}

	if out := attach.Stdout(); out != nil && res.Stdout != "" {
		if _, err := io.WriteString(out, res.Stdout); err != nil {
			return fmt.Errorf("writing stdout: %w", err)
		}
	}
	if errOut := attach.Stderr(); errOut != nil && res.Stderr != "" {
		if _, err := io.WriteString(errOut, res.Stderr); err != nil {
			return fmt.Errorf("writing stderr: %w", err)
		}
	}

	// A non-zero exit is reported as an error because that is how the kubelet API conveys
	// it, and as an ExitError specifically so the CODE survives the trip (see
	// exitCodeError). Returning nil would make every failing command look successful.
	if res.ExitCode != 0 {
		return exitCodeError{code: res.ExitCode}
	}
	return nil
}

// execInteractive relays a PTY session: stdin and resize events in, output out, until
// either side ends.
//
// Three concurrent concerns, so three goroutines and a done channel rather than one
// select loop: reading from the daemon parks in the controller, reading the caller's
// stdin parks in the API server's stream, and resize events arrive on their own channel.
// None of them can be polled without blocking the others.
func (h *Handler) execInteractive(
	ctx context.Context, log logr.Logger,
	daemonID string, cmd []string, attach vkapi.AttachIO,
) error {
	rows, cols := uint16(24), uint16(80)
	// Take the caller's initial geometry if it is already known. A resize event may
	// arrive before the first read, but starting at the real size avoids a visible reflow
	// on connect.
	select {
	case size := <-attach.Resize():
		if size.Height > 0 && size.Width > 0 {
			rows, cols = size.Height, size.Width
		}
	default:
	}

	sess, err := h.exec.OpenSession(daemonID, rows, cols, "")
	if err != nil {
		return translateExecError(err, daemonID)
	}
	// Closes the PTY on every exit path, including a panic in a relay goroutine: an
	// un-closed session leaves a shell running on a billing instance.
	defer func() {
		if cerr := sess.Close(); cerr != nil {
			log.Info("closing SandD session failed", "daemon", daemonID, "error", cerr.Error())
		}
	}()

	// If a command was given, run it in the PTY. `kubectl exec -it <pod> -- bash` arrives
	// here with cmd=["bash"], and the daemon's PTY already starts a shell, so writing the
	// command as a line is what makes an explicit interpreter behave as asked instead of
	// being silently ignored.
	if len(cmd) > 0 {
		if _, err := sess.Write([]byte(strings.Join(cmd, " ") + "\n")); err != nil {
			return translateExecError(err, daemonID)
		}
	}

	// Buffered by 1 on each: whichever goroutine finishes first must not block on a
	// receiver that has already returned, or it leaks for the life of the process.
	outDone := make(chan error, 1)
	inDone := make(chan error, 1)
	stop := make(chan struct{})

	// daemon -> caller
	go func() {
		buf := make([]byte, controller.ReadBufSize)
		for {
			select {
			case <-stop:
				outDone <- nil
				return
			default:
			}

			n, err := sess.Read(buf, sessionReadTimeout)
			switch {
			case errors.Is(err, controller.ErrSessionClosed):
				outDone <- nil
				return
			case err != nil:
				outDone <- err
				return
			case n == 0:
				// Idle timeout: a quiet terminal, not a failure. Loop so ctx and stop are
				// re-checked.
				continue
			}
			if out := attach.Stdout(); out != nil {
				if _, werr := out.Write(buf[:n]); werr != nil {
					outDone <- werr
					return
				}
			}
		}
	}()

	// caller -> daemon
	go func() {
		if in := attach.Stdin(); in != nil {
			// io.Copy into an adapter rather than a manual loop: it handles short writes
			// and treats EOF as a clean end, which is what closing a terminal produces.
			_, err := io.Copy(sessionWriter{sess}, in)
			inDone <- err
			return
		}
		inDone <- nil
	}()

	// resize events
	go func() {
		for {
			select {
			case <-stop:
				return
			case size, ok := <-attach.Resize():
				if !ok {
					return
				}
				if size.Height == 0 || size.Width == 0 {
					continue
				}
				if err := sess.Resize(size.Height, size.Width); err != nil {
					// Non-fatal: a wrong-sized terminal is usable, a torn-down session is
					// not. Logged rather than returned.
					log.Info("resizing SandD session failed",
						"daemon", daemonID, "error", err.Error())
				}
			}
		}
	}()

	defer close(stop)

	// The session ends when the OUTPUT ends — the remote shell exited — or when the client
	// goes away (ctx). Stdin running out deliberately does NOT end it.
	//
	// That asymmetry is the point: `echo hi | kubectl exec -i pod -- cat` closes stdin
	// immediately, and treating that as the end would return before the output goroutine
	// had relayed a byte, silently truncating exactly the output the user asked for. Stdin
	// reaching EOF only means there is nothing left to send.
	//
	// A stdin FAILURE is still worth surfacing, but not worth killing a live shell over, so
	// it is only consulted once output has also finished.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-outDone:
		if err != nil {
			return err
		}
		// Output is done, so stdin's fate can no longer truncate anything. Report a real
		// stdin error if one is already waiting; never block for one, because a still-open
		// stdin (the normal interactive case) would hang here forever.
		select {
		case ierr := <-inDone:
			if ierr != nil && !errors.Is(ierr, io.EOF) {
				return ierr
			}
		default:
		}
		return nil
	}
}

// sessionWriter adapts a Session to io.Writer so stdin can be io.Copy'd into it.
type sessionWriter struct{ s Session }

func (w sessionWriter) Write(p []byte) (int, error) { return w.s.Write(p) }

// translateExecError maps an Execer error onto the errdefs categories the kubelet API
// understands, so kubectl reports "not found" rather than a generic 500 for the common
// case of a daemon that has not dialed in yet.
func translateExecError(err error, daemonID string) error {
	// errors.Is, not a substring match on err.Error(): the sentinel is part of the Execer
	// contract, so this keeps working when an implementation rewords its message, and
	// cannot be fooled by an unrelated error that happens to contain "not found".
	if errors.Is(err, controller.ErrNoDaemon) {
		return errdefs.NotFoundf(
			"no SandD daemon connected for %s: the instance may still be booting, "+
				"or its daemon cannot reach the controller", daemonID)
	}
	return fmt.Errorf("sandd exec on %s: %w", daemonID, err)
}

// exitCodeError carries a command's non-zero exit status to the client.
//
// It implements k8s.io/utils/exec.ExitError because that is the ONLY way the exit status
// reaches kubectl: VK's remotecommand layer type-asserts the returned error to that
// interface and writes the code into the stream's error channel, so kubectl can print
// "command terminated with exit code N" and exit with it. A plain fmt.Errorf would be
// reported as an internal error and kubectl would exit 1 whatever the command did —
// silently breaking `kubectl exec ... && next` and any script that branches on the code.
type exitCodeError struct{ code int }

func (e exitCodeError) Error() string {
	return fmt.Sprintf("command terminated with exit code %d", e.code)
}
func (e exitCodeError) ExitStatus() int { return e.code }
func (e exitCodeError) Exited() bool    { return true }

// String satisfies utilexec.ExitError, which embeds it via the runtime's Stringer.
func (e exitCodeError) String() string { return e.Error() }

var _ utilexec.ExitError = exitCodeError{}
