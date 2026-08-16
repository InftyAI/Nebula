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
	"fmt"
	"io"
	"sync"
	"time"

	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	utilexec "k8s.io/utils/exec"

	"github.com/InftyAI/Nebula/pkg/provider"
)

const (
	// execDrainGrace is how long output may still arrive after the exit code has. The two
	// travel on separate streams, so the last bytes can lose the race. Bounded, because a
	// stream that never signals EOF must not hang the request forever.
	execDrainGrace = 2 * time.Second

	// execStartTimeout caps how long a provider may take to START the command — dialling
	// the instance, waiting for its container. Matches the stream creation timeout the
	// kubelet routes use, so the whole handshake has one budget.
	execStartTimeout = 30 * time.Second
)

// runExec pumps one `kubectl exec`: the client's terminal (vkapi.AttachIO) on one side,
// the provider's command (provider.Process) on the other. The provider seam only starts
// the command, so this is the whole contract in one place and every provider behaves the
// same.
//
// The returned error is what the VK exec route turns into a status: nil for a clean run,
// a utilexec.ExitError for a non-zero exit, anything else an internal error.
func runExec(ctx context.Context, proc provider.Process, attach vkapi.AttachIO) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Cancel is what releases the provider's streams, so nothing keeps streaming after
	// this returns — including when the copies below end because the client hung up.
	go func() {
		<-ctx.Done()
		_ = proc.Close()
	}()

	// Resize events are dropped: no provider carries a window size today. They are still
	// drained, or the VK goroutine sending them parks until the exec ends.
	if resize := attach.Resize(); resize != nil {
		go func() {
			for {
				select {
				case <-resize:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Stdin is copied without being waited on: an interactive shell's stdin ends only when
	// the client goes away, so the command decides when the exec is over. Closing on EOF is
	// what makes `kubectl exec -i -- cat < file` terminate.
	if src, dst := attach.Stdin(), proc.Stdin(); src != nil && dst != nil {
		go func() {
			_, _ = io.Copy(dst, src)
			_ = dst.Close()
		}()
	}

	// A copy that FAILS means the client is gone or the transport died, and it is the only
	// signal we get: the VK exec route runs on a context of its own, so a disconnect never
	// reaches ctx. Without this, closing a laptop mid-shell leaves Wait blocked for as long
	// as the command lives.
	output := copyOutput(attach, proc, cancel)

	code, err := proc.Wait(ctx)
	// Let output still in flight land before reporting the outcome, so a command's last
	// line is not lost to its exit code overtaking it.
	select {
	case <-output:
	case <-ctx.Done():
	case <-time.After(execDrainGrace):
	}

	if err != nil {
		return fmt.Errorf("wait for command: %w", err)
	}
	if code != 0 {
		// A TYPED exit error: the VK route reports anything else as an internal error, so
		// the user would see a 500 instead of "command terminated with exit code N".
		return utilexec.CodeExitError{
			Err:  fmt.Errorf("command terminated with exit code %d", code),
			Code: code,
		}
	}
	return nil
}

// copyOutput streams the command's stdout and stderr to the client and returns a channel
// closed once BOTH have ended — i.e. the command has produced everything it will. A copy
// that fails calls broken, since neither side can be reached any more.
func copyOutput(attach vkapi.AttachIO, proc provider.Process, broken func()) <-chan struct{} {
	var wg sync.WaitGroup
	pump := func(dst io.Writer, src io.Reader) {
		// A nil stream means the client did not ask for it, or the command has none (stderr
		// under a TTY, where the terminal carries both).
		if dst == nil || src == nil {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// EOF is the command finishing normally and returns nil; any other error means
			// this exec has nowhere left to go, so end it rather than wait out the command.
			if _, err := io.Copy(dst, src); err != nil {
				broken()
			}
		}()
	}
	pump(attach.Stdout(), proc.Stdout())
	pump(attach.Stderr(), proc.Stderr())

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}
