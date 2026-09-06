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

// Command logship copies the logs of Nebula's externally-run instances onto its own stdout, from
// which the cluster's log agent takes them the rest of the way. See the component's design.md.
//
// It takes no arguments and has one mode: discover every Nebula Pod in the cluster and ship it
// through the provider that Pod was placed on. Which providers this build can read is in fleet.go's
// register; which one an instance needs is the Pod's to say, never a flag's — see internal/provider.
//
// There is deliberately no way to ship, or to read, an instance named by hand. A record's identity —
// the pod name and the tenant labels — has exactly one source, the Pod the instance belongs to,
// because a second source can only ever disagree with it, and a record carrying the wrong identity is
// delivered, retained, billed and invisible, with nothing reporting a failure.
//
// stdout is the data channel and nothing else may write to it: every diagnostic goes to stderr,
// deliberately, all the way down to the stats line at the end.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/InftyAI/Nebula/components/logship/internal/emit"
	"github.com/InftyAI/Nebula/components/logship/internal/provider"
	"github.com/InftyAI/Nebula/components/logship/internal/supervise"
	"github.com/InftyAI/Nebula/components/logship/internal/watch"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "logship:", err)
		os.Exit(1)
	}
}

// run is main's body so that os.Exit cannot skip the providers' Close, which is what ends their
// connections rather than leaving the server to time them out.
func run() error {
	// No flags to define, and Parse stays regardless: it rejects an argument instead of ignoring one,
	// so an out-of-date invocation fails loudly rather than quietly shipping the whole cluster.
	flag.Parse()

	// A running instance never ends its stream, so Ctrl-C is the ordinary way out.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	set := provider.NewSet()
	register(set)
	defer func() {
		if err := set.Close(); err != nil {
			logf("closing the log providers", "err", err)
		}
	}()

	return shipCluster(ctx, set)
}

// shipCluster ships every Nebula Pod in the cluster, discovering them rather than being told one —
// which is the whole of what the Deployment does. See this package's doc comment for why there is no
// by-hand alternative.
func shipCluster(ctx context.Context, set *provider.Set) error {
	client, err := watch.NewClient()
	if err != nil {
		return err
	}
	f := newFleet(ctx, set)

	logf("watching Pods", "enabled", watch.EnabledLabel, "instanceID", watch.InstanceIDAnnotation,
		"providers", set.Names())
	w := &watch.Watcher{
		Client: client,
		Fleet:  f,
		Log:    logf,
	}
	if err := w.Run(ctx); err != nil {
		return err
	}
	f.shutdown()
	return nil
}

// newFleet wires the supervisor to the providers. Nothing is dialled here: a provider opens when the
// first Pod on it arrives, so a cluster using one of them needs no credentials for the others.
func newFleet(ctx context.Context, set *provider.Set) *fleet {
	f := &fleet{set: set, sink: emit.New(os.Stdout), log: logf}
	f.sup = supervise.New(ctx, supervise.Config{
		Streams: f.streams,
		Build:   f.build,
		Log:     logf,
	})
	return f
}

func (f *fleet) shutdown() {
	f.sup.Shutdown()
	fmt.Fprintf(os.Stderr, "logship: %+v\n", f.sup.Stats())
}

// Stderr, not stdout: stdout carries the records. A diagnostic printed there would reach CloudWatch
// as a record the consumer cannot parse.
func logf(msg string, kv ...any) {
	fmt.Fprintln(os.Stderr, append([]any{"logship:", msg}, kv...)...)
}
