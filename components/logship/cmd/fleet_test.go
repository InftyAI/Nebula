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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/InftyAI/Nebula/components/logship/internal/emit"
	"github.com/InftyAI/Nebula/components/logship/internal/provider"
	"github.com/InftyAI/Nebula/components/logship/internal/ship"
	"github.com/InftyAI/Nebula/components/logship/internal/supervise"
)

func podLabels() map[string]string {
	return map[string]string{
		"app":                        "sandbox",
		"example.com/org-id":         "org-1",
		"example.com/team-id":        "team-2",
		"example.com/experiment-id":  "exp-3",
		"pod-template-hash":          "7d9f",
		"example.com/something-else": "kept",
	}
}

func TestRecordCarriesEveryPodLabel(t *testing.T) {
	// The tenant keys are never read here — they are the consumer's, and it reads them off the record.
	// Copying the map wholesale is what keeps that true without this package knowing a key.
	inst := supervise.Instance{Pod: "pod-a", Labels: podLabels()}
	format := ship.Record{Pod: inst.Pod, Labels: inst.Labels}.Formatter()

	msg := format(ship.Line{Data: "hello", At: time.Unix(0, 0), Cursor: "1-0"})
	var rec struct {
		Log        string `json:"log"`
		Kubernetes struct {
			PodName string            `json:"pod_name"`
			Labels  map[string]string `json:"labels"`
		} `json:"kubernetes"`
	}
	if err := json.Unmarshal([]byte(msg), &rec); err != nil {
		t.Fatalf("record is not JSON: %v\n%s", err, msg)
	}
	if rec.Kubernetes.PodName != "pod-a" {
		t.Errorf("pod_name = %q", rec.Kubernetes.PodName)
	}
	if len(rec.Kubernetes.Labels) != len(podLabels()) {
		t.Fatalf("%d labels, want all %d of the Pod's", len(rec.Kubernetes.Labels), len(podLabels()))
	}
	if rec.Kubernetes.Labels["pod-template-hash"] != "7d9f" {
		t.Error("a label unrelated to tenancy was dropped")
	}

	// The nested envelope is what keeps the line out of the hidden `private` category.
	var inner struct {
		Level    string `json:"level"`
		Category string `json:"category"`
		Message  string `json:"message"`
	}
	if err := json.Unmarshal([]byte(rec.Log), &inner); err != nil {
		t.Fatalf("log is not nested JSON: %v", err)
	}
	if inner.Category != ship.CategoryUser || inner.Message != "hello" {
		t.Fatalf("inner = %+v", inner)
	}
}

// stub is a log provider with one stream, whose instances have already ended.
type stub struct{ reserved int }

func (s *stub) Streams() []string { return []string{"stdout"} }

func (s *stub) Source(_, stream string) (ship.Source, error) {
	if stream != "stdout" {
		return nil, fmt.Errorf("no stream %q", stream)
	}
	return endedStream{}, nil
}

func (s *stub) Reserve(instances int) error { s.reserved = instances; return nil }
func (s *stub) Close() error                { return nil }

type endedStream struct{}

func (endedStream) Follow(context.Context, string, func(ship.Batch) error) error { return nil }

// newTestFleet wires a fleet exactly as newFleet does, so what is under test is the routing rather
// than a rebuilt copy of it.
func newTestFleet(t *testing.T, set *provider.Set) (*fleet, *[]string) {
	t.Helper()

	var logged []string
	f := &fleet{
		set:  set,
		sink: emit.New(io.Discard),
		log:  func(msg string, _ ...any) { logged = append(logged, msg) },
	}
	f.sup = supervise.New(context.Background(), supervise.Config{Streams: f.streams, Build: f.build})
	t.Cleanup(f.sup.Shutdown)
	return f, &logged
}

func TestAnInstanceOnAnUnknownProviderIsNotShipped(t *testing.T) {
	// Nebula gaining a provider before this build does. Nothing can read the instance, and the only
	// other symptom is a run whose logs never arrive.
	f, logged := newTestFleet(t, provider.NewSet())

	f.Ensure(supervise.Instance{Provider: "aws", ID: "i-0abc", Pod: "exp-1-sandbox-0"})

	if len(*logged) == 0 {
		t.Error("an unreadable instance was skipped without a word")
	}
	if st := f.sup.Stats(); st.Started != 0 {
		t.Errorf("the fleet started %d instances it has no provider for", st.Started)
	}
}

func TestRoomIsMadeBeforeTheStreamsOpen(t *testing.T) {
	// Reserve has to run before Ensure, not after: for Modal a connection added once the streams are
	// queued cannot take them over, and gRPC-go queues them silently. See fleet.Ensure.
	p := &stub{}
	set := provider.NewSet()
	set.Register(provider.ProviderModal, func() (provider.Provider, error) { return p, nil })
	f, _ := newTestFleet(t, set)

	f.Ensure(supervise.Instance{Provider: provider.ProviderModal, ID: "sb-1", Pod: "p"})

	if p.reserved != 1 {
		t.Errorf("reserved for %d instances, want the one being added", p.reserved)
	}
}

func TestBuildRejectsAStreamTheProviderDoesNotHave(t *testing.T) {
	// The supervisor names streams with strings, so a typo has to fail loudly rather than open
	// something that ships nothing.
	set := provider.NewSet()
	set.Register(provider.ProviderModal, func() (provider.Provider, error) { return &stub{}, nil })
	f, _ := newTestFleet(t, set)

	inst := supervise.Instance{Provider: provider.ProviderModal, ID: "sb-1"}
	if _, err := f.build(inst, "stdlog"); err == nil {
		t.Fatal("build accepted a stream the provider does not have")
	}
}
