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

package watch

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/InftyAI/Nebula/components/logship/internal/supervise"
)

// sandboxPod is a Nebula sandbox Pod as it looks once the virtual kubelet has written the id back.
// Copied from a real one, including the two label domains: the nebula.inftyai.com markers this package
// reads by name, and the consumer's own tenant labels on a separate domain.
func sandboxPod(name, id string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "org-00000000-0000-4000-8000-000000000001",
			Labels: map[string]string{
				EnabledLabel:                          EnabledValue,
				"nebula.inftyai.com/nodepool":         "modal",
				"nebula.inftyai.com/accelerator-type": "l4",
				"app":                                 "sandbox",
				"example.com/org-id":                  "o1",
				"example.com/team-id":                 "t1",
				"example.com/experiment-id":           "e1",
			},
			Annotations: map[string]string{InstanceIDAnnotation: id},
		},
		Spec: corev1.PodSpec{NodeSelector: map[string]string{ProviderSelector: "modal"}},
	}
}

func TestInstanceFor(t *testing.T) {
	t.Run("a provisioned pod ships", func(t *testing.T) {
		inst, ok := instanceFor(sandboxPod("exp-1-sandbox-0", "sb-abc123"))
		if !ok {
			t.Fatal("expected a shippable instance")
		}
		if inst.ID != "sb-abc123" || inst.Pod != "exp-1-sandbox-0" {
			t.Errorf("got %+v, want the instance id and the pod's own name", inst)
		}
		if inst.Labels["app"] != "sandbox" || inst.Labels["example.com/org-id"] != "o1" {
			t.Errorf("labels = %v, want the tenancy the consumer filters on", inst.Labels)
		}
	})

	// The one that matters: the annotation holds the PROVIDER's id, so this Pod offers an EC2 instance
	// id, and it must reach the fleet labelled as such. Handing it to another provider's reader would
	// fail every attempt and burn the restart budget doing it.
	t.Run("the provider travels with the id", func(t *testing.T) {
		pod := sandboxPod("exp-1-sandbox-0", "i-0abc123def456")
		pod.Spec.NodeSelector[ProviderSelector] = "aws"
		inst, ok := instanceFor(pod)
		if !ok {
			t.Fatal("expected a Pod on another provider to still be an instance")
		}
		if inst.Provider != "aws" {
			t.Errorf("Provider = %q, want the nodeSelector's value", inst.Provider)
		}
	})

	t.Run("not shippable yet or not ours", func(t *testing.T) {
		for name, mangle := range map[string]func(*corev1.Pod){
			"id not written back yet": func(p *corev1.Pod) { delete(p.Annotations, InstanceIDAnnotation) },
			"empty id":                func(p *corev1.Pod) { p.Annotations[InstanceIDAnnotation] = "" },
			"not placed yet":          func(p *corev1.Pod) { p.Spec.NodeSelector = nil },
			"not opted into nebula":   func(p *corev1.Pod) { delete(p.Labels, EnabledLabel) },
			"opted in with a typo":    func(p *corev1.Pod) { p.Labels[EnabledLabel] = "True" },
		} {
			t.Run(name, func(t *testing.T) {
				pod := sandboxPod("exp-1-sandbox-0", "sb-abc123")
				mangle(pod)
				if _, ok := instanceFor(pod); ok {
					t.Error("expected the Pod to be skipped")
				}
			})
		}
	})

	// The handler must not hold a piece of the informer's object: the cache is shared, and its copies
	// are not the watch's to keep.
	t.Run("labels are copied out of the cache", func(t *testing.T) {
		pod := sandboxPod("exp-1-sandbox-0", "sb-abc123")
		inst, _ := instanceFor(pod)
		inst.Labels["app"] = "mutated"
		if pod.Labels["app"] != "sandbox" {
			t.Error("instanceFor aliased the Pod's label map")
		}
	})

	// A terminal Pod keeps shipping on purpose: the stream ends by itself, and dropping it here would
	// cut off the tail that says why the run ended.
	t.Run("a terminal pod still ships", func(t *testing.T) {
		pod := sandboxPod("exp-1-sandbox-0", "sb-abc123")
		pod.Status.Phase = corev1.PodFailed
		if _, ok := instanceFor(pod); !ok {
			t.Error("expected a failed Pod's tail to still be shipped")
		}
	})
}

// modalRef is what a sandboxPod's instance is tracked under: the provider travels with the id, so a
// Forget names both — see supervise.Ref.
func modalRef(id string) supervise.Ref {
	return supervise.Ref{Provider: "modal", ID: id}
}

// recorder stands in for the Supervisor: the watch's contract is which calls it makes, not what the
// pipelines then do.
type recorder struct {
	mu      sync.Mutex
	ensured []string
	forgot  []supervise.Ref
}

func (r *recorder) Ensure(inst supervise.Instance) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensured = append(r.ensured, inst.ID)
}

func (r *recorder) Forget(ref supervise.Ref) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.forgot = append(r.forgot, ref)
}

func (r *recorder) waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		ok := cond()
		r.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t.Fatalf("timed out waiting for %s (ensured=%v forgot=%v)", what, r.ensured, r.forgot)
}

func TestWatcherDrivesTheFleet(t *testing.T) {
	// Present before the watch starts, so it arrives through the initial list rather than an event.
	existing := sandboxPod("exp-1-sandbox-0", "sb-existing")
	client := fake.NewSimpleClientset(existing)
	rec := &recorder{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{Client: client, Fleet: rec, Resync: time.Hour}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	rec.waitFor(t, "the existing pod to be ensured", func() bool {
		return len(rec.ensured) == 1 && rec.ensured[0] == "sb-existing"
	})
	// The ordinary path: a Pod is created before it has an id, and gains one when Provision returns.
	pending := sandboxPod("exp-1-sandbox-1", "")
	delete(pending.Annotations, InstanceIDAnnotation)
	pods := client.CoreV1().Pods(pending.Namespace)
	if _, err := pods.Create(ctx, pending, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	provisioned := sandboxPod("exp-1-sandbox-1", "sb-new")
	if _, err := pods.Update(ctx, provisioned, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, "the update that carries the new id", func() bool {
		for _, id := range rec.ensured {
			if id == "sb-new" {
				return true
			}
		}
		return false
	})

	if err := pods.Delete(ctx, provisioned.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, "the delete to forget it", func() bool {
		return len(rec.forgot) == 1 && rec.forgot[0] == modalRef("sb-new")
	})

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil on cancellation", err)
	}
}

// Re-provisioning rewrites the annotation on the SAME Pod, so the instance it displaced never gets a
// delete event. Nothing else takes it out: an instance is forgotten by id, and every delete names the
// id the Pod carries now.
func TestAReplacedInstanceIsForgotten(t *testing.T) {
	pod := sandboxPod("exp-1-sandbox-0", "sb-old")
	client := fake.NewSimpleClientset(pod)
	rec := &recorder{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{Client: client, Fleet: rec, Resync: time.Hour}
	go func() { _ = w.Run(ctx) }()
	rec.waitFor(t, "the first instance", func() bool {
		return len(rec.ensured) > 0 && rec.ensured[0] == "sb-old"
	})

	pods := client.CoreV1().Pods(pod.Namespace)
	if _, err := pods.Update(ctx, sandboxPod("exp-1-sandbox-0", "sb-new"), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, "the replaced instance to be forgotten", func() bool {
		return len(rec.forgot) == 1 && rec.forgot[0] == modalRef("sb-old")
	})
	rec.waitFor(t, "the replacement to ship", func() bool {
		for _, id := range rec.ensured {
			if id == "sb-new" {
				return true
			}
		}
		return false
	})

	// And only the replacement is left to stop: forgetting sb-old twice would be harmless, but it would
	// mean the watch still believes the Pod owns it.
	if err := pods.Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, "the delete", func() bool {
		return len(rec.forgot) == 2 && rec.forgot[1] == modalRef("sb-new")
	})
}

// An unchanged id must not be read as a replacement: the informer re-delivers the same Pod on every
// unrelated update and on every resync, and forgetting there would stop a healthy instance and then
// restart it from the beginning of its history.
func TestAnUnchangedIdIsNotAReplacement(t *testing.T) {
	w := &Watcher{Fleet: &recorder{}}
	first := modalRef("sb-1")

	if old, ok := w.replace("ns/pod", first); ok {
		t.Errorf("first sighting displaced %v, want nothing", old)
	}
	if old, ok := w.replace("ns/pod", first); ok {
		t.Errorf("re-delivery displaced %v, want nothing", old)
	}
	if old, ok := w.replace("ns/pod", modalRef("sb-2")); !ok || old != first {
		t.Errorf("replacement displaced %v (ok=%t), want %v", old, ok, first)
	}
	// Not reachable through a Pod, whose nodeSelector cannot change. Asserted because the comparison is
	// on the whole Ref: an unchanged id is not by itself what makes this a re-delivery.
	if old, ok := w.replace("ns/pod", supervise.Ref{Provider: "aws", ID: "sb-2"}); !ok || old.Provider != "modal" {
		t.Errorf("a second provider on the same id displaced %v (ok=%t), want the modal one", old, ok)
	}
}

// Instance Pod names are unique per org namespace, not per cluster: two orgs running the same
// experiment name would otherwise evict each other's instances.
func TestTwoOrgsCanRunTheSamePodName(t *testing.T) {
	w := &Watcher{Fleet: &recorder{}}
	a := sandboxPod("exp-1-sandbox-0", "sb-a")
	b := sandboxPod("exp-1-sandbox-0", "sb-b")
	b.Namespace = "org-00000000-0000-4000-8000-000000000002"

	w.ensure(a)
	w.ensure(b)
	if got := w.Fleet.(*recorder).forgot; len(got) != 0 {
		t.Errorf("forgot %v, want nothing: these are different Pods", got)
	}
}
