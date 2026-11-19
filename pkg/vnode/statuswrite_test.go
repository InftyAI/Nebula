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
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// TestPersistStatus runs the status write against a real API server: what it keeps, takes over
// and refuses is server-side apply behaviour that a fake clientset does not implement.
func TestPersistStatus(t *testing.T) {
	cs := startAPIServer(t)
	ctx := context.Background()
	pods := cs.CoreV1().Pods("default")
	h := NewHandler(&fakeProvider{}, cs, nil, openCluster())

	create := func(t *testing.T, name string) *corev1.Pod {
		t.Helper()
		pod, err := pods.Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "img"}}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return pod
	}
	live := func(t *testing.T, name string) *corev1.Pod {
		t.Helper()
		pod, err := pods.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return pod
	}
	running := func(pod *corev1.Pod) *corev1.Pod {
		pod = pod.DeepCopy()
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Reason, pod.Status.Message = "", ""
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		return pod
	}

	t.Run("a stale copy keeps what others wrote since", func(t *testing.T) {
		stale := create(t, "keep") // the handler's copy, taken at CreatePod
		pod := live(t, "keep")
		pod.Annotations = map[string]string{"controller.kubernetes.io/pod-deletion-cost": "-100"}
		pod, err := pods.Update(ctx, pod, metav1.UpdateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		pod.Status.Conditions = append(pod.Status.Conditions,
			corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue})
		if _, err := pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{FieldManager: "eviction"}); err != nil {
			t.Fatal(err)
		}

		h.persistStatus(ctx, running(stale))

		got := live(t, "keep")
		if got.Annotations["controller.kubernetes.io/pod-deletion-cost"] != "-100" {
			t.Errorf("annotation added after CreatePod was lost: %v", got.Annotations)
		}
		if !hasCondition(got, corev1.DisruptionTarget, corev1.ConditionTrue) {
			t.Errorf("another manager's condition was lost: %v", got.Status.Conditions)
		}
		if got.Status.Phase != corev1.PodRunning || !hasCondition(got, corev1.PodReady, corev1.ConditionTrue) {
			t.Errorf("status not written: phase %s, conditions %v", got.Status.Phase, got.Status.Conditions)
		}
	})

	t.Run("takes over the status VK wrote, so a cleared reason clears", func(t *testing.T) {
		pod := create(t, "migrate")
		// VK's write before this release: an update under the client's default manager.
		pod.Status.Phase = corev1.PodPending
		pod.Status.Reason = "Provisioning"
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
		if _, err := pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}

		h.persistStatus(ctx, running(pod))

		got := live(t, "migrate")
		if got.Status.Reason != "" || got.Status.Phase != corev1.PodRunning {
			t.Errorf("phase %s reason %q, want Running and the reason cleared", got.Status.Phase, got.Status.Reason)
		}
		for _, mf := range got.ManagedFields {
			if mf.Manager == vkFieldManager && mf.Subresource == "status" {
				t.Errorf("VK's status entry is still there, co-owning what it wrote: %+v", mf)
			}
		}
	})

	t.Run("a terminal Pod is not written again", func(t *testing.T) {
		pod := create(t, "final")
		pod.Status.Phase = corev1.PodFailed
		if _, err := pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}

		h.persistStatus(ctx, running(pod)) // the API server would accept Failed -> Running

		if got := live(t, "final").Status.Phase; got != corev1.PodFailed {
			t.Errorf("phase = %s, want Failed left alone", got)
		}
	})

	t.Run("a write for a deleted Pod misses its same-named successor", func(t *testing.T) {
		old := create(t, "reborn")
		if err := pods.Delete(ctx, "reborn", metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
			t.Fatal(err)
		}
		create(t, "reborn")

		h.persistStatus(ctx, running(old))

		if got := live(t, "reborn").Status.Phase; got == corev1.PodRunning {
			t.Error("the old Pod's status landed on the new one")
		}
	})

	t.Run("an unchanged status is not rewritten, a terminal one is final", func(t *testing.T) {
		pod := create(t, "dedup")
		k := key(pod.Namespace, pod.Name)
		h.mu.Lock()
		h.tracked[k] = &trackedPod{pod: pod}
		h.mu.Unlock()

		h.persistStatus(ctx, running(pod))
		rv := live(t, "dedup").ResourceVersion
		h.persistStatus(ctx, running(pod))
		if got := live(t, "dedup").ResourceVersion; got != rv {
			t.Errorf("unchanged status rewritten: resourceVersion %s -> %s", rv, got)
		}

		done := running(pod)
		done.Status.Phase = corev1.PodSucceeded
		h.persistStatus(ctx, done)
		h.persistStatus(ctx, running(pod)) // a stale tick after the terminal write
		if got := live(t, "dedup").Status.Phase; got != corev1.PodSucceeded {
			t.Errorf("phase = %s, want Succeeded to stay", got)
		}
	})
}

func hasCondition(pod *corev1.Pod, typ corev1.PodConditionType, status corev1.ConditionStatus) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == typ {
			return c.Status == status
		}
	}
	return false
}

// startAPIServer starts envtest's API server for the test, skipping it without the binaries
// (`make setup-envtest` installs them under bin/k8s; KUBEBUILDER_ASSETS also works).
func startAPIServer(t *testing.T) kubernetes.Interface {
	t.Helper()
	env := &envtest.Environment{}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		entries, _ := os.ReadDir(filepath.Join("..", "..", "bin", "k8s"))
		for _, e := range entries {
			if e.IsDir() {
				env.BinaryAssetsDirectory = filepath.Join("..", "..", "bin", "k8s", e.Name())
				break
			}
		}
		if env.BinaryAssetsDirectory == "" {
			t.Skip("no envtest binaries: run `make setup-envtest`")
		}
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	return kubernetes.NewForConfigOrDie(cfg)
}
