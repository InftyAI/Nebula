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
	"sync"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	vknode "github.com/virtual-kubelet/virtual-kubelet/node"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
)

// recordingProvider keeps the Pods virtual-kubelet hands CreatePod, and knows no other.
type recordingProvider struct {
	mu      sync.Mutex
	created []*corev1.Pod
}

func (p *recordingProvider) CreatePod(_ context.Context, pod *corev1.Pod) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.created = append(p.created, pod.DeepCopy())
	return nil
}

func (p *recordingProvider) UpdatePod(context.Context, *corev1.Pod) error { return nil }
func (p *recordingProvider) DeletePod(context.Context, *corev1.Pod) error { return nil }

func (p *recordingProvider) GetPod(_ context.Context, ns, name string) (*corev1.Pod, error) {
	return nil, errdefs.NotFoundf("pod %s/%s", ns, name)
}

func (p *recordingProvider) GetPodStatus(_ context.Context, ns, name string) (*corev1.PodStatus, error) {
	return nil, errdefs.NotFoundf("pod %s/%s", ns, name)
}

func (p *recordingProvider) GetPods(context.Context) ([]*corev1.Pod, error) { return nil, nil }

func (p *recordingProvider) first() *corev1.Pod {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.created) == 0 {
		return nil
	}
	return p.created[0]
}

// CreatePod relies on virtual-kubelet resolving every env reference first (see literalEnv). This
// runs a Pod through its pod controller, wired as Runner.Start wires it, and fails the day an
// upgrade or SkipDownwardAPIResolution hands CreatePod a reference instead.
func TestVirtualKubeletResolvesEnvBeforeCreatePod(t *testing.T) {
	const ns = "default"
	kube := fake.NewClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "creds"},
			Data:       map[string][]byte{"token": []byte("s3cr3t")},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "conf"},
			Data:       map[string]string{"MODE": "fast", "URL": "https://x"},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "work"},
			Spec: corev1.PodSpec{
				NodeName: "nebula-test",
				Containers: []corev1.Container{{
					Name: "main", Image: "img:1",
					EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "conf"}}}},
					Env: []corev1.EnvVar{
						{Name: "TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "creds"}, Key: "token"}}},
						// apiVersion as the API server defaults it, which virtual-kubelet requires.
						{Name: "POD", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
							APIVersion: "v1", FieldPath: "metadata.name"}}},
						{Name: "PLAIN", Value: "p"},
					},
				}},
			},
		},
	)
	podFactory := informers.NewSharedInformerFactory(kube, 0)
	scmFactory := informers.NewSharedInformerFactory(kube, 0)
	svcFactory := noServiceLinksFactory(kube)
	prov := &recordingProvider{}
	pc, err := vknode.NewPodController(vknode.PodControllerConfig{
		PodClient:         kube.CoreV1(),
		EventRecorder:     record.NewFakeRecorder(100),
		Provider:          prov,
		PodInformer:       podFactory.Core().V1().Pods(),
		SecretInformer:    scmFactory.Core().V1().Secrets(),
		ConfigMapInformer: scmFactory.Core().V1().ConfigMaps(),
		ServiceInformer:   svcFactory.Core().V1().Services(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	podFactory.Start(ctx.Done())
	scmFactory.Start(ctx.Done())
	svcFactory.Start(ctx.Done())
	go pc.Run(ctx, 1) //nolint:errcheck

	if err := wait.PollUntilContextTimeout(ctx, 20*time.Millisecond, 10*time.Second, true,
		func(context.Context) (bool, error) { return prov.first() != nil, nil }); err != nil {
		// Its reason is in virtual-kubelet's log: set its log.L to see it.
		t.Fatalf("virtual-kubelet never called CreatePod: %v", err)
	}
	got, err := literalEnv(prov.first())
	if err != nil {
		t.Fatalf("CreatePod got an unresolved env, which literalEnv refuses: %v", err)
	}
	want := map[string]string{"MODE": "fast", "URL": "https://x", "TOKEN": "s3cr3t", "POD": "work", "PLAIN": "p"}
	if len(got) != len(want) {
		t.Fatalf("env = %v, want exactly %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("env[%s] = %q, want %q", k, got[k], v)
		}
	}
}
