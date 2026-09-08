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
	"strings"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	vknode "github.com/virtual-kubelet/virtual-kubelet/node"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// Nothing here can be tested against a fake clientset: the emptiness of the service informer is
// the API server honouring a field selector, and the fake tracker ignores field selectors — a
// fake-backed test would report the opposite of production.
func TestServiceLinksNeverReachTheProvider(t *testing.T) {
	client := envtestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The shape that raised this: one Service per sandbox workload, in the workload's namespace.
	// Its ClusterIP is what the pod controller would otherwise hand to the provider.
	svc, err := client.CoreV1().Services("default").Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "cpu-speed-dox75t6-sandbox"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "http", Port: 8080}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create the sandbox Service: %v", err)
	}
	t.Logf("Service %s has ClusterIP %s", svc.Name, svc.Spec.ClusterIP)

	// A Service cannot be named this, which is what makes the informer empty rather than lucky.
	_, err = client.CoreV1().Services("default").Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: noSuchServiceName},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
	}, metav1.CreateOptions{})
	if err == nil {
		t.Fatalf("the API server accepted a Service named %q, so the selector can match one", noSuchServiceName)
	}
	t.Logf("naming a Service %q: %v", noSuchServiceName, err)

	// Two wirings, ours and the one that used to be here. The second is the control: without it
	// this test would also pass on a pod controller that populated no environment at all.
	for _, tc := range []struct {
		name      string
		node      string
		factory   func(kubernetes.Interface) informers.SharedInformerFactory
		wantLinks bool
	}{
		{"ours", "nebula-test-ours", noServiceLinksFactory, false},
		{
			"a cluster-wide service informer", "nebula-test-clusterwide",
			func(c kubernetes.Interface) informers.SharedInformerFactory {
				return informers.NewSharedInformerFactory(c, informerResync)
			},
			true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := envSeenByProvider(t, ctx, client, tc.node, tc.factory)

			// The Pod's own variable, and the proof that env population ran at all.
			if env["MY_OWN"] != "mine" {
				t.Fatalf("the provider saw env %v, want the Pod's own MY_OWN", env)
			}
			var links []string
			for name := range env {
				if strings.Contains(name, "_SERVICE_HOST") || strings.Contains(name, "_PORT_8080_TCP") {
					links = append(links, name)
				}
			}
			if tc.wantLinks && len(links) == 0 {
				t.Fatalf("no service links in %v: the control case must show what ours suppresses", env)
			}
			if !tc.wantLinks && len(links) > 0 {
				t.Errorf("the provider was handed service links %v", links)
			}
			// The master service is the half enableServiceLinks=false cannot reach, so it gets
			// its own assertion rather than riding on the suffix match above.
			if _, ok := env["KUBERNETES_SERVICE_HOST"]; ok != tc.wantLinks {
				t.Errorf("KUBERNETES_SERVICE_HOST present=%t, want %t", ok, tc.wantLinks)
			}
		})
	}
}

// envSeenByProvider runs a pod controller wired like Runner.Start, with the given Services source,
// and returns the environment of the one container in the Pod it hands to the provider. Everything
// but the Services source is production's own wiring, since that is what the assertion is about.
func envSeenByProvider(
	t *testing.T, ctx context.Context, client kubernetes.Interface, nodeName string,
	svcFactoryFor func(kubernetes.Interface) informers.SharedInformerFactory,
) map[string]string {
	t.Helper()

	podFactory := informers.NewSharedInformerFactoryWithOptions(
		client, informerResync,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", nodeName).String()
		}),
	)
	scmFactory := informers.NewSharedInformerFactoryWithOptions(client, informerResync)
	svcFactory := svcFactoryFor(client)

	prov := &capturingProvider{pods: make(chan *corev1.Pod, 1)}
	eb := record.NewBroadcaster()
	defer eb.Shutdown()
	pc, err := vknode.NewPodController(vknode.PodControllerConfig{
		PodClient:         client.CoreV1(),
		EventRecorder:     eb.NewRecorder(scheme.Scheme, corev1.EventSource{Component: nodeName}),
		Provider:          prov,
		PodInformer:       podFactory.Core().V1().Pods(),
		SecretInformer:    scmFactory.Core().V1().Secrets(),
		ConfigMapInformer: scmFactory.Core().V1().ConfigMaps(),
		ServiceInformer:   svcFactory.Core().V1().Services(),
	})
	if err != nil {
		t.Fatalf("build the pod controller: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// After NewPodController, which is what registers the informers with their factories: a
	// factory started before that has nothing to start, and every lister then reads an empty
	// store — which would make this test pass for the wrong reason.
	go podFactory.Start(ctx.Done())
	go scmFactory.Start(ctx.Done())
	go svcFactory.Start(ctx.Done())
	go pc.Run(ctx, 1) //nolint:errcheck

	select {
	case <-pc.Ready():
	case <-pc.Done():
		t.Fatalf("pod controller stopped before it was ready: %v", pc.Err())
	case <-time.After(30 * time.Second):
		t.Fatal("pod controller never became ready")
	}

	// Pre-bound to the virtual node, as a Pod is by the time it reaches the pod controller.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName + "-pod", Namespace: "default"},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{{
				Name:  "workload",
				Image: "busybox",
				Env:   []corev1.EnvVar{{Name: "MY_OWN", Value: "mine"}},
			}},
		},
	}
	if _, err := client.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the Pod: %v", err)
	}

	select {
	case got := <-prov.pods:
		env := map[string]string{}
		for _, e := range got.Spec.Containers[0].Env {
			env[e.Name] = e.Value
		}
		return env
	case <-time.After(30 * time.Second):
		t.Fatal("the provider was never asked to create the Pod")
		return nil
	}
}

// capturingProvider is the provider's half of the pod lifecycle, reduced to recording the one Pod
// this test is about. NotFound on the getters is what makes the controller treat it as new.
type capturingProvider struct {
	pods chan *corev1.Pod
}

func (p *capturingProvider) CreatePod(_ context.Context, pod *corev1.Pod) error {
	select {
	case p.pods <- pod:
	default:
	}
	return nil
}

func (p *capturingProvider) UpdatePod(context.Context, *corev1.Pod) error { return nil }
func (p *capturingProvider) DeletePod(context.Context, *corev1.Pod) error { return nil }

func (p *capturingProvider) GetPod(_ context.Context, ns, name string) (*corev1.Pod, error) {
	return nil, errdefs.NotFoundf("pod %s/%s", ns, name)
}

func (p *capturingProvider) GetPodStatus(_ context.Context, ns, name string) (*corev1.PodStatus, error) {
	return nil, errdefs.NotFoundf("pod %s/%s", ns, name)
}

func (p *capturingProvider) GetPods(context.Context) ([]*corev1.Pod, error) { return nil, nil }

// envtestClient starts a control plane for the test, skipping when its binaries are absent so a
// plain `go test ./...` still runs this package's fake-client tests. Same lookup as the other
// envtest suites here: KUBEBUILDER_ASSETS (set by the Makefile) first, then bin/k8s.
func envtestClient(t *testing.T) kubernetes.Interface {
	t.Helper()

	env := &envtest.Environment{}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		dir := firstEnvtestBinaryDir()
		if dir == "" {
			t.Skip("envtest binaries not found; run 'make setup-envtest'")
		}
		env.BinaryAssetsDirectory = dir
	}

	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start the test control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stop the test control plane: %v", err)
		}
	})

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("build a client for the test control plane: %v", err)
	}
	return client
}

func firstEnvtestBinaryDir() string {
	base := filepath.Join("..", "..", "bin", "k8s")
	entries, err := os.ReadDir(base)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if e.IsDir() {
			return filepath.Join(base, e.Name())
		}
	}
	return ""
}
