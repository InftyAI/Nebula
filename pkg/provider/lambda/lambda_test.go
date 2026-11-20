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

package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	nebula "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/provider/catalog"
)

func TestProvisionRecoversLostLaunchResponse(t *testing.T) {
	ctx := context.Background()
	fixture := &lostReplyCloud{}
	cloud := fixture.server(t)
	defer cloud.Close()

	pod, claim := workload()
	kube := testKube(t, claim)
	p := testProvider(t, cloud, kube)
	req := provider.ProvisionRequest{ClaimName: claim.Name, Region: "us-west-1"}
	if _, err := p.Provision(ctx, pod, req); !errors.Is(err, provider.ErrProvisionUnresolved) {
		t.Fatalf("lost launch response should hold the original workload: %v", err)
	}
	var stored nebula.NodeClaim
	if err := kube.Get(ctx, client.ObjectKey{Name: claim.Name}, &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Finalizers) != 1 || stored.Finalizers[0] != nebula.TerminateInstanceFinalizer {
		t.Fatal("launch did not install its cleanup finalizer")
	}

	p = testProvider(t, cloud, kube)
	if _, err := p.FindByClaim(ctx, claim.Name, req.Region); !errors.Is(err, errLaunchUnresolved) {
		t.Fatalf("an empty List during an ambiguous launch must retain the finalizer, got %v", err)
	}
	if _, err := p.Provision(ctx, pod, req); !errors.Is(err, errLaunchUnresolved) {
		t.Fatalf("retry before visibility = %v", err)
	}
	fixture.mu.Lock()
	fixture.visible = true
	fixture.mu.Unlock()
	result, err := p.Provision(ctx, pod, req)
	if err != nil || result.InstanceID != "vm-1" || !result.Reserved {
		t.Fatalf("recovered launch = %+v, %v", result, err)
	}
	listed, err := p.List(ctx)
	if err != nil || len(listed) != 1 || listed[0].ClaimName != claim.Name || listed[0].State != provider.InstancePending {
		t.Fatalf("instance List = %+v, %v", listed, err)
	}
	if err := p.Terminate(ctx, "vm-1", req.Region); err == nil {
		t.Fatal("terminate reported release before the VM was gone")
	}
	fixture.mu.Lock()
	fixture.visible = false
	fixture.mu.Unlock()
	if err := p.Terminate(ctx, "vm-1", req.Region); err != nil {
		t.Fatalf("repeat terminate: %v", err)
	}
	if found, err := p.FindByClaim(ctx, claim.Name, req.Region); found != nil || err != nil {
		t.Fatalf("terminated claim cannot finalize: %+v, %v", found, err)
	}
	if err := kube.Get(ctx, client.ObjectKey{Name: claim.Name}, &stored); err != nil {
		t.Fatal(err)
	}
	record, err := readAttempt(&stored)
	if err != nil || record == nil || record.InstanceID != "vm-1" || !record.Observed {
		t.Fatalf("recovery did not persist the instance ID: %+v, %v", record, err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.launches != 1 {
		t.Fatalf("launch requests = %d, want 1", fixture.launches)
	}
}

type lostReplyCloud struct {
	mu       sync.Mutex
	vm       *instance
	visible  bool
	launches int
}

func (f *lostReplyCloud) server(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/instance-types":
			_, _ = w.Write([]byte(testInstanceTypes))
		case "/instances":
			writeInstances(t, w, f.vm, f.visible)
		case "/instances/vm-1":
			if f.visible {
				if err := json.NewEncoder(w).Encode(map[string]any{"data": f.vm}); err != nil {
					t.Error(err)
				}
			} else {
				w.WriteHeader(404)
				_, _ = w.Write([]byte(`{"error":{"code":"global/object-does-not-exist"}}`))
			}
		case "/instance-operations/launch":
			f.launches++
			var req launchRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			if req.InstanceType != "gpu_1x_a10" || req.Workspace != "workspace" ||
				!strings.HasPrefix(req.UserData, "#!/bin/bash\n") {
				t.Error("launch did not send the expected SKU, workspace and plain cloud-init")
			}
			f.vm = &instance{
				ID: "vm-1", Status: "booting", Region: region{Name: req.Region}, Workspace: req.Workspace, Tags: req.Tags,
			}
			w.WriteHeader(502)
		case "/instance-operations/terminate":
			f.vm.Status = "terminating"
			reply := map[string]any{"data": map[string]any{"terminated_instances": []instance{*f.vm}}}
			if err := json.NewEncoder(w).Encode(reply); err != nil {
				t.Error(err)
			}
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			w.WriteHeader(500)
		}
	}))
}

func TestOwnershipAndReadiness(t *testing.T) {
	_, claim := workload()
	claim.Name = strings.Repeat("a", 253)
	p := testProvider(t, nil, testKube(t, claim))
	vm := instance{
		ID: "vm-1", Status: "active", IP: "198.51.100.20", Region: region{Name: "us-west-1"},
		Workspace: "workspace", Tags: claimTags("cluster", claim),
	}
	if !p.owns(vm, claim) || len(instanceName(claim)) > 64 {
		t.Fatal("long claim identity did not round trip")
	}
	old := claim.DeepCopy()
	old.UID = "old-claim"
	if p.owns(vm, old) || p.owns(instance{Tags: claimTags("other-cluster", claim)}, claim) {
		t.Fatal("accepted a foreign claim or cluster")
	}
	p.health = func(context.Context, string) bool { return false }
	if state := p.observe(context.Background(), vm, claim.Name, true).State; state != provider.InstanceNotReady {
		t.Fatalf("active VM with unhealthy container = %s", state)
	}
	p.health = func(_ context.Context, endpoint string) bool { return endpoint == "198.51.100.20:8080" }
	inst := p.observe(context.Background(), vm, claim.Name, true)
	if inst.State != provider.InstanceRunning || inst.Endpoint != "198.51.100.20:8080" {
		t.Fatalf("healthy workload = %+v", inst)
	}
	vm.IP = "169.254.169.254"
	if inst := p.observe(context.Background(), vm, claim.Name, true); inst.Endpoint != "" {
		t.Fatalf("published unsafe IP %q", inst.Endpoint)
	}
}

func TestBootstrapRejectsSecretsAndPreservesArguments(t *testing.T) {
	pod, _ := workload()
	pod.Spec.Containers[0].Command = []string{"python3", "-c"}
	pod.Spec.Containers[0].Args = []string{"print('$(date)')"}
	req := provider.ProvisionRequest{Env: provider.Env{Secrets: map[string]string{"SECRET": "private-value"}}}
	_, err := bootstrap(pod, req, 8080)
	if !errors.Is(err, provider.ErrRequestRejected) || strings.Contains(err.Error(), "private-value") {
		t.Fatalf("secret boot input = %v", err)
	}
	req.Env = provider.Env{Config: map[string]string{"MESSAGE": "hello world"}}
	script, err := bootstrap(pod, req, 8080)
	if err != nil || !strings.Contains(script, "--entrypoint 'python3'") ||
		!strings.Contains(script, `'python:3.12-slim' '-c' 'print('\''$(date)'\'')'`) ||
		!strings.Contains(script, "'MESSAGE=hello world'") {
		t.Fatalf("unexpected bootstrap: %v\n%s", err, script)
	}
}

const testInstanceTypes = `{
	"data": {
		"gpu_1x_a10": {
			"instance_type": {
				"architecture": "x86_64",
				"specs": {"vcpus": 30, "memory_gib": 226, "gpus": 1}
			},
			"regions_with_capacity_available": [{"name": "us-west-1"}]
		}
	}
}`

func workload() (*corev1.Pod, *nebula.NodeClaim) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "default", UID: "pod-uid",
			Labels: map[string]string{nebula.AcceleratorTypeLabel: "a10"}},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyAlways, Containers: []corev1.Container{{
			Name: "app", Image: "python:3.12-slim", Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}},
		}}},
	}
	claim := &nebula.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "default-pod", UID: "claim-uid",
			Labels: map[string]string{nebula.ProviderLabel: "lambda"}},
		Spec: nebula.NodeClaimSpec{Provider: "lambda", Region: "us-west-1",
			PodRef: nebula.PodReference{Namespace: "default", Name: "pod", UID: "pod-uid"}},
	}
	return pod, claim
}

func testKube(t *testing.T, claim *nebula.NodeClaim) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := nebula.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(claim).Build()
}

func testProvider(t *testing.T, cloud *httptest.Server, kube client.Client) *Provider {
	t.Helper()
	cat, err := catalog.Load()
	if err != nil {
		t.Fatal(err)
	}
	var api *cloudClient
	if cloud != nil {
		api = testClient(cloud)
	}
	return newProvider(api, kube, kube, cat, settings{
		workspace: "workspace", region: "us-west-1", sshKey: "test-key", firewall: "firewall", cluster: "cluster", port: 8080,
	})
}

func writeInstances(t *testing.T, w http.ResponseWriter, vm *instance, visible bool) {
	t.Helper()
	result := []instance{}
	if visible && vm != nil {
		result = append(result, *vm)
	}
	if err := json.NewEncoder(w).Encode(map[string]any{"data": result}); err != nil {
		t.Error(err)
	}
}

func TestReadinessSurvivesManagerRestart(t *testing.T) {
	ctx := context.Background()
	pod, claim := workload()
	vm := instance{ID: "vm-1", Status: "active", IP: "198.51.100.20", Region: region{Name: "us-west-1"},
		Workspace: "workspace", Tags: claimTags("cluster", claim)}
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeInstances(t, w, &vm, true)
	}))
	defer cloud.Close()
	kube := testKube(t, claim)
	if err := kube.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}

	for _, state := range []struct {
		phase   corev1.PodPhase
		healthy bool
		want    provider.InstanceState
	}{
		{corev1.PodPending, false, provider.InstancePending},
		{corev1.PodPending, true, provider.InstanceRunning},
		{corev1.PodRunning, false, provider.InstanceNotReady},
	} {
		pod.Status.Phase = state.phase
		if err := kube.Status().Update(ctx, pod); err != nil {
			t.Fatal(err)
		}
		p := testProvider(t, cloud, kube)
		p.health = func(context.Context, string) bool { return state.healthy }
		got, err := p.List(ctx)
		if err != nil || len(got) != 1 || got[0].State != state.want {
			t.Fatalf("Pod phase %s and healthy %v: got %+v, %v, want %s", state.phase, state.healthy, got, err, state.want)
		}
	}
}

func TestRefusesTeardownWhenClaimHasMultipleVMs(t *testing.T) {
	ctx := context.Background()
	_, claim := workload()
	claim.Annotations = map[string]string{attemptAnnotation: `{"started":"2026-11-20T00:00:00Z","instanceID":"vm-1"}`}
	first := instance{ID: "vm-1", Status: "active", Region: region{Name: "us-west-1"},
		Workspace: "workspace", Tags: claimTags("cluster", claim)}
	second := first
	second.ID = "vm-2"
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/instances":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []instance{first, second}})
		case "/instances/vm-1":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": first})
		case "/instance-operations/terminate":
			t.Error("termination sent for a claim with ambiguous ownership")
			w.WriteHeader(http.StatusBadRequest)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
		}
	}))
	defer cloud.Close()
	p := testProvider(t, cloud, testKube(t, claim))
	if _, err := p.FindByClaim(ctx, claim.Name, "us-west-1"); err == nil {
		t.Error("lookup accepted a duplicate despite the saved ID")
	}
	if err := p.Terminate(ctx, first.ID, "us-west-1"); err == nil {
		t.Error("termination accepted a duplicate despite the saved ID")
	}
}

func TestAcceptedInstanceNeverObservedStillNeedsCleanup(t *testing.T) {
	ctx := context.Background()
	_, claim := workload()
	claim.Annotations = map[string]string{attemptAnnotation: `{"started":"2020-01-01T00:00:00Z","instanceID":"vm-1"}`}
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/instances" {
			writeInstances(t, w, nil, false)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"code":"global/object-does-not-exist"}}`))
	}))
	defer cloud.Close()
	p := testProvider(t, cloud, testKube(t, claim))
	if _, err := p.FindByClaim(ctx, claim.Name, "us-west-1"); !errors.Is(err, errLaunchUnresolved) {
		t.Fatalf("a never-observed accepted VM lost its cleanup responsibility: %v", err)
	}
	got, err := p.List(ctx)
	if err != nil || len(got) != 1 || got[0].State != provider.InstancePending {
		t.Fatalf("a never-observed accepted VM should still look pending: %+v, %v", got, err)
	}
}

func TestConcurrentProvisionSendsOneLaunch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var launches atomic.Int32
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/instances":
			writeInstances(t, w, nil, false)
		case "/instances/vm-1":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"global/object-does-not-exist"}}`))
		case "/instance-types":
			_, _ = w.Write([]byte(testInstanceTypes))
		case "/instance-operations/launch":
			launches.Add(1)
			_, _ = w.Write([]byte(`{"data":{"instance_ids":["vm-1"]}}`))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer cloud.Close()

	pod, claim := workload()
	kube := testKube(t, claim)
	p := testProvider(t, cloud, kube)
	p.writer = &syncPatches{Writer: kube, both: make(chan struct{})}
	req := provider.ProvisionRequest{ClaimName: claim.Name, Region: "us-west-1"}
	outcome := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := p.Provision(ctx, pod, req)
			outcome <- err
		}()
	}
	succeeded := 0
	for range 2 {
		err := <-outcome
		if err == nil {
			succeeded++
		} else if !errors.Is(err, provider.ErrProvisionUnresolved) {
			t.Errorf("concurrent attempt returned %v", err)
		}
	}
	if launches.Load() != 1 || succeeded != 1 {
		t.Fatalf("launch count %d, successful provisions %d", launches.Load(), succeeded)
	}
}

// Both callers write the same resourceVersion before the API server chooses a winner.
type syncPatches struct {
	client.Writer
	arrived atomic.Int32
	both    chan struct{}
}

func (w *syncPatches) Patch(
	ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption,
) error {
	switch w.arrived.Add(1) {
	case 1:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.both:
		}
	case 2:
		close(w.both)
	}
	return w.Writer.Patch(ctx, obj, patch, opts...)
}
