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

package kubernetes

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

const testUID = types.UID("real-uid")

// testCluster is one fake cluster. bind makes the fake "scheduler" bind every created pod.
type testCluster struct {
	kube *kubefake.Clientset
}

func newTestCluster(t *testing.T, bind bool) testCluster {
	t.Helper()
	kube := kubefake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "org-1"}})
	kube.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		pod := a.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
		pod.UID = testUID
		if bind {
			pod.Spec.NodeName = "node-1"
		}
		return false, nil, nil // let the tracker store the mutated pod
	})
	return testCluster{kube: kube}
}

func (c testCluster) clients() clusterClients { return clusterClients{kube: c.kube} }

func provisionRequest() provider.ProvisionRequest {
	return provider.ProvisionRequest{
		ClaimName: "org-1-work-1", Region: "us-east-1",
		CapacityType: nebulav1alpha1.CapacityOnDemand,
		Env:          provider.Env{Config: map[string]string{"PLAIN": "p"}, Secrets: map[string]string{"K": "v"}},
	}
}

func karpenterEvent(reason, msg string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "ev-" + reason, Namespace: "org-1"},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "org-1-work-1", Namespace: "org-1", UID: testUID},
		Reason:         reason, Message: msg,
		Source: corev1.EventSource{Component: "karpenter"},
	}
}

func TestProvisionBoundToWarmNode(t *testing.T) {
	tc := newTestCluster(t, true)
	p := newTestProvider(map[string]clusterClients{"eks-use1": tc.clients()})

	res, err := p.Provision(context.Background(), placeholder(), provisionRequest())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Reserved || res.InstanceID != "us-east-1/org-1/org-1-work-1" {
		t.Fatalf("result = %+v", res)
	}
	sec, err := tc.kube.CoreV1().Secrets("org-1").Get(context.Background(), "org-1-work-1-env", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if sec.StringData["K"] != "v" || len(sec.OwnerReferences) != 1 || sec.OwnerReferences[0].UID != testUID {
		t.Errorf("secret = %+v", sec)
	}

	inst, err := p.FindByClaim(context.Background(), "org-1-work-1", "eks-use1")
	if err != nil || inst == nil || inst.ID != res.InstanceID {
		t.Fatalf("FindByClaim = %+v, %v", inst, err)
	}
	list, err := p.List(context.Background())
	if err != nil || len(list) != 1 || list[0].ClaimName != "org-1-work-1" {
		t.Fatalf("List = %+v, %v", list, err)
	}
	if err := p.Terminate(context.Background(), res.InstanceID, ""); err != nil {
		t.Fatal(err)
	}
	if got, err := p.Get(context.Background(), res.InstanceID, ""); err != nil || got != nil {
		t.Fatalf("after Terminate, Get = %+v, %v", got, err)
	}
	if err := p.Terminate(context.Background(), res.InstanceID, ""); err != nil {
		t.Fatalf("Terminate is not idempotent: %v", err)
	}
}

func TestProvisionNoCapacityDeletesPod(t *testing.T) {
	for _, reason := range []string{reasonInsufficientCapacity, reasonFailedScheduling} {
		t.Run(reason, func(t *testing.T) {
			tc := newTestCluster(t, false)
			if _, err := tc.kube.CoreV1().Events("org-1").Create(context.Background(),
				karpenterEvent(reason, "no p5 capacity"), metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			p := newTestProvider(map[string]clusterClients{"eks-use1": tc.clients()})

			_, err := p.Provision(context.Background(), placeholder(), provisionRequest())
			if !errors.Is(err, provider.ErrNoCapacity) {
				t.Fatalf("err = %v, want ErrNoCapacity", err)
			}
			assertGone(t, tc)
		})
	}
}

func TestProvisionIgnoresSchedulerFailedScheduling(t *testing.T) {
	tc := newTestCluster(t, false)
	ev := karpenterEvent(reasonFailedScheduling, "0/3 nodes are available")
	ev.Source.Component = "default-scheduler"
	if _, err := tc.kube.CoreV1().Events("org-1").Create(context.Background(), ev, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	p := newTestProvider(map[string]clusterClients{"eks-use1": tc.clients()})

	// The scheduler's event is no verdict, so the window closes without one: the pod is
	// accepted unreserved and kept for the poll loop to settle.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	res, err := p.Provision(ctx, placeholder(), provisionRequest())
	if err != nil || res.Reserved || res.InstanceID == "" {
		t.Fatalf("Provision = %+v, %v; want accepted, unreserved", res, err)
	}
	_, err = tc.kube.CoreV1().Pods("org-1").Get(context.Background(), "org-1-work-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("pod was deleted: %v", err)
	}
}

func TestListReportsUnschedulablePastBindTimeout(t *testing.T) {
	tc := newTestCluster(t, false)
	ctx := context.Background()
	unbound := placeholder()
	unbound.Name = "org-1-work-1"
	unbound.Labels = ownerLabels()
	unbound.Spec.NodeName = ""
	unbound.CreationTimestamp = metav1.NewTime(time.Now())
	bound := unbound.DeepCopy()
	bound.Name = "org-1-work-2"
	bound.Spec.NodeName = "node-1"
	for _, pod := range []*corev1.Pod{unbound, bound} {
		if _, err := tc.kube.CoreV1().Pods("org-1").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	p := newTestProvider(map[string]clusterClients{"eks-use1": tc.clients()})

	states := func() map[string]provider.Instance {
		list, err := p.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]provider.Instance{}
		for _, inst := range list {
			out[inst.ClaimName] = inst
		}
		return out
	}
	if got := states()["org-1-work-1"].State; got != provider.InstancePending {
		t.Fatalf("young unbound pod = %s, want Pending", got)
	}

	p.now = func() time.Time { return time.Now().Add(karpenterBindTimeout + time.Second) }
	got := states()
	if got["org-1-work-1"].State != provider.InstanceUnschedulable {
		t.Errorf("old unbound pod = %+v, want Unschedulable", got["org-1-work-1"])
	}
	if got["org-1-work-2"].State == provider.InstanceUnschedulable {
		t.Error("a bound pod must never report Unschedulable, however old")
	}
}

func TestProvisionRejectsUnknownClusterAndPullSecret(t *testing.T) {
	p := newTestProvider(map[string]clusterClients{})
	req := provisionRequest()
	req.Region = "nowhere"
	if _, err := p.Provision(context.Background(), placeholder(), req); err == nil {
		t.Error("unknown cluster: want an error")
	}
	req = provisionRequest()
	req.RegistryAuth = &provider.RegistryAuth{}
	if _, err := p.Provision(context.Background(), placeholder(), req); err == nil {
		t.Error("pull credential: want an error")
	}
}

func TestProvisionCreatesNamespaceInRemoteCluster(t *testing.T) {
	tc := newTestCluster(t, true)
	if err := tc.kube.CoreV1().Namespaces().Delete(context.Background(), "org-1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	p := newTestProvider(map[string]clusterClients{"eks-use1": tc.clients()})
	if _, err := p.Provision(context.Background(), placeholder(), provisionRequest()); err != nil {
		t.Fatal(err)
	}
	ns, err := tc.kube.CoreV1().Namespaces().Get(context.Background(), "org-1", metav1.GetOptions{})
	if err != nil || ns.Labels[nebulav1alpha1.ManagedByLabel] != nebulav1alpha1.ManagedByValue {
		t.Fatalf("namespace = %+v, %v", ns, err)
	}
}

func assertGone(t *testing.T, tc testCluster) {
	t.Helper()
	ctx := context.Background()
	_, err := tc.kube.CoreV1().Pods("org-1").Get(ctx, "org-1-work-1", metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("real pod not deleted: %v", err)
	}
	// The fake clientset has no garbage collector, so what is checked is that a real one would
	// take the env Secret with the pod: it is owned by it, or was never written.
	sec, err := tc.kube.CoreV1().Secrets("org-1").Get(ctx, "org-1-work-1-env", metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		t.Errorf("get env secret: %v", err)
	case len(sec.OwnerReferences) != 1 || sec.OwnerReferences[0].UID != testUID:
		t.Errorf("env secret is not owned by the real pod: %+v", sec.OwnerReferences)
	}
}

func TestKarpenterVerify(t *testing.T) {
	tc := newTestCluster(t, false)
	if err := (karpenter{}).verify(context.Background(), tc.clients()); err == nil {
		t.Fatal("verify passed on a cluster without Karpenter")
	}
	tc.kube.Resources = []*metav1.APIResourceList{{
		GroupVersion: nodeClaimGV.String(),
		APIResources: []metav1.APIResource{{Name: "nodeclaims"}},
	}}
	if err := (karpenter{}).verify(context.Background(), tc.clients()); err != nil {
		t.Fatalf("verify = %v", err)
	}
}

// A new node: the pod binds a while after it was created.
func TestProvisionWaitsForBinding(t *testing.T) {
	tc := newTestCluster(t, false)
	go func() {
		ctx := context.Background()
		for {
			pod, err := tc.kube.CoreV1().Pods("org-1").Get(ctx, "org-1-work-1", metav1.GetOptions{})
			if err == nil {
				pod.Spec.NodeName = "node-1"
				if _, err := tc.kube.CoreV1().Pods("org-1").Update(ctx, pod, metav1.UpdateOptions{}); err == nil {
					return
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	p := newTestProvider(map[string]clusterClients{"eks-use1": tc.clients()})

	res, err := p.Provision(context.Background(), placeholder(), provisionRequest())
	if err != nil || !res.Reserved {
		t.Fatalf("Provision = %+v, %v", res, err)
	}
}

// In the host cluster the placeholder sits beside the real pod; List must see only the latter.
func TestListIgnoresPodsWeDidNotCreate(t *testing.T) {
	tc := newTestCluster(t, true)
	ctx := context.Background()
	half := placeholder()
	half.Name = "users-own"
	half.Labels = map[string]string{nebulav1alpha1.ProviderLabel: provider.ProviderKubernetes}
	for _, pod := range []*corev1.Pod{placeholder(), half} {
		if _, err := tc.kube.CoreV1().Pods("org-1").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	p := newTestProvider(map[string]clusterClients{"eks-use1": tc.clients()})
	if _, err := p.Provision(ctx, placeholder(), provisionRequest()); err != nil {
		t.Fatal(err)
	}

	list, err := p.List(ctx)
	if err != nil || len(list) != 1 || list[0].ClaimName != "org-1-work-1" {
		t.Fatalf("List = %+v, %v; want only the real pod", list, err)
	}
}

// A user's own Secret that happens to carry the env Secret's name is refused, never overwritten.
func TestProvisionRefusesToOverwriteForeignSecret(t *testing.T) {
	tc := newTestCluster(t, true)
	theirs := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "org-1-work-1-env", Namespace: "org-1"},
		StringData: map[string]string{"THEIRS": "keep"},
	}
	if _, err := tc.kube.CoreV1().Secrets("org-1").Create(context.Background(), theirs,
		metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	p := newTestProvider(map[string]clusterClients{"eks-use1": tc.clients()})

	if _, err := p.Provision(context.Background(), placeholder(), provisionRequest()); err == nil {
		t.Fatal("Provision overwrote a Secret Nebula did not create")
	}
	got, _ := tc.kube.CoreV1().Secrets("org-1").Get(context.Background(), "org-1-work-1-env", metav1.GetOptions{})
	if got.StringData["THEIRS"] != "keep" {
		t.Fatalf("the user's Secret was changed: %+v", got.StringData)
	}
}

// Whatever already holds the names is refused before anything is created, so no container can
// start with a Secret that is not this request's; the error blocks nothing, the Pod's owner
// retries.
func TestProvisionRefusesWhatHoldsTheName(t *testing.T) {
	finished := func() *corev1.Pod {
		pod := buildPod(placeholder(), podSpec{claim: "org-1-work-1"})
		pod.Status.Phase = corev1.PodSucceeded
		return pod
	}
	stale := func() *corev1.Secret {
		sec := envSecret("org-1", "org-1-work-1", map[string]string{"K": "stale"})
		sec.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "org-1-work-1", UID: "old"}}
		return sec
	}
	cases := map[string]runtime.Object{
		"earlier pod, finished": finished(),
		"earlier pod's secret":  stale(),
	}
	for name, obj := range cases {
		t.Run(name, func(t *testing.T) {
			tc := newTestCluster(t, true)
			if err := tc.kube.Tracker().Add(obj); err != nil {
				t.Fatal(err)
			}
			p := newTestProvider(map[string]clusterClients{"eks-use1": tc.clients()})

			_, err := p.Provision(context.Background(), placeholder(), provisionRequest())
			if err == nil {
				t.Fatal("Provision took a name already held")
			}
			if scope := p.ClassifyProvisionError(err, "H100:1", "us-east-1"); scope != (provider.BlockScope{}) {
				t.Errorf("a name collision blocked %+v; it says nothing about capacity", scope)
			}
			if sec, err := tc.kube.CoreV1().Secrets("org-1").Get(context.Background(), "org-1-work-1-env",
				metav1.GetOptions{}); err == nil && sec.StringData["K"] == "v" {
				t.Error("the request's Secret was written despite the refusal")
			}
		})
	}
}

// A live pod of ours is the one an interrupted Provision left behind: adopted, with its Secret.
func TestProvisionAdoptsLivePod(t *testing.T) {
	tc := newTestCluster(t, true)
	live := buildPod(placeholder(), podSpec{claim: "org-1-work-1"})
	live.UID, live.Spec.NodeName, live.Status.Phase = testUID, "node-1", corev1.PodRunning
	sec := envSecret("org-1", "org-1-work-1", map[string]string{"K": "v"})
	sec.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "org-1-work-1", UID: testUID}}
	for _, obj := range []runtime.Object{live, sec} {
		if err := tc.kube.Tracker().Add(obj); err != nil {
			t.Fatal(err)
		}
	}
	p := newTestProvider(map[string]clusterClients{"eks-use1": tc.clients()})

	res, err := p.Provision(context.Background(), placeholder(), provisionRequest())
	if err != nil || !res.Reserved || res.InstanceID != "us-east-1/org-1/org-1-work-1" {
		t.Fatalf("Provision = %+v, %v; want the live pod adopted", res, err)
	}
}
