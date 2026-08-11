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

package fake

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

func testPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "img"}}},
	}
}

func TestProvisionReportsRunningAndLists(t *testing.T) {
	p := New()
	ctx := context.Background()

	id, reserved, err := p.Provision(ctx, testPod(), provider.ProvisionRequest{
		ClaimName:    "claim-a",
		CapacityType: nebulav1alpha1.CapacityOnDemand,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if id == "" {
		t.Fatal("expected a non-empty instance id")
	}
	// The fake has no queueing to model — an instance is Running the moment it is
	// created — so it always reserves. Reporting false would make the fake exercise
	// the Modal-shaped path and leave Pods at Provisioning forever.
	if !reserved {
		t.Fatal("reserved = false; the fake allocates synchronously and is Running immediately")
	}

	// Get reports it Running with the claim recovered.
	inst, err := p.Get(ctx, id)
	if err != nil || inst == nil {
		t.Fatalf("Get = (%v, %v), want a live instance", inst, err)
	}
	if inst.State != provider.InstanceRunning {
		t.Fatalf("state = %q, want Running", inst.State)
	}
	if inst.ClaimName != "claim-a" {
		t.Fatalf("claim = %q, want claim-a", inst.ClaimName)
	}

	// List surfaces exactly the one instance.
	list, err := p.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].ID != id {
		t.Fatalf("List = %v, want one instance %q", list, id)
	}
}

func TestProvisionIdempotentOnClaim(t *testing.T) {
	p := New()
	ctx := context.Background()
	req := provider.ProvisionRequest{ClaimName: "claim-a"}

	id1, _, err := p.Provision(ctx, testPod(), req)
	if err != nil {
		t.Fatalf("Provision #1: %v", err)
	}
	id2, _, err := p.Provision(ctx, testPod(), req)
	if err != nil {
		t.Fatalf("Provision #2: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("ids differ (%q vs %q); Provision must be idempotent on ClaimName", id1, id2)
	}
	if list, _ := p.List(ctx); len(list) != 1 {
		t.Fatalf("expected 1 instance after idempotent re-provision, got %d", len(list))
	}
}

func TestTerminateIsIdempotent(t *testing.T) {
	p := New()
	ctx := context.Background()

	id, _, err := p.Provision(ctx, testPod(), provider.ProvisionRequest{ClaimName: "claim-a"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if err := p.Terminate(ctx, id); err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	// Gone from Get/List.
	if inst, _ := p.Get(ctx, id); inst != nil {
		t.Fatalf("Get after Terminate = %v, want nil (terminated)", inst)
	}
	// A repeat Terminate (and terminating an unknown id) is a no-op, not an error.
	if err := p.Terminate(ctx, id); err != nil {
		t.Fatalf("repeat Terminate: %v", err)
	}
	if err := p.Terminate(ctx, "never-existed"); err != nil {
		t.Fatalf("Terminate unknown id: %v", err)
	}
}

func TestMapAcceleratorFromCatalog(t *testing.T) {
	p := New()
	// A GPU in the fixed catalog resolves (case-insensitively); one that is not
	// offered reports ok=false, so selectPlacement skips the fake for it.
	if _, ok := p.MapAccelerator("h100", 1); !ok {
		t.Fatal("expected H100 to be offered by the fake catalog")
	}
	if _, ok := p.MapAccelerator("TPU-v4", 1); ok {
		t.Fatal("did not expect TPU-v4 to be offered by the fake catalog")
	}
}
