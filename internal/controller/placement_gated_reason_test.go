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

package controller

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/failover"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/util"
)

// A Pod left gated carries the reason in its PodScheduled condition's message.
func TestPlacement_GatedPodCarriesTheReason(t *testing.T) {
	onDemand := []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}
	h100 := func() *corev1.Pod { return gatedPod("p1", "default", "uid-1", "pool-a", "H100") }
	modal := func() provider.Provider { return &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}} }

	for _, tc := range []struct {
		name      string
		pod       func() *corev1.Pod
		pool      *nebulav1alpha1.NodePool
		provs     []provider.Provider
		blocklist Blocklister
		want      string // the message, or its prefix
	}{{
		name: "no pool label",
		pod: func() *corev1.Pod {
			p := h100()
			delete(p.Labels, nebulav1alpha1.PoolLabel)
			return p
		},
		pool: poolWith("pool-a", onDemand, provider.ProviderModal), provs: []provider.Provider{modal()},
		want: "the Pod has no " + nebulav1alpha1.PoolLabel + " label",
	}, {
		name:  "pool does not exist",
		pod:   h100,
		pool:  poolWith("pool-b", onDemand, provider.ProviderModal),
		provs: []provider.Provider{modal()},
		want:  `NodePool "pool-a" named by the Pod's`,
	}, {
		name: "GPU count without a type",
		pod: func() *corev1.Pod {
			p := gatedPod("p1", "default", "uid-1", "pool-a", "")
			p.Spec.Containers[0].Resources.Limits = corev1.ResourceList{util.NvidiaGPUResource: resource.MustParse("1")}
			return p
		},
		pool: poolWith("pool-a", onDemand, provider.ProviderModal), provs: []provider.Provider{modal()},
		want: "invalid accelerator request: ",
	}, {
		name: "unknown region",
		pod: func() *corev1.Pod {
			p := h100()
			p.Annotations = map[string]string{nebulav1alpha1.RegionsAnnotation: "europe"}
			return p
		},
		pool: poolWith("pool-a", onDemand, provider.ProviderModal), provs: []provider.Provider{modal()},
		want: `no region in ` + nebulav1alpha1.RegionsAnnotation + ` "europe" is a known geography`,
	}, {
		name: "provider outside the pool",
		pod: func() *corev1.Pod {
			p := h100()
			p.Annotations = map[string]string{nebulav1alpha1.ProvidersAnnotation: "aws"}
			return p
		},
		pool: poolWith("pool-a", onDemand, provider.ProviderModal), provs: []provider.Provider{modal()},
		want: `no provider in ` + nebulav1alpha1.ProvidersAnnotation +
			` "aws" is in NodePool pool-a (it lists modal)`,
	}, {
		name:  "nothing can serve it",
		pod:   h100,
		pool:  poolWith("pool-a", onDemand, provider.ProviderModal, provider.ProviderRunPod),
		provs: []provider.Provider{&fakeProvider{name: provider.ProviderRunPod, gpus: []string{"A100"}}},
		want: "no provider in NodePool pool-a can serve H100:1: " +
			"modal not registered; runpod doesn't offer H100:1",
	}, {
		// OnDemand finds the accelerator missing; the Spot pass must not replace that with
		// the tier the provider lacks.
		name: "a later tier keeps the earlier, specific reason",
		pod:  h100,
		pool: poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand, nebulav1alpha1.CapacitySpot},
			provider.ProviderModal),
		provs: []provider.Provider{&fakeProvider{name: provider.ProviderModal, gpus: []string{"A100"}}},
		want:  "no provider in NodePool pool-a can serve H100:1: modal doesn't offer H100:1",
	}, {
		name: "no region offers the accelerator",
		pod:  h100,
		pool: poolWithRegions("pool-a", onDemand, provider.ProviderKubernetes, "us"),
		provs: []provider.Provider{regionalProvider{
			fakeProvider: &fakeProvider{
				name: provider.ProviderKubernetes, gpus: []string{"H100"},
				expandRegions: func([]string) []string { return []string{"us-east-1", "us-west-1"} },
			},
		}},
		want: "no provider in NodePool pool-a can serve H100:1: " +
			"kubernetes none of regions [us-east-1 us-west-1] offers H100:1",
	}, {
		name: "every candidate blocked",
		pod:  h100, pool: poolWith("pool-a", onDemand, provider.ProviderModal), provs: []provider.Provider{modal()},
		blocklist: &fakeBlocklist{blocked: []failover.Candidate{{Provider: provider.ProviderModal}}},
		want: "every candidate in NodePool pool-a that can serve H100:1 " +
			"failed recently and is held off by failover",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			pod := tc.pod()
			// As the API server sets it for a gated Pod, beside a condition someone else owns.
			pod.Status.Conditions = []corev1.PodCondition{apiServerGatedCondition(), otherCondition()}
			r, c := newPlacementReconciler(t, []client.Object{pod, tc.pool}, tc.provs...)
			if tc.blocklist != nil {
				r.Blocklist = tc.blocklist
			}

			reconcilePod(t, r, "default", "p1")

			got := getPod(t, c, "default", "p1")
			cond := scheduledCondition(t, got)
			if !strings.HasPrefix(cond.Message, tc.want) {
				t.Errorf("message = %q\nwant prefix %q", cond.Message, tc.want)
			}
			if cond.Status != corev1.ConditionFalse || cond.Reason != corev1.PodReasonSchedulingGated {
				t.Errorf("condition = %s/%s, want False/SchedulingGated so kubectl still reads it", cond.Status, cond.Reason)
			}
			if !cond.LastTransitionTime.Equal(&gatedSince) {
				t.Errorf("lastTransitionTime = %v, want %v: the status did not change", cond.LastTransitionTime, gatedSince)
			}
			if len(got.Status.Conditions) != 2 {
				t.Errorf("conditions = %+v, want the other one kept", got.Status.Conditions)
			}
		})
	}
}

// A placed Pod's condition is left to the scheduler, which replaces it at bind.
func TestPlacement_PlacedPodKeepsTheAPIServersMessage(t *testing.T) {
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pod.Status.Conditions = []corev1.PodCondition{apiServerGatedCondition()}
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	r, c := newPlacementReconciler(t, []client.Object{pod, pool},
		&fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}})

	reconcilePod(t, r, "default", "p1")

	if msg := scheduledCondition(t, getPod(t, c, "default", "p1")).Message; msg != apiServerGatedCondition().Message {
		t.Errorf("message = %q, want the API server's", msg)
	}
}

var gatedSince = metav1.NewTime(metav1.Now().Rfc3339Copy().Add(-time.Hour))

func apiServerGatedCondition() corev1.PodCondition {
	return corev1.PodCondition{
		Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonSchedulingGated,
		Message: "Scheduling is blocked due to non-empty scheduling gates", LastTransitionTime: gatedSince,
	}
}

func otherCondition() corev1.PodCondition {
	return corev1.PodCondition{Type: "example.com/Ready", Status: corev1.ConditionFalse}
}

func scheduledCondition(t *testing.T, pod *corev1.Pod) corev1.PodCondition {
	t.Helper()
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled {
			return c
		}
	}
	t.Fatalf("no PodScheduled condition in %+v", pod.Status.Conditions)
	return corev1.PodCondition{}
}
