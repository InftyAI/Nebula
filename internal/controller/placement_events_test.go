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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/failover"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/util"
)

// A Pod left gated carries the reason as an Event, which is all `kubectl describe pod` shows.
func TestPlacement_GatedPodGetsTheReasonAsAnEvent(t *testing.T) {
	onDemand := []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}
	h100 := func() *corev1.Pod { return gatedPod("p1", "default", "uid-1", "pool-a", "H100") }
	modal := func() provider.Provider { return &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}} }

	for _, tc := range []struct {
		name      string
		pod       func() *corev1.Pod
		pool      *nebulav1alpha1.NodePool
		provs     []provider.Provider
		blocklist Blocklister
		want      string // "<type> <reason> <message prefix or fragment>"
	}{{
		name: "no pool label",
		pod: func() *corev1.Pod {
			p := h100()
			delete(p.Labels, nebulav1alpha1.PoolLabel)
			return p
		},
		pool: poolWith("pool-a", onDemand, provider.ProviderModal), provs: []provider.Provider{modal()},
		want: "Warning PlacementFailed the Pod has no " + nebulav1alpha1.PoolLabel + " label",
	}, {
		name:  "pool does not exist",
		pod:   h100,
		pool:  poolWith("pool-b", onDemand, provider.ProviderModal),
		provs: []provider.Provider{modal()},
		want:  `Warning PlacementFailed NodePool "pool-a" named by the Pod's`,
	}, {
		name: "GPU count without a type",
		pod: func() *corev1.Pod {
			p := gatedPod("p1", "default", "uid-1", "pool-a", "")
			p.Spec.Containers[0].Resources.Limits = corev1.ResourceList{util.NvidiaGPUResource: resource.MustParse("1")}
			return p
		},
		pool: poolWith("pool-a", onDemand, provider.ProviderModal), provs: []provider.Provider{modal()},
		want: "Warning PlacementFailed invalid accelerator request: ",
	}, {
		name: "unknown region",
		pod: func() *corev1.Pod {
			p := h100()
			p.Annotations = map[string]string{nebulav1alpha1.RegionsAnnotation: "europe"}
			return p
		},
		pool: poolWith("pool-a", onDemand, provider.ProviderModal), provs: []provider.Provider{modal()},
		want: `Warning PlacementFailed no region in ` + nebulav1alpha1.RegionsAnnotation + ` "europe" is a known geography`,
	}, {
		name: "provider outside the pool",
		pod: func() *corev1.Pod {
			p := h100()
			p.Annotations = map[string]string{nebulav1alpha1.ProvidersAnnotation: "aws"}
			return p
		},
		pool: poolWith("pool-a", onDemand, provider.ProviderModal), provs: []provider.Provider{modal()},
		want: `Warning PlacementFailed no provider in ` + nebulav1alpha1.ProvidersAnnotation +
			` "aws" is in NodePool pool-a (it lists modal)`,
	}, {
		name:  "nothing can serve it",
		pod:   h100,
		pool:  poolWith("pool-a", onDemand, provider.ProviderModal, provider.ProviderRunPod),
		provs: []provider.Provider{&fakeProvider{name: provider.ProviderRunPod, gpus: []string{"A100"}}},
		want: "Warning PlacementFailed no provider in NodePool pool-a can serve H100:1: " +
			"modal not registered; runpod doesn't offer H100:1",
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
		want: "Warning PlacementFailed no provider in NodePool pool-a can serve H100:1: " +
			"kubernetes none of regions [us-east-1 us-west-1] offers H100:1",
	}, {
		name: "every candidate blocked",
		pod:  h100, pool: poolWith("pool-a", onDemand, provider.ProviderModal), provs: []provider.Provider{modal()},
		blocklist: &fakeBlocklist{blocked: []failover.Candidate{{Provider: provider.ProviderModal}}},
		want: "Normal PlacementDeferred every candidate in NodePool pool-a that can serve H100:1 " +
			"failed recently and is held off by failover; retrying in ",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newPlacementReconciler(t, []client.Object{tc.pod(), tc.pool}, tc.provs...)
			recorder := events.NewFakeRecorder(10)
			r.Recorder = recorder
			if tc.blocklist != nil {
				r.Blocklist = tc.blocklist
			}

			reconcilePod(t, r, "default", "p1")

			select {
			case got := <-recorder.Events:
				if !strings.HasPrefix(got, tc.want) {
					t.Errorf("event = %q\nwant prefix %q", got, tc.want)
				}
			default:
				t.Fatal("no event recorded for a Pod left gated")
			}
		})
	}
}

func TestPlacement_PlacedPodGetsNoEvent(t *testing.T) {
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	r, _ := newPlacementReconciler(t, []client.Object{pod, pool},
		&fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}})
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder

	reconcilePod(t, r, "default", "p1")

	if len(recorder.Events) != 0 {
		t.Errorf("events = %q, want none for a placed Pod", <-recorder.Events)
	}
}
