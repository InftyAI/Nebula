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
	"slices"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/InftyAI/Nebula/pkg/provider"
)

func TestAbandonedLaunchRecordDoesNotHoldNodeClaim(t *testing.T) {
	for _, tc := range []struct {
		name     string
		lostAck  bool
		launches int32
	}{
		{name: "reserve patch persisted but its acknowledgement was lost", lostAck: true},
		{name: "Lambda rejected launch before Kubernetes cleanup failed", launches: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			pod, claim := workload()
			kube := testKube(t, claim)
			var launches atomic.Int32
			cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/instances":
					writeInstances(t, w, nil, false)
				case "/instance-types":
					_, _ = w.Write([]byte(testInstanceTypes))
				case "/instance-operations/launch":
					launches.Add(1)
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(`{"error":{"code":"instance-operations/launch/insufficient-capacity"}}`))
				default:
					t.Errorf("unexpected API path %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer cloud.Close()

			p := testProvider(t, cloud, kube)
			p.writer = &unavailableAttemptWriter{Writer: kube, loseFirstAck: tc.lostAck}
			result, err := p.Provision(ctx, pod, provider.ProvisionRequest{ClaimName: claim.Name, Region: "us-west-1"})
			if err == nil || result.InstanceID != "" || launches.Load() != tc.launches {
				t.Fatalf("Provision = %+v, %v, cloud launch requests = %d", result, err, launches.Load())
			}
			if tc.launches == 1 && !errors.Is(err, provider.ErrNoCapacity) {
				t.Fatalf("definitive refusal lost capacity classification: %v", err)
			}
			p.writer = kube // K8s recovers after the create request has returned.
			if got, err := p.FindByClaim(ctx, claim.Name, "us-west-1"); got != nil || err != nil {
				t.Fatalf("FindByClaim still holds cleanup after a known non-launch: %+v, %v", got, err)
			}
			if launches.Load() != tc.launches {
				t.Fatal("cleanup unexpectedly sent a launch")
			}
		})
	}
}

// The first patch reserves the attempt. Later writes fail until the fake API recovers.
type unavailableAttemptWriter struct {
	client.Writer
	loseFirstAck bool
	patched      bool
}

func (w *unavailableAttemptWriter) Patch(
	ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption,
) error {
	if !w.patched {
		w.patched = true
		if err := w.Writer.Patch(ctx, obj, patch, opts...); err != nil || !w.loseFirstAck {
			return err
		}
	}
	return errors.New("Kubernetes unavailable")
}

func TestListIsolatesInvalidLaunchRecord(t *testing.T) {
	ctx := context.Background()
	_, claim := workload()
	bad := claim.DeepCopy()
	bad.Name, bad.UID, bad.ResourceVersion = "broken-claim", "broken-uid", ""
	bad.Annotations = map[string]string{attemptAnnotation: "broken JSON"}
	kube := testKube(t, claim)
	if err := kube.Create(ctx, bad); err != nil {
		t.Fatal(err)
	}
	vm := instance{ID: "ready-vm", Status: "active", IP: "198.51.100.20", Region: region{Name: "us-west-1"},
		Workspace: "workspace", Tags: claimTags("cluster", claim)}
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeInstances(t, w, &vm, true)
	}))
	defer cloud.Close()
	p := testProvider(t, cloud, kube)
	p.health = func(context.Context, string) bool { return true }

	got, err := p.List(ctx)
	if err != nil {
		t.Fatalf("invalid record froze all provider observations: %v", err)
	}
	good := slices.IndexFunc(got, func(in provider.Instance) bool { return in.ClaimName == claim.Name })
	unknown := slices.IndexFunc(got, func(in provider.Instance) bool { return in.ClaimName == bad.Name })
	if good < 0 || got[good].State != provider.InstanceRunning {
		t.Fatalf("valid claim did not advance: %+v", got)
	}
	if unknown < 0 || got[unknown].State != provider.InstanceUnknown || got[unknown].Err == nil || got[unknown].ID != "" {
		t.Fatalf("failed claim must be reported as uncertain: %+v", got)
	}
}

func TestListRecoversTerminatedLaunch(t *testing.T) {
	ctx := context.Background()
	_, claim := workload()
	claim.Annotations = map[string]string{attemptAnnotation: `{"started":"2026-11-20T00:00:00Z"}`}
	vm := instance{ID: "ended-vm", Status: "terminated", Region: region{Name: "us-west-1"},
		Workspace: "workspace", Tags: claimTags("cluster", claim)}
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := json.NewEncoder(w).Encode(map[string]any{"data": []instance{vm}}); err != nil {
			t.Error(err)
		}
	}))
	defer cloud.Close()
	kube := testKube(t, claim)
	p := testProvider(t, cloud, kube)
	got, err := p.List(ctx)
	if err != nil || len(got) != 1 || got[0].ID != "ended-vm" || got[0].State != provider.InstanceTerminated {
		t.Fatalf("List did not report recovered terminal instance: %+v, %v", got, err)
	}
	recovered, err := p.FindByClaim(ctx, claim.Name, "us-west-1")
	if err != nil || recovered == nil || recovered.State != provider.InstanceTerminated {
		t.Fatalf("FindByClaim did not resolve the same observation: %+v, %v", recovered, err)
	}
}

func TestUnsupportedVolumeBlocksLambdaOnlyForTheWorkload(t *testing.T) {
	pod, claim := workload()
	pod.Spec.Volumes = []corev1.Volume{{
		Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}
	p := testProvider(t, nil, testKube(t, claim))
	req := provider.ProvisionRequest{ClaimName: claim.Name, Region: "us-west-1"}
	_, err := p.Provision(context.Background(), pod, req)
	scope := p.ClassifyProvisionError(err, "A10:1", "us-west-1")
	if scope.Deny != provider.DenyWorkload || scope.Region != "us-west-1" {
		t.Fatalf("unsupported workload got %v with block scope %+v", err, scope)
	}
}

func TestAbandonmentFencesLateReservationPatch(t *testing.T) {
	ctx := context.Background()
	pod, claim := workload()
	kube := testKube(t, claim)
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/instances":
			writeInstances(t, w, nil, false)
		case "/instance-types":
			_, _ = w.Write([]byte(testInstanceTypes))
		default:
			t.Errorf("unexpected API request %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer cloud.Close()
	p := testProvider(t, cloud, kube)
	writer := &delayedReservationWriter{Writer: kube}
	p.writer = writer

	if _, err := p.Provision(ctx, pod, provider.ProvisionRequest{ClaimName: claim.Name, Region: "us-west-1"}); err == nil {
		t.Fatal("Provision accepted a reservation with an unconfirmed write")
	}
	if err := writer.commit(); !apierrors.IsConflict(err) {
		t.Fatalf("late reservation was not fenced: %v", err)
	}
	p = testProvider(t, cloud, kube)
	got, err := p.FindByClaim(ctx, claim.Name, "us-west-1")
	if got != nil || err != nil {
		t.Fatalf("restart did not recover the no-launch result: %+v, %v", got, err)
	}
}

type delayedReservationWriter struct {
	client.Writer
	commit func() error
}

func (w *delayedReservationWriter) Patch(
	ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption,
) error {
	if w.commit != nil {
		return w.Writer.Patch(ctx, obj, patch, opts...)
	}
	w.commit = func() error { return w.Writer.Patch(context.Background(), obj, patch, opts...) }
	return errors.New("reservation response timed out before the write")
}
