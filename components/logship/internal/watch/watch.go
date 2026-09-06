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

// Package watch turns the cluster's Nebula Pods into the instance set supervise runs, so the process
// finds its own work instead of being told one sandbox on the command line.
//
// It watches Pods rather than the NodeClaims the drain finalizer will read, because one Pod carries
// both halves of what a record needs: the provider's instance id on an annotation, and the tenant
// labels the consumer filters on. Reading them off two objects would mean joining them.
package watch

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/InftyAI/Nebula/components/logship/internal/supervise"
)

// The keys the watch reads a Pod's identity from. Hardcoded, and not configurable: Nebula's placement
// controller and virtual kubelet write all three as a set, so anything that could configure them apart
// could only configure them into disagreement.
//
// Nebula's own group, which is NOT the domain the tenant labels use — those belong to the consumer
// and ride along in instanceFor's clone rather than being read by name. A Pod carries both domains at
// once, so a rename here does not touch them and must not.
const (
	EnabledLabel         = "nebula.inftyai.com/enabled"
	InstanceIDAnnotation = "nebula.inftyai.com/instance-id"

	// ProviderSelector is a spec.nodeSelector key, not a label: it is what routes the Pod to a
	// provider's virtual node, and the only thing on the object that names the provider.
	ProviderSelector = "nebula.inftyai.com/provider"

	EnabledValue = "true"
)

// DefaultResync bounds how long a lost watch event can go uncorrected. The informer relists on it and
// synthesizes the deletes it missed, which is the only thing here that repairs a dropped event.
const DefaultResync = 10 * time.Minute

// instanceFor decides whether a Pod is one to ship, and builds its Instance.
//
// Reading the provider off the nodeSelector is not a formality: InstanceIDAnnotation holds *the
// provider's* id, so it is a sandbox id on one Pod and an EC2 instance id on the next, and an id
// handed to the wrong backend fails every attempt and spends the whole restart budget doing it. The
// nodeSelector is also the only place to read it — nothing on the Pod's labels names the provider.
//
// Which providers can actually be read is not decided here. A Pod on one this build has no backend
// for is still an Instance, so the caller can say so; dropping it here would make it look identical
// to a Pod Nebula never placed.
//
// Phase is deliberately not consulted. An instance's stream ends on its own and supervise does not
// retry a stream that ended, so shipping a terminal Pod costs nothing; stopping at the terminal phase
// would instead cut the tail of the log off, which is the part that says why the run ended.
func instanceFor(pod *corev1.Pod) (supervise.Instance, bool) {
	if pod.Labels[EnabledLabel] != EnabledValue {
		return supervise.Instance{}, false
	}
	provider := pod.Spec.NodeSelector[ProviderSelector]
	if provider == "" {
		return supervise.Instance{}, false
	}
	id := pod.Annotations[InstanceIDAnnotation]
	if id == "" {
		return supervise.Instance{}, false
	}
	return supervise.Instance{
		Provider: provider,
		ID:       id,
		Pod:      pod.Name,
		// Cloned: the informer's object is shared cache state that no handler may retain a piece of,
		// and this map outlives the event that delivered it.
		Labels: maps.Clone(pod.Labels),
	}, true
}

// fleet is the subset of *supervise.Supervisor the watch drives, extracted so tests can assert on the
// calls without building real pipelines.
type fleet interface {
	Ensure(inst supervise.Instance)
	Forget(id string)
	Sync(want []supervise.Instance)
}

// Watcher drives a fleet from the Pods of one cluster. Client and Fleet are required.
type Watcher struct {
	Client kubernetes.Interface
	Fleet  fleet

	// Resync is the informer's relist interval; zero means DefaultResync.
	Resync time.Duration

	// Log matches supervise's, so cmd passes the same function to both. Nil is silent.
	Log func(msg string, keysAndValues ...any)

	// mu guards shipped. The handlers are serialized against each other, but Run calls reconcile once
	// the cache has synced, by which time they are already firing.
	mu sync.Mutex

	// shipped is the instance each Pod was last started for, keyed namespace/name because instance Pod
	// names repeat across the one-namespace-per-org layout.
	//
	// It exists to notice a REPLACED id: re-provisioning rewrites the annotation in place, so the
	// instance it displaced gets no delete event of its own, and Sync — the only other thing that
	// prunes — runs once at startup.
	shipped map[string]string
}

// Run watches until ctx is cancelled, and returns early only if the Pod cache never syncs.
//
// The selector is server-side, so the API server never sends this process a Pod that is not Nebula's.
// That matters at scale, and it also means removing the enabled label arrives here as a delete —
// which is the only way an already-shipping instance stops short of the Pod going away.
func (w *Watcher) Run(ctx context.Context) error {
	resync := w.Resync
	if resync <= 0 {
		resync = DefaultResync
	}
	factory := informers.NewSharedInformerFactoryWithOptions(w.Client, resync,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.LabelSelector = EnabledLabel + "=" + EnabledValue
		}))
	informer := factory.Core().V1().Pods().Informer()

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) { w.ensure(obj) },
		// Ensure on update too, and not as a refresh: the id is absent at CREATE and written when
		// Provision returns one, so for most instances the update IS the event that starts shipping.
		UpdateFunc: func(_, obj any) { w.ensure(obj) },
		DeleteFunc: w.forget,
	}); err != nil {
		return fmt.Errorf("add pod handler: %w", err)
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return fmt.Errorf("pod cache did not sync")
	}
	w.reconcile(informer.GetStore().List())

	<-ctx.Done()
	return nil
}

func (w *Watcher) ensure(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	inst, ok := instanceFor(pod)
	if !ok {
		return
	}
	if old := w.replace(podKey(pod), inst.ID); old != "" {
		// This cuts the replaced instance's streams off mid-flight, losing whatever tail had not
		// shipped — the same loss the drain finalizer will close. Stopping it anyway: the Pod no longer
		// claims that instance, so leaving it running ships two instances' output under one pod name,
		// and nothing else would ever take it out of the tracked set.
		w.log("this Pod's instance was replaced", "pod", pod.Name, "was", old, "now", inst.ID)
		w.Fleet.Forget(old)
	}
	w.Fleet.Ensure(inst)
}

// forget stops an instance when its Pod goes.
//
// Both the id on the object and the one this Pod was started for, because they differ exactly when the
// update that rewrote the annotation is the event that got dropped. Forgetting an id nothing was
// started for is a no-op, so the union is free and the alternative leaks.
func (w *Watcher) forget(obj any) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	started := w.drop(podKey(pod))
	if id := pod.Annotations[InstanceIDAnnotation]; id != "" && id != started {
		w.Fleet.Forget(id)
	}
	if started != "" {
		w.Fleet.Forget(started)
	}
}

// reconcile hands the fleet the whole desired set, for the startup case: nothing can deliver a delete
// event for a Pod that disappeared while this process was down.
func (w *Watcher) reconcile(objs []any) {
	var want []supervise.Instance
	fresh := make(map[string]string, len(objs))
	for _, obj := range objs {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			continue
		}
		if inst, ok := instanceFor(pod); ok {
			want = append(want, inst)
			fresh[podKey(pod)] = inst.ID
		}
	}
	// Replaced wholesale, not merged: Sync is about to make the tracked set exactly want, so any id
	// this map still remembers is one Sync is about to forget.
	w.mu.Lock()
	w.shipped = fresh
	w.mu.Unlock()

	w.log("syncing", "instances", len(want))
	w.Fleet.Sync(want)
}

// replace records the instance now on this Pod and returns the one it displaced, if any.
func (w *Watcher) replace(key, id string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.shipped == nil {
		w.shipped = map[string]string{}
	}
	old := w.shipped[key]
	w.shipped[key] = id
	if old == id {
		return ""
	}
	return old
}

// drop stops tracking the Pod, returning the instance it was started for.
func (w *Watcher) drop(key string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	started := w.shipped[key]
	delete(w.shipped, key)
	return started
}

func podKey(pod *corev1.Pod) string { return pod.Namespace + "/" + pod.Name }

func (w *Watcher) log(msg string, kv ...any) {
	if w.Log != nil {
		w.Log(msg, kv...)
	}
}
