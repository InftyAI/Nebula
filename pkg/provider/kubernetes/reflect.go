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
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/metrics"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// Reflection copies the home cluster's Secrets and ConfigMaps that a rule selects (see
// ReflectionConfig) into the same namespace of every non-host cluster, so a real pod there can
// reference them by name as it would at home. Copies have no owner reference: a copy is deleted
// when its source is deleted or stops being selected, and reflectSweep cleans up after missed
// events. An existing object that isn't a copy (see isReflected) is never touched; the source
// gets a Warning Event instead.

var _ provider.Reflector = (*Provider)(nil)

const (
	resourceSecrets    = "secrets"
	resourceConfigMaps = "configmaps"

	reflectWorkers = 2
	// reflectSweep re-reflects every selected source and every copy regardless of events, to repair
	// copies edited or deleted in a target and orphans left behind while Nebula was down.
	reflectSweep = 10 * time.Minute
	// reflectTimeout caps reflecting one object to one target, so a stuck cluster doesn't delay
	// the others.
	reflectTimeout = 30 * time.Second

	reasonReflectionRefused = "ReflectionRefused"
)

// reflectRule is a validated ReflectionRule with its selectors parsed.
type reflectRule struct {
	resources  []string
	namespaces labels.Selector
	objects    labels.Selector
}

// compile validates and parses the rules. objectSelector must be set and non-empty, because an
// empty selector matches every object.
func (c ReflectionConfig) compile() ([]reflectRule, error) {
	out := make([]reflectRule, 0, len(c.Rules))
	for i, r := range c.Rules {
		if len(r.Resources) == 0 {
			return nil, fmt.Errorf("kubernetes: reflection rule %d lists no resources", i)
		}
		for _, res := range r.Resources {
			if res != resourceSecrets && res != resourceConfigMaps {
				return nil, fmt.Errorf("kubernetes: reflection rule %d: resource %q is not %q or %q",
					i, res, resourceSecrets, resourceConfigMaps)
			}
		}
		// TODO: support a DenyList mode, where objectSelector names the objects to exclude.
		if r.Mode != "" && r.Mode != ReflectionAllowList {
			return nil, fmt.Errorf("kubernetes: reflection rule %d: mode %q is not supported (want %q)",
				i, r.Mode, ReflectionAllowList)
		}
		if r.ObjectSelector == nil || len(r.ObjectSelector.MatchLabels)+len(r.ObjectSelector.MatchExpressions) == 0 {
			return nil, fmt.Errorf("kubernetes: reflection rule %d: objectSelector is required and must not be empty", i)
		}
		if key, ok := nebulaKey(r.ObjectSelector); ok {
			return nil, fmt.Errorf("kubernetes: reflection rule %d: objectSelector key %q is Nebula's own; "+
				"select by your own labels", i, key)
		}
		objects, err := metav1.LabelSelectorAsSelector(r.ObjectSelector)
		if err != nil {
			return nil, fmt.Errorf("kubernetes: reflection rule %d: objectSelector: %w", i, err)
		}
		namespaces := labels.Everything()
		if r.NamespaceSelector != nil {
			if namespaces, err = metav1.LabelSelectorAsSelector(r.NamespaceSelector); err != nil {
				return nil, fmt.Errorf("kubernetes: reflection rule %d: namespaceSelector: %w", i, err)
			}
		}
		out = append(out, reflectRule{resources: r.Resources, namespaces: namespaces, objects: objects})
	}
	return out, nil
}

// nebulaKey returns a key of sel under Nebula's label prefix. Those keys are reserved for Nebula
// (see ParsePropagateLabels), so a rule may not select on them.
func nebulaKey(sel *metav1.LabelSelector) (string, bool) {
	keys := slices.Collect(maps.Keys(sel.MatchLabels))
	for _, e := range sel.MatchExpressions {
		keys = append(keys, e.Key)
	}
	for _, k := range keys {
		if strings.HasPrefix(k, nebulav1alpha1.GroupVersion.Group+"/") {
			return k, true
		}
	}
	return "", false
}

// objectKey identifies a source. Its copies have the same namespace and name in every target.
type objectKey struct {
	resource, namespace, name string
}

type reflector struct {
	rules      []reflectRule
	targets    []cluster
	secrets    corev1listers.SecretLister
	configMaps corev1listers.ConfigMapLister
	namespaces corev1listers.NamespaceLister
	events     record.EventRecorder
	queue      workqueue.TypedRateLimitingInterface[objectKey]

	// reflectTo and timeout are overridden in tests, because the fake clientset ignores deadlines.
	reflectTo func(context.Context, kubeClient, objectKey, runtime.Object) (bool, error)
	timeout   time.Duration
}

// Reflect implements provider.Reflector.
func (p *Provider) Reflect(ctx context.Context, home provider.ReflectSources) error {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: home.Client.CoreV1().Events("")})
	r := &reflector{
		rules:      p.reflection,
		targets:    p.reflectTargets(),
		secrets:    home.Secrets.Lister(),
		configMaps: home.ConfigMaps.Lister(),
		namespaces: home.Namespaces.Lister(),
		events:     broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "nebula-reflect"}),
		queue:      newReflectQueue(),
		reflectTo:  reflectTo,
		timeout:    reflectTimeout,
	}
	informers := map[string]cache.SharedIndexInformer{
		resourceSecrets: home.Secrets.Informer(), resourceConfigMaps: home.ConfigMaps.Informer(),
	}
	for resource, inf := range informers {
		if _, err := inf.AddEventHandler(r.objectEvents(resource)); err != nil {
			return fmt.Errorf("kubernetes: watch %s to reflect: %w", resource, err)
		}
	}
	nsInformer := home.Namespaces.Informer()
	if _, err := nsInformer.AddEventHandler(r.namespaceEvents()); err != nil {
		return fmt.Errorf("kubernetes: watch namespaces to reflect: %w", err)
	}
	go func() {
		defer broadcaster.Shutdown()
		r.run(ctx, informers[resourceSecrets].HasSynced, informers[resourceConfigMaps].HasSynced, nsInformer.HasSynced)
	}()
	return nil
}

func newReflectQueue() workqueue.TypedRateLimitingInterface[objectKey] {
	return workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[objectKey](),
		workqueue.TypedRateLimitingQueueConfig[objectKey]{Name: "nebula-reflect"},
	)
}

// reflectTargets returns the clusters copies go to: every cluster except the host, which holds
// the sources.
func (p *Provider) reflectTargets() []cluster {
	var out []cluster
	for _, region := range p.order {
		if cl := p.clusters[region]; !cl.cfg.Host {
			out = append(out, cl)
		}
	}
	return out
}

// mayBeSelected checks only the object's own labels, not its namespace's. Event handlers use it as
// a pre-filter because the namespace cache may not have synced yet; handle makes the final call.
func (r *reflector) mayBeSelected(resource string, objLabels map[string]string) bool {
	return slices.ContainsFunc(r.rules, func(rule reflectRule) bool {
		return slices.Contains(rule.resources, resource) && rule.objects.Matches(labels.Set(objLabels))
	})
}

func (r *reflector) selected(resource string, obj metav1.Object) bool {
	ns, err := r.namespaces.Get(obj.GetNamespace())
	if err != nil {
		return false
	}
	return slices.ContainsFunc(r.rules, func(rule reflectRule) bool {
		return slices.Contains(rule.resources, resource) &&
			rule.namespaces.Matches(labels.Set(ns.Labels)) && rule.objects.Matches(labels.Set(obj.GetLabels()))
	})
}

// objectEvents queues a source that may be selected now or may have been before this update, so
// its copy is also deleted once it stops matching.
func (r *reflector) objectEvents(resource string) cache.ResourceEventHandler {
	queue := func(obj any) {
		if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = t.Obj
		}
		if m, ok := obj.(metav1.Object); ok && r.mayBeSelected(resource, m.GetLabels()) {
			r.queue.Add(objectKey{resource, m.GetNamespace(), m.GetName()})
		}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc: queue,
		UpdateFunc: func(old, cur any) {
			o, ok1 := old.(metav1.Object)
			c, ok2 := cur.(metav1.Object)
			if !ok1 || !ok2 || o.GetResourceVersion() == c.GetResourceVersion() {
				return
			}
			queue(old)
			queue(cur)
		},
		DeleteFunc: queue,
	}
}

// namespaceEvents re-queues a namespace's candidate sources when its labels change, since that can
// change which namespaceSelectors match.
func (r *reflector) namespaceEvents() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if ns, ok := obj.(*corev1.Namespace); ok {
				r.queueNamespace(ns.Name)
			}
		},
		UpdateFunc: func(old, cur any) {
			o, ok1 := old.(*corev1.Namespace)
			c, ok2 := cur.(*corev1.Namespace)
			if ok1 && ok2 && !maps.Equal(o.Labels, c.Labels) {
				r.queueNamespace(c.Name)
			}
		},
	}
}

// queueNamespace queues the candidate sources in namespace, or in every namespace for NamespaceAll.
func (r *reflector) queueNamespace(namespace string) {
	secrets, _ := r.secrets.Secrets(namespace).List(labels.Everything())
	for _, s := range secrets {
		if r.mayBeSelected(resourceSecrets, s.Labels) {
			r.queue.Add(objectKey{resourceSecrets, s.Namespace, s.Name})
		}
	}
	configMaps, _ := r.configMaps.ConfigMaps(namespace).List(labels.Everything())
	for _, c := range configMaps {
		if r.mayBeSelected(resourceConfigMaps, c.Labels) {
			r.queue.Add(objectKey{resourceConfigMaps, c.Namespace, c.Name})
		}
	}
}

func (r *reflector) run(ctx context.Context, synced ...cache.InformerSynced) {
	defer r.queue.ShutDown()
	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		return
	}
	for range reflectWorkers {
		go func() {
			for r.next(ctx) {
			}
		}()
	}
	t := time.NewTicker(reflectSweep)
	defer t.Stop()
	for {
		r.sweep(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// sweep queues every candidate source at home and every existing copy in each target.
func (r *reflector) sweep(ctx context.Context) {
	r.queueNamespace(metav1.NamespaceAll)
	log := logf.FromContext(ctx).WithName("kubernetes-reflect")
	copies := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(reflectedLabels()).String()}
	for _, cl := range r.targets {
		kube := cl.clients.kube
		secrets, err := kube.CoreV1().Secrets(metav1.NamespaceAll).List(ctx, copies)
		if err != nil {
			log.Error(err, "cannot list the copied Secrets; the next sweep retries", "cluster", cl.cfg.Name)
		} else {
			for _, s := range secrets.Items {
				r.queue.Add(objectKey{resourceSecrets, s.Namespace, s.Name})
			}
		}
		configMaps, err := kube.CoreV1().ConfigMaps(metav1.NamespaceAll).List(ctx, copies)
		if err != nil {
			log.Error(err, "cannot list the copied ConfigMaps; the next sweep retries", "cluster", cl.cfg.Name)
		} else {
			for _, c := range configMaps.Items {
				r.queue.Add(objectKey{resourceConfigMaps, c.Namespace, c.Name})
			}
		}
	}
}

// next processes one queued key. It returns false once the queue has shut down.
func (r *reflector) next(ctx context.Context) bool {
	key, shutdown := r.queue.Get()
	if shutdown {
		return false
	}
	defer r.queue.Done(key)
	if err := r.handle(ctx, key); err != nil {
		logf.FromContext(ctx).WithName("kubernetes-reflect").Error(err, "cannot reflect; will retry",
			"resource", key.resource, "namespace", key.namespace, "name", key.name)
		r.queue.AddRateLimited(key)
		return true
	}
	r.queue.Forget(key)
	return true
}

// handle syncs every target's copy of key with its source: written while the source is selected,
// deleted otherwise.
func (r *reflector) handle(ctx context.Context, key objectKey) error {
	var src runtime.Object
	var err error
	switch key.resource {
	case resourceSecrets:
		src, err = r.secrets.Secrets(key.namespace).Get(key.name)
	case resourceConfigMaps:
		src, err = r.configMaps.ConfigMaps(key.namespace).Get(key.name)
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err != nil || !r.selected(key.resource, src.(metav1.Object)) {
		src = nil
	}
	var errs []error
	for _, cl := range r.targets {
		callCtx, cancel := context.WithTimeout(ctx, r.timeout)
		refused, err := r.reflectTo(callCtx, cl.clients.kube, key, src)
		cancel()
		result := metrics.ReflectSucceeded
		switch {
		case err != nil:
			result = metrics.ReflectFailed
			errs = append(errs, fmt.Errorf("cluster %s: %w", cl.cfg.Name, err))
		case refused:
			result = metrics.ReflectRefused
			r.events.Eventf(src, corev1.EventTypeWarning, reasonReflectionRefused,
				"Not reflected to cluster %s, which has an object of this name that isn't Nebula's copy",
				cl.cfg.Name)
		}
		metrics.Reflections.WithLabelValues(cl.cfg.Name, result).Inc()
	}
	return errors.Join(errs...)
}

// reflectTo writes src's copy into kube, or deletes key's copy when src is nil. refused means an
// object of that name exists but isn't a copy, so it was left untouched.
func reflectTo(ctx context.Context, kube kubeClient, key objectKey, src runtime.Object) (refused bool, err error) {
	switch s := src.(type) {
	case *corev1.Secret:
		return applySecret(ctx, kube, s)
	case *corev1.ConfigMap:
		return applyConfigMap(ctx, kube, s)
	}
	if key.resource == resourceSecrets {
		secrets := kube.CoreV1().Secrets(key.namespace)
		return false, deleteCopy(ctx, secrets.Get, secrets.Delete, key.name)
	}
	configMaps := kube.CoreV1().ConfigMaps(key.namespace)
	return false, deleteCopy(ctx, configMaps.Get, configMaps.Delete, key.name)
}

// deleteCopy deletes the copy called name, if any. A non-copy object of that name is kept.
func deleteCopy[T metav1.Object](
	ctx context.Context,
	get func(context.Context, string, metav1.GetOptions) (T, error),
	del func(context.Context, string, metav1.DeleteOptions) error,
	name string,
) error {
	got, err := get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return classifyAPI(err)
	case !isReflected(got):
		return nil
	}
	// The UID precondition spares a copy recreated after the Get, e.g. for a source selected again.
	err = del(ctx, name, *metav1.NewPreconditionDeleteOptions(string(got.GetUID())))
	if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return classifyAPI(err)
	}
	return nil
}

// applySecret creates or updates kube's copy of src with src's type and data, creating the
// namespace first if it is missing.
func applySecret(ctx context.Context, kube kubeClient, src *corev1.Secret) (refused bool, err error) {
	secrets := kube.CoreV1().Secrets(src.Namespace)
	data := secretData(src)
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		got, err := secrets.Get(ctx, src.Name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			if err := ensureNamespace(ctx, kube, src.Namespace); err != nil {
				return err
			}
			_, err = secrets.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: src.Name, Namespace: src.Namespace, Labels: reflectedLabels()},
				Type:       src.Type,
				Data:       data,
			}, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				return apierrors.NewConflict(corev1.Resource(resourceSecrets), src.Name, err)
			}
			return err
		case err != nil:
			return err
		case got.UID == src.UID:
			return nil // the source itself: this target is the home cluster
		case !isReflected(got):
			refused = true
			return nil
		case got.Type != src.Type:
			// A Secret's type is immutable, so delete the copy and let the retry recreate it.
			err := secrets.Delete(ctx, src.Name, *metav1.NewPreconditionDeleteOptions(string(got.UID)))
			if err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			return apierrors.NewConflict(corev1.Resource(resourceSecrets), src.Name, errors.New("type changed"))
		case maps.EqualFunc(got.Data, data, bytes.Equal):
			return nil
		}
		got.Data, got.StringData = data, nil
		_, err = secrets.Update(ctx, got, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return false, classifyAPI(err)
	}
	return refused, nil
}

// applyConfigMap is applySecret for a ConfigMap.
func applyConfigMap(ctx context.Context, kube kubeClient, src *corev1.ConfigMap) (refused bool, err error) {
	configMaps := kube.CoreV1().ConfigMaps(src.Namespace)
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		got, err := configMaps.Get(ctx, src.Name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			if err := ensureNamespace(ctx, kube, src.Namespace); err != nil {
				return err
			}
			_, err = configMaps.Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: src.Name, Namespace: src.Namespace, Labels: reflectedLabels()},
				Data:       maps.Clone(src.Data),
				BinaryData: maps.Clone(src.BinaryData),
			}, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				return apierrors.NewConflict(corev1.Resource(resourceConfigMaps), src.Name, err)
			}
			return err
		case err != nil:
			return err
		case got.UID == src.UID:
			return nil
		case !isReflected(got):
			refused = true
			return nil
		case maps.Equal(got.Data, src.Data) && maps.EqualFunc(got.BinaryData, src.BinaryData, bytes.Equal):
			return nil
		}
		got.Data, got.BinaryData = maps.Clone(src.Data), maps.Clone(src.BinaryData)
		_, err = configMaps.Update(ctx, got, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return false, classifyAPI(err)
	}
	return refused, nil
}

// secretData returns s's data as the API server stores it, with StringData merged into Data.
func secretData(s *corev1.Secret) map[string][]byte {
	out := maps.Clone(s.Data)
	if out == nil {
		out = map[string][]byte{}
	}
	for k, v := range s.StringData {
		out[k] = []byte(v)
	}
	return out
}
