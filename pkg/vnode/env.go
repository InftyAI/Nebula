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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// resolveEnv builds the environment a provider gets as ProvisionRequest.Env, following the
// references the Pod makes. The only place envFrom/valueFrom are read, since a provider has
// no cluster access to follow them with.
//
// Kubelet rules: envFrom first in listed order with each Prefix, then env overriding it.
// optional means "the object or key may not exist" — NOT "we may fail to look", so a read
// failure is an error either way. A required miss is an error too, never a silent omission:
// booting a GPU without its credentials bills for a workload that cannot run.
//
// Two divergences, both because there is no machine here: resourceFieldRef and status.*
// fieldRefs are refused (see fieldRefValue), and $(VAR) is not expanded — values pass
// through verbatim.
// TODO: expand $(VAR) here if a workload needs it; the provider only sees the resolved map.
//
// Reads bypass node.go's Secret/ConfigMap informers on purpose: VK waits only for the POD
// informer to sync before CreatePod, and a cold cache reports an existing Secret as absent —
// the one signal this function acts on. A nil client is the usual test seam (literals only).
func resolveEnv(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod) (map[string]string, error) {
	if len(pod.Spec.Containers) == 0 {
		return nil, nil
	}
	// One container per Nebula Pod, as everywhere else that reads the workload.
	c := pod.Spec.Containers[0]
	if len(c.Env) == 0 && len(c.EnvFrom) == 0 {
		return nil, nil
	}

	r := &envResolver{client: client, pod: pod}
	out := make(map[string]string, len(c.Env))
	if err := r.fromSources(ctx, out, c.EnvFrom); err != nil {
		return nil, err
	}
	if err := r.fromVars(ctx, out, c.Env); err != nil {
		return nil, err
	}
	return out, nil
}

// envResolver resolves one container's references, memoizing what it reads so ten
// secretKeyRefs into one Secret cost one GET. Misses are memoized too (a nil entry). It
// lives for a single resolveEnv call, so nothing here can go stale.
type envResolver struct {
	client  kubernetes.Interface
	pod     *corev1.Pod
	secrets map[string]*corev1.Secret
	configs map[string]*corev1.ConfigMap
}

// fromSources applies envFrom in listed order. Every key of each source becomes a variable,
// with the source's Prefix prepended.
func (r *envResolver) fromSources(ctx context.Context, out map[string]string, sources []corev1.EnvFromSource) error {
	for _, src := range sources {
		switch {
		case src.SecretRef != nil:
			s, err := r.secret(ctx, src.SecretRef.Name, optional(src.SecretRef.Optional))
			if err != nil {
				return err
			}
			if s == nil {
				continue // optional and absent
			}
			// Data is the whole content: the API server folds write-only StringData into it.
			for k, v := range s.Data {
				r.put(ctx, out, src.Prefix+k, string(v), "Secret", src.SecretRef.Name)
			}
		case src.ConfigMapRef != nil:
			cm, err := r.configMap(ctx, src.ConfigMapRef.Name, optional(src.ConfigMapRef.Optional))
			if err != nil {
				return err
			}
			if cm == nil {
				continue // optional and absent
			}
			// Data only, as in the kubelet: BinaryData is not text and has no env rendering.
			for k, v := range cm.Data {
				r.put(ctx, out, src.Prefix+k, v, "ConfigMap", src.ConfigMapRef.Name)
			}
		}
	}
	return nil
}

// fromVars applies the explicit env list, overriding anything envFrom contributed.
func (r *envResolver) fromVars(ctx context.Context, out map[string]string, vars []corev1.EnvVar) error {
	for _, e := range vars {
		if e.ValueFrom == nil {
			out[e.Name] = e.Value
			continue
		}
		v, found, err := r.valueFrom(ctx, e.ValueFrom)
		if err != nil {
			return fmt.Errorf("env %q: %w", e.Name, err)
		}
		// Not found means optional-and-absent (a required miss errored), so leave the
		// variable unset, as the kubelet does.
		if found {
			out[e.Name] = v
		}
	}
	return nil
}

// valueFrom resolves one env[].valueFrom. found=false means the reference was optional and
// its object or key is absent; an err means it was required, or the source is one this node
// cannot answer.
func (r *envResolver) valueFrom(ctx context.Context, src *corev1.EnvVarSource) (string, bool, error) {
	switch {
	case src.SecretKeyRef != nil:
		ref := src.SecretKeyRef
		opt := optional(ref.Optional)
		s, err := r.secret(ctx, ref.Name, opt)
		if err != nil || s == nil {
			return "", false, err
		}
		v, ok := s.Data[ref.Key]
		if !ok {
			// Object present, key absent: its own message, because the fix differs.
			if opt {
				return "", false, nil
			}
			return "", false, fmt.Errorf("key %q not in Secret %q", ref.Key, ref.Name)
		}
		return string(v), true, nil

	case src.ConfigMapKeyRef != nil:
		ref := src.ConfigMapKeyRef
		opt := optional(ref.Optional)
		cm, err := r.configMap(ctx, ref.Name, opt)
		if err != nil || cm == nil {
			return "", false, err
		}
		v, ok := cm.Data[ref.Key]
		if !ok {
			if opt {
				return "", false, nil
			}
			return "", false, fmt.Errorf("key %q not in ConfigMap %q", ref.Key, ref.Name)
		}
		return v, true, nil

	case src.FieldRef != nil:
		v, err := fieldRefValue(r.pod, src.FieldRef)
		return v, err == nil, err

	case src.ResourceFieldRef != nil:
		// The kubelet defaults an unset limit to the node's allocatable, and this node
		// advertises a synthetic 1k CPU / 10Ti (virtualCapacity) — so a plausible wrong
		// number would end up in the workload's own sizing (GOMEMLIMIT) unflagged.
		// TODO: answer from the container's explicit requests/limits if a workload needs it.
		return "", false, fmt.Errorf("resourceFieldRef %q is not supported on a virtual node",
			src.ResourceFieldRef.Resource)
	}
	return "", false, fmt.Errorf("valueFrom has no recognized source")
}

// fieldRefValue answers a downward-API fieldRef from the Pod: metadata, plus the two spec
// fields a virtual node knows. status.* is refused — there is no machine here, and a workload
// advertising a placeholder address fails looking like a network problem, not a config one.
func fieldRefValue(pod *corev1.Pod, ref *corev1.ObjectFieldSelector) (string, error) {
	// v1 is the only schema these paths are defined against; anything else is a Pod written
	// against an API this code does not implement.
	if ref.APIVersion != "" && ref.APIVersion != "v1" {
		return "", fmt.Errorf("fieldRef apiVersion %q is not supported", ref.APIVersion)
	}
	switch path := ref.FieldPath; path {
	case "metadata.name":
		return pod.Name, nil
	case "metadata.namespace":
		return pod.Namespace, nil
	case "metadata.uid":
		return string(pod.UID), nil
	case "spec.nodeName":
		// True and useful: the Pod really is bound to this virtual node.
		return pod.Spec.NodeName, nil
	case "spec.serviceAccountName":
		return pod.Spec.ServiceAccountName, nil
	default:
		if k, ok := subscript(path, "metadata.labels"); ok {
			return pod.Labels[k], nil
		}
		if k, ok := subscript(path, "metadata.annotations"); ok {
			return pod.Annotations[k], nil
		}
		return "", fmt.Errorf("fieldRef %q is not supported on a virtual node", path)
	}
}

// subscript parses the metadata.labels['key'] form. A bare metadata.labels (the whole map)
// is volume-only in the downward API, so it yields ok=false as it would on a real kubelet.
func subscript(path, prefix string) (string, bool) {
	rest, ok := strings.CutPrefix(path, prefix+"['")
	if !ok {
		return "", false
	}
	return strings.CutSuffix(rest, "']")
}

// secret reads one Secret from the Pod's namespace, memoized. (nil, nil) means optional and
// absent; a NotFound on a required ref is an error, as is any other read failure whether or
// not the ref is optional.
func (r *envResolver) secret(ctx context.Context, name string, opt bool) (*corev1.Secret, error) {
	s, memoized := r.secrets[name]
	if !memoized {
		if r.client == nil {
			return nil, nil // test seam: no cluster to read from
		}
		got, err := r.client.CoreV1().Secrets(r.pod.Namespace).Get(ctx, name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			s = nil
		case err != nil:
			// Not memoized: a transport failure says nothing about the object.
			return nil, fmt.Errorf("read Secret %q: %w", name, err)
		default:
			s = got
		}
		if r.secrets == nil {
			r.secrets = map[string]*corev1.Secret{}
		}
		r.secrets[name] = s
	}
	if s == nil && !opt {
		return nil, r.absentErr("Secret", name)
	}
	return s, nil
}

// configMap is the ConfigMap half of secret, with the same contract.
func (r *envResolver) configMap(ctx context.Context, name string, opt bool) (*corev1.ConfigMap, error) {
	cm, memoized := r.configs[name]
	if !memoized {
		if r.client == nil {
			return nil, nil // test seam: no cluster to read from
		}
		got, err := r.client.CoreV1().ConfigMaps(r.pod.Namespace).Get(ctx, name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			cm = nil
		case err != nil:
			return nil, fmt.Errorf("read ConfigMap %q: %w", name, err)
		default:
			cm = got
		}
		if r.configs == nil {
			r.configs = map[string]*corev1.ConfigMap{}
		}
		r.configs[name] = cm
	}
	if cm == nil && !opt {
		return nil, r.absentErr("ConfigMap", name)
	}
	return cm, nil
}

// absentErr is the error for a required reference whose object does not exist.
func (r *envResolver) absentErr(kind, name string) error {
	return fmt.Errorf("%s %q not found in namespace %q", kind, name, r.pod.Namespace)
}

// put writes one envFrom-derived variable, dropping a name env cannot carry.
//
// Dropped rather than fatal, as the kubelet does (InvalidEnvironmentVariableNames, container
// still starts): a ConfigMap consumed wholesale carries whatever keys it was written with.
// Logged, because this is the one place a variable disappears without an error. The bar is
// Kubernetes' own, laxer than C_IDENTIFIER — "app.conf" passes, a leading digit does not.
func (r *envResolver) put(ctx context.Context, out map[string]string, name, value, kind, source string) {
	if errs := validation.IsEnvVarName(name); len(errs) > 0 {
		logf.FromContext(ctx).WithName("vnode-env").Info("dropping environment variable with an illegal name",
			"pod", key(r.pod.Namespace, r.pod.Name), "name", name,
			"source", kind+"/"+source, "reason", strings.Join(errs, "; "))
		return
	}
	out[name] = value
}

// optional dereferences the *bool the API uses for optional, where nil means false.
func optional(b *bool) bool { return b != nil && *b }
