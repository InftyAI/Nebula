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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/InftyAI/Nebula/pkg/provider"
)

// SecretTypeAWSRole marks a Secret carrying the AWS IAM role a provider assumes to pull a
// private ECR image:
//
//	roleARN: arn:aws:iam::123456789012:role/nebula-ecr-pull   # required
//	region:  us-west-2                                        # required, the REGISTRY's region
//
// A dedicated TYPE rather than an Opaque Secret sniffed for known keys: one declared field to
// dispatch on, and room for the other delegated-identity kinds (GCP, Azure) to arrive as
// their own types. It holds no actual secret — an ARN is an identifier — and is a Secret only
// because imagePullSecrets can reference nothing else.
const SecretTypeAWSRole corev1.SecretType = "nebula.inftyai.com/aws-role"

// resolveRegistryAuth builds ProvisionRequest.RegistryAuth from the Pod's imagePullSecrets,
// which name Secrets in the Pod's namespace that an adapter cannot read — the same reason
// resolveEnv lives here. (nil, nil) is an anonymous pull.
//
// The FIRST entry of a recognized type wins, never a merge: a provider attaches ONE
// credential to one image, and the kubelet's own rule (try each until a pull succeeds) is
// unavailable, since the pull happens inside the provider after this returns.
//
// Every other outcome is an error. imagePullSecrets states that this image needs a
// credential, and pulling without one either 401s opaquely or succeeds against a PUBLIC image
// of the same name.
func resolveRegistryAuth(
	ctx context.Context, client kubernetes.Interface, pod *corev1.Pod,
) (*provider.RegistryAuth, error) {
	if len(pod.Spec.ImagePullSecrets) == 0 || len(pod.Spec.Containers) == 0 {
		return nil, nil
	}
	if client == nil {
		// No literals-only path to degrade to, unlike resolveEnv: this is a read or nothing.
		return nil, fmt.Errorf("imagePullSecrets are set but this node has no cluster client")
	}
	// One container per Nebula Pod, as everywhere else that reads the workload.
	registry := registryHost(pod.Spec.Containers[0].Image)

	for _, ref := range pod.Spec.ImagePullSecrets {
		s, err := client.CoreV1().Secrets(pod.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			// Includes NotFound, and non-terminally so: the caller stamps ConfigError and
			// retries, which is right for a Secret written moments after the Pod.
			return nil, fmt.Errorf("read imagePullSecret %q: %w", ref.Name, err)
		}
		switch s.Type {
		case SecretTypeAWSRole:
			arn := strings.TrimSpace(string(s.Data[secretKeyRoleARN]))
			region := strings.TrimSpace(string(s.Data[secretKeyRegion]))
			// Both required. The region is the REGISTRY's, which the ECR host does encode,
			// but a pull-through or replicated reference makes reading it off the image a
			// guess — and a wrong one only ever surfaces as an opaque auth failure.
			if arn == "" || region == "" {
				return nil, fmt.Errorf("imagePullSecret %q of type %s needs a non-empty %q and %q",
					ref.Name, s.Type, secretKeyRoleARN, secretKeyRegion)
			}
			return &provider.RegistryAuth{
				Registry: registry,
				AWSRole:  &provider.AWSRoleAuth{RoleARN: arn, Region: region},
			}, nil

		case corev1.SecretTypeDockerConfigJson:
			auth, err := dockerConfigAuth(s, registry)
			if err != nil {
				return nil, fmt.Errorf("imagePullSecret %q: %w", ref.Name, err)
			}
			return auth, nil
		}
	}
	return nil, fmt.Errorf("no imagePullSecret of a supported type (%s, %s) among %v",
		SecretTypeAWSRole, corev1.SecretTypeDockerConfigJson, pullSecretNames(pod))
}

// The keys SecretTypeAWSRole carries, both required.
const (
	secretKeyRoleARN = "roleARN"
	secretKeyRegion  = "region"
)

// dockerConfigAuth reads registry's entry from a kubernetes.io/dockerconfigjson Secret. Only
// the auths map, never credsStore/credHelpers: a helper names a binary on the machine holding
// the config, and there is no such machine here.
func dockerConfigAuth(s *corev1.Secret, registry string) (*provider.RegistryAuth, error) {
	raw, ok := s.Data[corev1.DockerConfigJsonKey]
	if !ok {
		return nil, fmt.Errorf("no %q key", corev1.DockerConfigJsonKey)
	}
	var cfg struct {
		Auths map[string]struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Auth     string `json:"auth"`
		} `json:"auths"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse %q: %w", corev1.DockerConfigJsonKey, err)
	}

	entry, ok := cfg.Auths[registry]
	if !ok {
		// Docker Hub is the one registry whose config key is historically not its host.
		// Everything else must match exactly — guessing which entry covers a host is how a
		// credential reaches the wrong registry.
		for _, alias := range dockerHubAliases(registry) {
			if entry, ok = cfg.Auths[alias]; ok {
				break
			}
		}
	}
	if !ok {
		return nil, fmt.Errorf("no auths entry for registry %q", registry)
	}

	username, password := entry.Username, entry.Password
	if username == "" && entry.Auth != "" {
		// base64("user:password"), which is what `docker login` writes; the explicit
		// username/password pair is the older form. Both are valid.
		decoded, err := base64.StdEncoding.DecodeString(entry.Auth)
		if err != nil {
			return nil, fmt.Errorf("decode auth for registry %q: %w", registry, err)
		}
		username, password, _ = strings.Cut(string(decoded), ":")
	}
	if username == "" || password == "" {
		return nil, fmt.Errorf("auths entry for registry %q has no usable credential", registry)
	}
	return &provider.RegistryAuth{
		Registry: registry,
		Basic:    &provider.BasicAuth{Username: username, Password: password},
	}, nil
}

// registryHost extracts the registry host from an image reference, by Docker's rule: the
// first segment is a registry only if it looks like a host or is "localhost"; otherwise the
// reference is a Docker Hub name (library/nginx, myorg/app).
func registryHost(image string) string {
	first, _, found := strings.Cut(image, "/")
	if !found {
		return dockerHubRegistry // bare "nginx:1.27"
	}
	if first == "localhost" || strings.ContainsAny(first, ".:") {
		return first
	}
	return dockerHubRegistry
}

// dockerHubRegistry is what an image with no registry resolves to.
const dockerHubRegistry = "docker.io"

// dockerHubAliases returns the other keys a dockerconfigjson may hold Docker Hub credentials
// under — `docker login` writes the v1 index URL to this day. Nil for anything else.
func dockerHubAliases(registry string) []string {
	if registry != dockerHubRegistry {
		return nil
	}
	return []string{"https://index.docker.io/v1/", "index.docker.io", "registry-1.docker.io"}
}

// pullSecretNames lists the Pod's imagePullSecret names, so the "no supported type" error
// names what was found and not only what was wanted.
func pullSecretNames(pod *corev1.Pod) []string {
	out := make([]string, 0, len(pod.Spec.ImagePullSecrets))
	for _, ref := range pod.Spec.ImagePullSecrets {
		out = append(out, ref.Name)
	}
	return out
}
