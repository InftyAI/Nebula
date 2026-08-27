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
// The first USABLE entry wins, never a merge: a provider attaches ONE credential to one image.
// Every earlier entry that did not work out — unreadable, mistyped, malformed, or holding
// credentials for a different registry — is recorded and skipped, not raised. Kubernetes lets
// a Pod list one imagePullSecret per registry precisely so they can be tried in turn, so
// [docker-hub, ghcr] has to pull from GHCR; failing at the first entry made the ORDER decide
// whether a pull worked.
//
// An error only when NOTHING works, and it then carries every reason collected, since which
// one the user meant to be the working entry is unknowable from here. It cannot degrade to an
// anonymous pull: imagePullSecrets states that this image needs a credential, and pulling
// without one either 401s opaquely or succeeds against a PUBLIC image of the same name.
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

	// Why each entry so far was unusable, in the order tried. Collected rather than returned
	// because a later imagePullSecret may still be the right one, and then none of this
	// mattered.
	var problems []string

	for _, ref := range pod.Spec.ImagePullSecrets {
		auth, err := registryAuthFromSecret(ctx, client, pod.Namespace, ref.Name, registry)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		return auth, nil
	}
	// Non-empty by construction: the loop either returned or appended on every entry, and
	// there is at least one.
	return nil, fmt.Errorf("no imagePullSecret yielded a credential for registry %q: %s",
		registry, strings.Join(problems, "; "))
}

// registryAuthFromSecret resolves ONE imagePullSecret, or says why it cannot be used.
//
// It never returns (nil, nil): "this Secret is not the one" is an error here so the caller can
// report it, and the caller decides whether it matters — none of these failures is fatal while
// another imagePullSecret is left to try.
func registryAuthFromSecret(
	ctx context.Context, client kubernetes.Interface, namespace, name, registry string,
) (*provider.RegistryAuth, error) {
	s, err := client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		// Includes NotFound. Still worth retrying when it is the only entry, which the
		// caller's aggregate error gets for free: it stamps ConfigError and comes back, and a
		// Secret written moments after the Pod then resolves.
		return nil, fmt.Errorf("read imagePullSecret %q: %w", name, err)
	}

	switch s.Type {
	case SecretTypeAWSRole:
		arn := strings.TrimSpace(string(s.Data[secretKeyRoleARN]))
		region := strings.TrimSpace(string(s.Data[secretKeyRegion]))
		// Both required. The region is the REGISTRY's, which the ECR host does encode, but a
		// pull-through or replicated reference makes reading it off the image a guess — and a
		// wrong one only ever surfaces as an opaque auth failure.
		if arn == "" || region == "" {
			return nil, fmt.Errorf("imagePullSecret %q of type %s needs a non-empty %q and %q",
				name, s.Type, secretKeyRoleARN, secretKeyRegion)
		}
		return &provider.RegistryAuth{
			Registry: registry,
			AWSRole:  &provider.AWSRoleAuth{RoleARN: arn, Region: region},
		}, nil

	case corev1.SecretTypeDockerConfigJson:
		auth, err := dockerConfigAuth(s, registry)
		if err != nil {
			return nil, fmt.Errorf("imagePullSecret %q: %w", name, err)
		}
		return auth, nil

	default:
		return nil, fmt.Errorf("imagePullSecret %q has unsupported type %s, want %s or %s",
			name, s.Type, SecretTypeAWSRole, corev1.SecretTypeDockerConfigJson)
	}
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
		// A miss, not a fault — the commonest reason a Pod lists more than one
		// imagePullSecret. The caller simply moves on to the next.
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
