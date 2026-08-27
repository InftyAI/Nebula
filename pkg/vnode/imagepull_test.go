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
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/InftyAI/Nebula/pkg/provider"
)

// pullPod is a Pod pulling image with the named imagePullSecrets.
func pullPod(image string, secretNames ...string) *corev1.Pod {
	pod := testPod("default", "p1")
	pod.Spec.Containers[0].Image = image
	for _, n := range secretNames {
		pod.Spec.ImagePullSecrets = append(pod.Spec.ImagePullSecrets,
			corev1.LocalObjectReference{Name: n})
	}
	return pod
}

// typedSecret is a Secret of an explicit type, which is what resolveRegistryAuth dispatches
// on (secretObj in env_test.go leaves the type empty).
func typedSecret(name string, t corev1.SecretType, data map[string]string) *corev1.Secret {
	s := secretObj(name, data)
	s.Type = t
	return s
}

// dockerConfigSecret renders a kubernetes.io/dockerconfigjson Secret for one registry. Uses
// the base64 auth form, since that is what `docker login` writes.
func dockerConfigSecret(name, registry, user, password string) *corev1.Secret {
	auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + password))
	return typedSecret(name, corev1.SecretTypeDockerConfigJson, map[string]string{
		corev1.DockerConfigJsonKey: fmt.Sprintf(`{"auths":{%q:{"auth":%q}}}`, registry, auth),
	})
}

const ecrImage = "123456789012.dkr.ecr.us-west-2.amazonaws.com/team/trainer:v3"

func TestResolveRegistryAuth(t *testing.T) {
	cases := []struct {
		name    string
		pod     *corev1.Pod
		objs    []runtime.Object
		want    *provider.RegistryAuth
		wantErr string
	}{
		{
			// The overwhelmingly common case: a public image and no credential at all.
			name: "no imagePullSecrets is an anonymous pull",
			pod:  pullPod("nginx:1.27"),
		},
		{
			name: "aws-role Secret yields the role, region from the Secret",
			pod:  pullPod(ecrImage, "ecr-pull"),
			objs: []runtime.Object{typedSecret("ecr-pull", SecretTypeAWSRole, map[string]string{
				"roleARN": "arn:aws:iam::123456789012:role/pull",
				"region":  "eu-central-1",
			})},
			want: &provider.RegistryAuth{
				Registry: "123456789012.dkr.ecr.us-west-2.amazonaws.com",
				AWSRole: &provider.AWSRoleAuth{
					RoleARN: "arn:aws:iam::123456789012:role/pull",
					Region:  "eu-central-1",
				},
			},
		},
		{
			// Not derived from the image host: the region is stated or it is an error.
			name: "an aws-role Secret without a region is an error",
			pod:  pullPod(ecrImage, "ecr-pull"),
			objs: []runtime.Object{typedSecret("ecr-pull", SecretTypeAWSRole, map[string]string{
				"roleARN": "arn:aws:iam::123456789012:role/pull",
			})},
			wantErr: `needs a non-empty "roleARN" and "region"`,
		},
		{
			name: "whitespace around the values is trimmed",
			pod:  pullPod(ecrImage, "ecr-pull"),
			objs: []runtime.Object{typedSecret("ecr-pull", SecretTypeAWSRole, map[string]string{
				"roleARN": "  arn:aws:iam::123456789012:role/pull\n",
				"region":  " us-west-2 ",
			})},
			want: &provider.RegistryAuth{
				Registry: "123456789012.dkr.ecr.us-west-2.amazonaws.com",
				AWSRole: &provider.AWSRoleAuth{
					RoleARN: "arn:aws:iam::123456789012:role/pull",
					Region:  "us-west-2",
				},
			},
		},
		{
			name: "dockerconfigjson yields a basic credential",
			pod:  pullPod("ghcr.io/org/app:v1", "ghcr"),
			objs: []runtime.Object{dockerConfigSecret("ghcr", "ghcr.io", "bot", "hunter2")},
			want: &provider.RegistryAuth{
				Registry: "ghcr.io",
				Basic:    &provider.BasicAuth{Username: "bot", Password: "hunter2"},
			},
		},
		{
			// A registry-less image is Docker Hub, whose config key is the legacy index URL.
			name: "docker hub matches its legacy index key",
			pod:  pullPod("myorg/app:v1", "hub"),
			objs: []runtime.Object{
				dockerConfigSecret("hub", "https://index.docker.io/v1/", "u", "p"),
			},
			want: &provider.RegistryAuth{
				Registry: "docker.io",
				Basic:    &provider.BasicAuth{Username: "u", Password: "p"},
			},
		},
		{
			// A credential for another registry must not be handed to this one.
			name:    "a dockerconfigjson for a different registry is an error",
			pod:     pullPod("ghcr.io/org/app:v1", "hub"),
			objs:    []runtime.Object{dockerConfigSecret("hub", "docker.io", "u", "p")},
			wantErr: `no auths entry for registry "ghcr.io"`,
		},
		{
			// The first RECOGNIZED type wins, so an unknown type earlier is skipped over.
			name: "the first recognized type wins",
			pod:  pullPod(ecrImage, "opaque", "ecr-pull"),
			objs: []runtime.Object{
				secretObj("opaque", map[string]string{"roleARN": "arn:aws:iam::1:role/ignored"}),
				typedSecret("ecr-pull", SecretTypeAWSRole, map[string]string{
					"roleARN": "arn:aws:iam::123456789012:role/pull",
					"region":  "us-west-2",
				}),
			},
			want: &provider.RegistryAuth{
				Registry: "123456789012.dkr.ecr.us-west-2.amazonaws.com",
				AWSRole: &provider.AWSRoleAuth{
					RoleARN: "arn:aws:iam::123456789012:role/pull",
					Region:  "us-west-2",
				},
			},
		},
		{
			// NOT an anonymous-pull downgrade: the Pod asked for a credential.
			name:    "a missing Secret is an error",
			pod:     pullPod(ecrImage, "ecr-pull"),
			wantErr: `read imagePullSecret "ecr-pull"`,
		},
		{
			name: "an aws-role Secret without a roleARN is an error",
			pod:  pullPod(ecrImage, "ecr-pull"),
			objs: []runtime.Object{
				typedSecret("ecr-pull", SecretTypeAWSRole, map[string]string{"region": "us-west-2"}),
			},
			wantErr: `needs a non-empty "roleARN" and "region"`,
		},
		{
			name:    "no Secret of a supported type is an error",
			pod:     pullPod(ecrImage, "opaque"),
			objs:    []runtime.Object{secretObj("opaque", map[string]string{"roleARN": "arn:x"})},
			wantErr: "no imagePullSecret of a supported type",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveRegistryAuth(context.Background(), fake.NewSimpleClientset(tc.objs...), tc.pod)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("got nil error, want one containing %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
				}
				if got != nil {
					t.Errorf("auth = %v on error, want nil — a partial credential must not reach a provider", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := authDiff(got, tc.want); diff != "" {
				t.Errorf("auth mismatch: %s", diff)
			}
		})
	}
}

// TestResolveRegistryAuthNoClient covers the seam other vnode tests rely on: a nil client is
// fine until a Pod actually references a Secret, which is a read or nothing.
func TestResolveRegistryAuthNoClient(t *testing.T) {
	if _, err := resolveRegistryAuth(context.Background(), nil, pullPod("nginx:1.27")); err != nil {
		t.Fatalf("no imagePullSecrets with a nil client: %v", err)
	}
	_, err := resolveRegistryAuth(context.Background(), nil, pullPod(ecrImage, "ecr-pull"))
	if err == nil {
		t.Fatal("imagePullSecrets with a nil client: got nil error, want one")
	}
}

func TestRegistryHost(t *testing.T) {
	cases := map[string]string{
		"nginx:1.27":                   "docker.io",
		"myorg/app:v1":                 "docker.io",
		"library/nginx":                "docker.io",
		"ghcr.io/org/app:v1":           "ghcr.io",
		"localhost:5000/app":           "localhost:5000",
		"localhost/app":                "localhost",
		ecrImage:                       "123456789012.dkr.ecr.us-west-2.amazonaws.com",
		"registry.example.com:443/a/b": "registry.example.com:443",
	}
	for image, want := range cases {
		if got := registryHost(image); got != want {
			t.Errorf("registryHost(%q) = %q, want %q", image, got, want)
		}
	}
}

// TestRegistryAuthStringRedacts pins the one thing that must never regress: a password does
// not print, while the role ARN does (it is an identifier, and the actionable part of a
// "cannot assume this role" report).
func TestRegistryAuthStringRedacts(t *testing.T) {
	basic := &provider.RegistryAuth{
		Registry: "ghcr.io",
		Basic:    &provider.BasicAuth{Username: "bot", Password: "hunter2"},
	}
	if s := basic.String(); strings.Contains(s, "hunter2") {
		t.Errorf("String() = %q, must not contain the password", s)
	}
	role := &provider.RegistryAuth{
		Registry: "x.dkr.ecr.us-west-2.amazonaws.com",
		AWSRole:  &provider.AWSRoleAuth{RoleARN: "arn:aws:iam::1:role/pull"},
	}
	if s := role.String(); !strings.Contains(s, "arn:aws:iam::1:role/pull") {
		t.Errorf("String() = %q, want the role ARN named", s)
	}
	if s := (*provider.RegistryAuth)(nil).String(); s != "none" {
		t.Errorf("nil String() = %q, want %q", s, "none")
	}
}

// authDiff renders the difference between two credentials, without printing a password.
func authDiff(got, want *provider.RegistryAuth) string {
	switch {
	case got == nil && want == nil:
		return ""
	case got == nil || want == nil:
		return fmt.Sprintf("got %v, want %v", got, want)
	case got.Registry != want.Registry:
		return fmt.Sprintf("registry = %q, want %q", got.Registry, want.Registry)
	}
	switch {
	case want.AWSRole != nil:
		if got.AWSRole == nil || *got.AWSRole != *want.AWSRole {
			return fmt.Sprintf("role = %v, want %v", got.AWSRole, want.AWSRole)
		}
	case want.Basic != nil:
		if got.Basic == nil || *got.Basic != *want.Basic {
			// Both are test fixtures, so printing them is safe here.
			return fmt.Sprintf("basic = %v, want %v", got.Basic, want.Basic)
		}
	}
	return ""
}
