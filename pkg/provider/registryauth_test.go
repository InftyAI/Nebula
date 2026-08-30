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

package provider

import (
	"errors"
	"strings"
	"testing"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

func TestRegistryAuthValidate(t *testing.T) {
	role := func(arn, region string) *RegistryAuth {
		return &RegistryAuth{Registry: "r", AWSRole: &AWSRoleAuth{RoleARN: arn, Region: region}}
	}
	basic := func(user, pass string) *RegistryAuth {
		return &RegistryAuth{Registry: "r", Basic: &BasicAuth{Username: user, Password: pass}}
	}

	cases := []struct {
		name string
		auth *RegistryAuth
		ok   bool
	}{
		// An anonymous pull is legal, so nil is well-formed rather than malformed.
		{"nil", nil, true},
		{"role and region", role("arn:aws:iam::1:role/pull", "us-west-2"), true},
		{"user and password", basic("bot", "hunter2"), true},

		// A half-filled credential is the case worth catching: it reaches the registry and
		// comes back as an opaque 401 that names nothing.
		{"role without a region", role("arn:aws:iam::1:role/pull", ""), false},
		{"region without a role", role("", "us-west-2"), false},
		{"user without a password", basic("bot", ""), false},
		{"password without a user", basic("", "hunter2"), false},
		{"no kind at all", &RegistryAuth{Registry: "r"}, false},
		{
			// Ambiguous, and which one an adapter would pick is arbitrary.
			name: "two kinds at once",
			auth: &RegistryAuth{
				Registry: "r",
				AWSRole:  &AWSRoleAuth{RoleARN: "arn:aws:iam::1:role/pull", Region: "us-west-2"},
				Basic:    &BasicAuth{Username: "bot", Password: "hunter2"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.auth.Validate()
			if tc.ok {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Validate() = nil, want an error")
			}
			// The sentinel is the contract: ErrImage scopes the block to this Pod's
			// request, where ErrAuth would fence off the entire provider.
			if !errors.Is(err, ErrImage) {
				t.Errorf("Validate() = %v, want it to wrap ErrImage", err)
			}
			if errors.Is(err, ErrAuth) {
				t.Errorf("Validate() = %v, must NOT wrap ErrAuth (it widens to DenyAll)", err)
			}
			if strings.Contains(err.Error(), "hunter2") {
				t.Errorf("Validate() = %q, must not leak the password", err)
			}
		})
	}
}

func TestRegistryAuthUnsupported(t *testing.T) {
	err := (&RegistryAuth{
		Registry: "ghcr.io",
		Basic:    &BasicAuth{Username: "bot", Password: "hunter2"},
	}).Unsupported("aws")

	if !errors.Is(err, ErrImage) {
		t.Errorf("err = %v, want it to wrap ErrImage", err)
	}
	// The refusal is the POD's, so it must blocklist nothing: the candidate accepts other
	// Pods' credentials perfectly well, and "unsupported credential" text left to the
	// heuristics would read as auth and fence off the whole provider.
	if scope := ClassifyError(err, nebulav1alpha1.CapacityOnDemand, "H100:1"); scope != (BlockScope{}) {
		t.Errorf("BlockScope = %+v, want zero", scope)
	}
	if !strings.Contains(err.Error(), "aws") {
		t.Errorf("err = %q, want the refusing provider named", err)
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("err = %q, must not leak the password", err)
	}
}
