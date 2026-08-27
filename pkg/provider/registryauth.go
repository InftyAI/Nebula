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

import "fmt"

// RegistryAuth is the resolved credential for pulling the workload's image, in a
// provider-neutral form each adapter translates into its own mechanism (Modal calls
// Images.FromAwsEcr; an EC2 adapter would run `aws ecr get-login-password`).
//
// Exactly one source field is set, and which one IS the kind — the shape
// corev1.EnvVarSource uses. No kind enum, which could disagree with the fields, and no flat
// struct, whose inapplicable halves invite reading one that was never set.
//
// The contract every adapter owes it: honour it or FAIL the Provision, as with
// ProvisionRequest.Egress. A silent fall-through to an anonymous pull either 401s opaquely
// or succeeds against a PUBLIC image of the same name.
type RegistryAuth struct {
	// Registry is the host the credential applies to, from the image reference.
	Registry string
	// AWSRole authenticates by role assumption; see AWSRoleAuth.
	AWSRole *AWSRoleAuth
	// Basic authenticates with a username and password; see BasicAuth.
	Basic *BasicAuth
}

// AWSRoleAuth is an AWS IAM role the PROVIDER assumes to pull from a private ECR registry —
// a delegation, not a credential: neither field is secret, and the role only works where its
// trust policy trusts that provider's identity (for Modal, its OIDC provider).
type AWSRoleAuth struct {
	RoleARN string
	// Region is where GetAuthorizationToken is called: the REGISTRY's region, not where the
	// workload runs. Required, and stated rather than parsed out of the image reference: the
	// ECR host encodes a region, but a pull-through or replicated reference makes that a
	// guess, and the guess is only discovered wrong as an opaque auth failure.
	Region string
}

// BasicAuth is HTTP Basic against the registry, which all of them accept. An ECR "password"
// is a 12-hour token though, so a fixed one there rots — use AWSRoleAuth.
//
// SECRET-BEARING in Password, hence the redaction in RegistryAuth.String.
type BasicAuth struct {
	Username string
	Password string
}

// Validate reports whether a is WELL-FORMED: exactly one kind set, and that kind's fields
// all present. Not whether any given adapter can honour it — that is per-adapter (see
// Unsupported) — so every adapter calls this for the kinds it does support.
//
// Here rather than in each adapter because these are invariants of the TYPE: an ECR pull
// needs the role and the registry's region wherever it runs, and a half-filled credential
// only ever surfaces as an opaque 401 from the registry.
//
// Callers upstream (vnode.resolveRegistryAuth) reject the same things earlier with a better
// message; this is the adapter boundary's own guarantee, since ProvisionRequest can be built
// by anyone.
func (a *RegistryAuth) Validate() error {
	switch {
	case a == nil:
		return nil // no credential is not a malformed one; an anonymous pull is legal
	case a.AWSRole != nil && a.Basic != nil:
		return fmt.Errorf("registry auth for %q sets two kinds at once: %w",
			a.Registry, ErrImagePull)
	case a.AWSRole != nil:
		if a.AWSRole.RoleARN == "" || a.AWSRole.Region == "" {
			return fmt.Errorf("AWS role auth for %q needs both a role ARN and a region: %w",
				a.Registry, ErrImagePull)
		}
	case a.Basic != nil:
		if a.Basic.Username == "" || a.Basic.Password == "" {
			return fmt.Errorf("basic auth for %q needs both a username and a password: %w",
				a.Registry, ErrImagePull)
		}
	default:
		return fmt.Errorf("registry auth for %q sets no credential: %w", a.Registry, ErrImagePull)
	}
	return nil
}

// Unsupported is the error an adapter returns for a credential kind it cannot honour, named
// so the message says which provider refused. Shared so the ErrImagePull wrap cannot be
// forgotten: unwrapped, the same refusal reads as unattributable and the Pod retries against
// a provider that will never accept it (see ErrImagePull, IsRejection).
func (a *RegistryAuth) Unsupported(providerName string) error {
	return fmt.Errorf("%s: unsupported image pull credential %s: %w", providerName, a, ErrImagePull)
}

// String names the role ARN — an identifier, and what makes "cannot assume this role"
// actionable — and never a password.
func (a *RegistryAuth) String() string {
	switch {
	case a == nil:
		return "none"
	case a.AWSRole != nil:
		return fmt.Sprintf("AWSRole(%s,role=%s,region=%s)",
			a.Registry, a.AWSRole.RoleARN, a.AWSRole.Region)
	case a.Basic != nil:
		return fmt.Sprintf("Basic(%s,user=%s,password=%s)",
			a.Registry, a.Basic.Username, redacted(a.Basic.Password))
	default:
		return fmt.Sprintf("unknown(%s)", a.Registry)
	}
}
