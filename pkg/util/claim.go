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

// Package util holds small, dependency-free helpers shared across Nebula's
// control plane and provider adapters.
package util

// ClaimName is the instance-identity token Nebula encodes into a provider
// instance's name/tag so List/Terminate can find it later without a durable id.
// It is a deterministic function of the served workload's namespace and name, so
// any component that knows the Pod (the virtual kubelet) or the claim's PodRef
// (the teardown backstop) computes the same token. Keep this the single source
// of truth for the convention — the vnode handler and the NodeClaim finalizer
// both depend on producing identical values.
func ClaimName(namespace, name string) string {
	return namespace + "-" + name
}
