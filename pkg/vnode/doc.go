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

// Package vnode implements the Virtual Kubelet integration: one static virtual
// Node per provider, whose PodLifecycleHandler provisions/terminates external
// instances through the provider seam. See docs/architecture.md §3.
//
// Ownership model ("VK owns provisioning"): the pod controller's CreatePod calls
// provider.Provision and DeletePod calls provider.Terminate directly. The Pod is
// the single source of truth for the workload's SHAPE, and nothing more: every
// provisioning input that constrains rather than describes it is read from
// cluster state the workload's owner cannot patch — the NodePool for policy, the
// NodeClaim for the capacity tier and region placement chose (see ClusterReader).
// Instance identity is derived deterministically from the Pod (ClaimName), so a
// provider whose List reports the claim tag can recover and reclaim an instance
// across a controller restart without a durable ledger.
package vnode
