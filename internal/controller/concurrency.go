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

package controller

// concurrentReconciles is how many objects a FLEET-SCALED controller reconciles at once —
// one whose object count tracks the number of workloads (NodeClaims, Pods, Sandboxes)
// rather than the number of policies.
//
// controller-runtime defaults to 1, which is right for a controller with a handful of
// objects and wrong for these: a single worker turns the whole fleet into a queue served
// one item at a time, and each item's cost is dominated by WAITING — an API write round
// trip, or a provider call — not by CPU. So the worker sits idle while the fleet backs up,
// and the observed rate is 1/latency regardless of how much CPU the manager is given.
//
// 8 matches pkg/vnode's podSyncWorkers, deliberately: the two pipelines hand work to each
// other (VK writes the Pod status the claim controller waits on, and the claim controller's
// teardown follows VK's DeletePod), so sizing them alike keeps either from being the
// other's ceiling. Raising it further trades API-server pressure for latency, and the
// server, not this constant, is the next limit.
//
// Safe because controller-runtime never reconciles the same key concurrently, so per-object
// state needs no locking, and the only state shared ACROSS objects is read-only (the
// provider registry) or already guarded (pkg/failover.Blocklist).
//
// Not applied to the NodePool or SandboxSet controllers: their object counts track policy,
// not fleet size, and their fan-in watches collapse thousands of child events onto a few
// keys (the workqueue dedups by key), so a second worker would mostly add conflict retries
// on the same status.
const concurrentReconciles = 8
