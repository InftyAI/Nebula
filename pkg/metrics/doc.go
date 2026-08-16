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

// Package metrics holds Nebula's Prometheus instrumentation.
//
// Everything here registers into controller-runtime's registry, so it is served on
// the manager's existing --metrics-bind-address endpoint alongside the standard
// controller/workqueue metrics. Importing this package is what registers the
// collectors (see the init in each file); no wiring is needed in main.
//
// The instrumented surface is the path a Pod takes from admission to a running
// external instance, one file per leg:
//
//	placement.go   the Pod is gated -> a candidate is chosen -> the gate is removed
//	provision.go   the provider is called -> the instance reports Running
//
// Those are the legs whose cost and failure modes are otherwise invisible: placement
// can silently leave a Pod gated forever, and provisioning runs against a third party,
// takes minutes, bills money, and fails for reasons Pod status flattens away.
// Everything else is covered elsewhere and not duplicated — reconcile counts, queue
// depth and API latency by controller-runtime's collectors, "how many Pods are gated
// right now?" by kube-state-metrics.
//
// The two legs deliberately share one label set (labels.go) so a placement and the
// provisioning attempt it led to carry identical label values and can be joined in
// PromQL without label surgery. See docs/metrics.md for the operator-facing view.
package metrics
