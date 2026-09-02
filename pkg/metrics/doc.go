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
// controller/workqueue metrics. Importing this package is enough wiring: every metric
// self-registers (see the init in each file) and every entry point is a helper the callers
// already on those paths invoke.
//
// The cost counter is the one exception: its label names depend on --cost-labels, so main must
// call InitCost once after flags are parsed. See InitCost for why it cannot self-register.
//
// Nothing here reads or writes the API. Cost also accrues onto a NodeClaim status field, so
// the loop that advances it stays in internal/controller and calls in here with each closed
// window.
//
// The instrumented surface is the path a Pod takes from admission to a running
// external instance, one file per leg, plus what that instance costs while it runs:
//
//	placement.go   the Pod is gated -> a candidate is chosen -> the gate is removed
//	provision.go   the provider is called -> the instance reports Running
//	cost.go        dollars accrued, one billing window at a time
//	attribution.go whose dollars those are, from the operator's chosen Pod labels
//
// Those are the legs whose cost and failure modes are otherwise invisible: placement
// can silently leave a Pod gated forever, provisioning runs against a third party,
// takes minutes, bills money, and fails for reasons Pod status flattens away, and the
// instance it produces keeps charging whether or not anything is using it.
// Everything else is covered elsewhere and not duplicated — reconcile counts, queue
// depth and API latency by controller-runtime's collectors, "how many Pods are gated
// right now?" by kube-state-metrics.
//
// The two legs deliberately share one label set (labels.go) so a placement and the
// provisioning attempt it led to carry identical label values and can be joined in
// PromQL without label surgery. See docs/metrics.md for the operator-facing view.
package metrics
