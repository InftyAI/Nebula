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

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// ErrNoPrice: this Pricer has no price for this request — no matching catalog row, or one
// that carries no usable price. Kept a distinct sentinel so a caller can treat it exactly
// as it treats a provider implementing no Pricer at all (report no cost and carry on),
// while a genuinely malformed request still surfaces as a loud error.
//
// It is NOT a provision failure and must never reach ClassifyError: a candidate nobody can
// price is still perfectly launchable.
var ErrNoPrice = errors.New("provider: no price for request")

// PriceRequest is what to price. It describes a CANDIDATE, not an instance, and carries
// no Pod: the optimizer compares candidates before anything is provisioned, and a
// Pod-shaped signature would put pricing out of reach there.
//
// No region axis, because no catalog row is region-partitioned today (every CSV leaves
// the column empty), so a price is uniform across a provider's regions. Add the axis back
// alongside the first region-varying row.
// TODO: maybe need to support region in the future.
type PriceRequest struct {
	// AcceleratorType is the canonical type ("H100"), empty for a CPU-only workload.
	AcceleratorType string
	// Count is how many accelerators. It does two jobs, and both providers need it: it
	// SELECTS the row where a provider bakes the count into the offering (AWS's
	// g4dn.xlarge is T4x1, g4dn.metal is T4x8), and it MULTIPLIES the rate where the
	// provider prices per accelerator instead (Modal). Which one applies is read off the
	// matched row's GPUCount rather than hardcoded per provider — see Offering.GPUCount.
	Count int32
	// CapacityType selects between a row's Spot and OnDemand prices, which differ
	// sharply (AWS p5.48xlarge: $34.412 Spot vs $98.320 OnDemand).
	CapacityType nebulav1alpha1.CapacityType
	// CPUCores and MemoryMiB are the workload's RESERVATION, priced only by providers
	// that meter them separately from the accelerator. Ignored by a provider whose
	// instance price is all-in.
	CPUCores  float64
	MemoryMiB int
}

// Pricer reports the hourly USD cost of what a Provision would create: ONE rate, with every
// resource the provider bills for folded in.
//
// No per-resource breakdown, because providers disagree on what is even billed separately —
// AWS sells an instance whose price already covers its vCPU and RAM, while Modal meters GPU,
// CPU and memory apart. Splitting the all-in case means inventing an allocation no invoice
// line matches. Add a breakdown when a provider's own billing hands us one.
//
// It is OPTIONAL, resolved by type assertion like LogStreamer and Executor: a provider that
// cannot price its instances implements nothing and its cost goes unreported, which is
// better than a confidently wrong number. Adapters embedding catalog.Base get an
// implementation for free.
type Pricer interface {
	PricePerHour(PriceRequest) (float64, error)
}
