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

package catalog

import (
	"context"
	"fmt"
	"strings"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// Lookup is the price/availability seam a provider adapter depends on: given a
// provider name it returns that provider's offering rows. The concrete *Catalog
// satisfies it; tests inject a trivial fake so an adapter can be unit-tested
// without embedding CSVs. It lives in this package (not in provider) so all
// catalog-shaped types share one home and there is no provider.Catalog /
// catalog.Catalog name clash.
type Lookup interface {
	// Offerings returns providerName's rows, or nil if the provider has no
	// catalog entry. Implementations return a copy the caller may safely annotate.
	Offerings(providerName string) []provider.Offering
}

// Base supplies the parts of provider.Provider that are identical for every
// adapter whose price/availability comes from a catalog: Name, Offerings, a
// default identity MapAccelerator, and the optional provider.Pricer. Adapters
// embed it so none of those are re-implemented per provider:
//
//	type Provider struct {
//	    catalog.Base
//	    client Client
//	}
//
// Name and Offerings are fully generic. MapAccelerator is generic only while a provider
// names its accelerators like Nebula's canonical names (Modal does); one whose identifiers
// diverge overrides just that method and still reuses the rest. PricePerHour is generic
// only while a provider's catalog price is all-in; one that meters CPU/memory separately
// overrides it and adds those components.
//
// Lifecycle, Capabilities and ClassifyProvisionError are genuinely provider-specific and
// are not provided here.
type Base struct {
	// ProviderName is this provider's stable identifier (e.g. "modal"), used both
	// as Name() and as the key into the catalog.
	ProviderName string
	// Catalog is the shared price/availability lookup.
	Catalog Lookup
}

// Name returns the provider's stable identifier.
func (b Base) Name() string { return b.ProviderName }

// Offerings returns this provider's rows from the catalog. The error is always
// nil today (the catalog is in-memory); the signature matches provider.Provider
// so an adapter that later combines the static catalog with a live availability
// probe can return a real error without a signature change.
func (b Base) Offerings(context.Context) ([]provider.Offering, error) {
	return b.Catalog.Offerings(b.ProviderName), nil
}

// ExpandRegions passes the declared regions through unchanged: one candidate each, tokens
// used verbatim as region names. Right for a provider whose own vocabulary already spans
// both levels the pool speaks AND whose provision reports capacity failures synchronously,
// so walking candidates actually buys a retry in the next region. nil stays nil, which
// every adapter reads as "unconstrained".
//
// Both halves have real overriders, in opposite directions: AWS expands a group token into
// many candidates ("us" is not a callable region), while Modal collapses everything into
// ONE candidate because its create cannot fail over. Check which a new provider resembles
// before inheriting this.
func (b Base) ExpandRegions(declared []string) []string { return declared }

// MapAccelerator translates a canonical accelerator request (type + count) into this
// provider's own ids, using the catalog as the mapping table: matching rows contribute
// their AcceleratorIDs in catalog order — PRIMARY first, then interchangeable alternates,
// deduped. A blank AcceleratorID falls back to the canonical name, so an identity-mapped
// provider needs no per-name data. Since the mapping is all in the CSV, a provider whose
// ids diverge just fills in accelerator_id/gpu_count instead of overriding this.
//
// Count matching honours both catalog shapes (see Offering.GPUCount): a provider that bakes
// the count in (AWS: T4x1=g4dn.xlarge, T4x8=g4dn.metal) emits one row per count, which is
// what keeps (L4, 1) and (L4, 8) on DISTINCT primaries so one's block cannot exclude the
// other. A provider taking count as a parameter (Modal) leaves GPUCount 0, and a 0 row
// matches any count.
//
// Dedup collapses the per-capacity-type/per-region duplicates (an instance type is the same
// object whether the row prices Spot or OnDemand). ids[0] is what failover blocks on;
// alternates broaden a launch but never the blocklist. ok=false when no row matches.
//
// Availability gates the mapping: a row with Available false contributes no id, so a (type,
// count) with no available row maps to ok=false. This is the one seam placement, AWS and
// Modal all consult, so flipping a CSV row off removes it from scheduling everywhere
// without touching Go.
func (b Base) MapAccelerator(canonical string, count int32) (providerAcceleratorIDs []string, ok bool) {
	offerings := b.Catalog.Offerings(b.ProviderName)
	ids := make([]string, 0, len(offerings))
	seen := make(map[string]bool)
	for _, o := range offerings {
		if !strings.EqualFold(o.AcceleratorType, canonical) {
			continue
		}
		// An unavailable row cannot serve the request: skip it so its id is not
		// offered up for a launch that the provider would only reject.
		if !o.Available {
			continue
		}
		// GPUCount 0 => count is not a lookup dimension for this provider (matches any
		// requested count); otherwise the row's count must equal the request.
		if o.GPUCount != 0 && o.GPUCount != count {
			continue
		}
		id := o.AcceleratorID
		if id == "" {
			id = o.AcceleratorType
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids, len(ids) > 0
}

// PricePerHour implements the optional provider.Pricer from the matched catalog row. That
// row is the WHOLE rate only where a provider's price is all-in (AWS sells an instance); one
// metering CPU and memory apart (Modal, whose rows quote a GPU alone) overrides this and
// adds them.
//
// Rows match as MapAccelerator matches them, plus capacity type: the one dimension
// MapAccelerator can ignore and pricing cannot (AWS p5.48xlarge is $34.412 Spot against
// $98.320 OnDemand). Among interchangeable alternates the FIRST row wins, so the price
// describes the id a launch actually tries first.
func (b Base) PricePerHour(req provider.PriceRequest) (float64, error) {
	// A CPU-only request. No row can match an empty type, so this is the same ErrNoPrice the
	// loop below would reach; it is stated here for a clearer message, and because it MUST
	// land before the Count check, which would otherwise flag count 0 as malformed. A
	// provider that really can run CPU-only work (Modal) overrides this and prices it.
	if req.AcceleratorType == "" {
		return 0, fmt.Errorf("catalog: %s prices accelerators only: %w", b.ProviderName, provider.ErrNoPrice)
	}
	// Contradictory input, not a missing price — hence a plain error, which callers do not
	// swallow the way they swallow ErrNoPrice.
	if req.Count <= 0 {
		return 0, fmt.Errorf("catalog: %s: accelerator %q with count %d", b.ProviderName, req.AcceleratorType, req.Count)
	}
	// An empty tier is the candidate a pool declaring no capacityTypes emits.
	// servesCapacityTier already reads that as non-Spot, so resolve it here rather than let
	// row order decide — on AWS that is a threefold difference.
	tier := req.CapacityType
	if tier == "" {
		tier = nebulav1alpha1.CapacityOnDemand
	}

	for _, o := range b.Catalog.Offerings(b.ProviderName) {
		if !strings.EqualFold(o.AcceleratorType, req.AcceleratorType) || !o.Available {
			continue
		}
		if o.CapacityType != tier {
			continue
		}
		if o.GPUCount != 0 && o.GPUCount != req.Count {
			continue
		}
		// An unpriced placeholder row (a hand-edited ConfigMap), not a free accelerator.
		if o.PricePerHour <= 0 {
			continue
		}
		rate := o.PricePerHour
		// GPUCount 0 => the row prices ONE accelerator and the count is a runtime knob, so
		// the rate scales with it. A row carrying a count prices the whole offering already.
		if o.GPUCount == 0 {
			rate *= float64(req.Count)
		}
		return rate, nil
	}
	return 0, fmt.Errorf("catalog: %s has no available %s x%d row on %s: %w",
		b.ProviderName, req.AcceleratorType, req.Count, tier, provider.ErrNoPrice)
}
