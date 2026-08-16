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
	"strings"

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
// adapter whose price/availability comes from a catalog: Name, Offerings, and a
// default identity MapAccelerator. Adapters embed it so those three methods are
// not re-implemented per provider:
//
//	type Provider struct {
//	    catalog.Base
//	    client Client
//	}
//
// Name and Offerings are fully generic. MapAccelerator is generic only while a provider
// names its accelerators like Nebula's canonical names (Modal does); one whose identifiers
// diverge overrides just that method and still reuses the rest.
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
