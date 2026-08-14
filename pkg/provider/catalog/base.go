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
// The split is deliberate:
//   - Name and Offerings are FULLY generic — every catalog-backed adapter would
//     otherwise write the identical one-liners.
//   - MapAccelerator is generic ONLY while a provider names its accelerators
//     exactly like Nebula's canonical names (Modal does). A provider whose
//     identifiers diverge (e.g. RunPod's "NVIDIA H100 80GB HBM3") overrides just
//     this one method — its own method shadows the promoted one — while still
//     reusing Name/Offerings.
//
// Lifecycle (Provision/Terminate/Get/List), Capabilities and
// ClassifyProvisionError are genuinely provider-specific and are NOT provided
// here; each adapter implements them itself.
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

// ExpandRegions passes the pool's declared regions through unchanged: one candidate
// per declared region, and the declaration's tokens used verbatim as region names.
// This is the right default for a provider whose OWN vocabulary already spans both
// levels the pool speaks (so there is nothing to expand — the token IS the region
// name, which stays future-proof as the provider adds regions) AND whose provision
// call reports a capacity failure synchronously, so walking candidates one at a time
// actually buys a retry in the next region.
//
// nil stays nil, which every adapter must read as "unconstrained": send no region
// and let the provider place freely.
//
// Both halves have real overriders, in opposite directions. AWS expands a group
// token into many candidates, because "us" is not an EC2 region name you can call.
// Modal collapses the whole declaration into ONE candidate holding every region,
// because its create cannot fail over — splitting would strand the workload in
// whichever region was walked first. Check which of those a new provider resembles
// before inheriting this.
func (b Base) ExpandRegions(declared []string) []string { return declared }

// MapAccelerator translates a canonical accelerator request (type + count) into
// this provider's own accelerator ids using the catalog as the mapping table. It
// finds the offering rows whose AcceleratorType matches (case-insensitively) and
// whose GPUCount matches the request, and returns their AcceleratorIDs in catalog
// order — the PRIMARY (first matching row) first, then any interchangeable
// alternates, de-duplicated. When a row's AcceleratorID is blank (a provider whose
// id equals the canonical name) it falls back to the canonical name, so an
// identity-mapped provider needs no per-name data. Because the mapping lives
// entirely in the CSV, a provider whose ids diverge (AWS instance types) does NOT
// need to override this — it just populates the accelerator_id and gpu_count
// columns, and adds a row per alternate to widen the launch.
//
// Count matching honours the two catalog shapes (see Offering.GPUCount): a
// provider that bakes the count into the offering (AWS: T4x1=g4dn.xlarge,
// T4x8=g4dn.metal) emits one row per count, so the row whose GPUCount equals the
// request is the match — this is what keeps (L4, 1) and (L4, 8) on DISTINCT primary
// ids so one's capacity block does not exclude the other. A provider that takes the
// count as a free parameter (Modal) leaves GPUCount 0 on its single row; a 0-count
// row matches any requested count, since count is not a lookup dimension there.
//
// Dedup collapses the catalog's per-capacity-type/per-region row duplicates (an
// instance type is the same object whether the row prices Spot or OnDemand). The
// primary — ids[0] — is the identity failover blocks on; alternates broaden a
// single launch but never the blocklist. Returns ok=false when the provider offers
// no row for that (type, count).
//
// Availability gates the mapping: a row whose Available is false (the catalog's
// availability column, e.g. a type we no longer wish to schedule onto) contributes
// no id, so a (type, count) whose every row is unavailable maps to ok=false. This
// is the single seam placement (servability check), AWS Provision and Modal
// Provision all consult, so flipping a CSV row's availability off removes it from
// scheduling everywhere without touching Go — no launch is attempted for it.
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
