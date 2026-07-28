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

// MapAccelerator translates a canonical accelerator type into this provider's
// own accelerator id using the catalog as the mapping table: it finds the row
// whose AcceleratorType matches (case-insensitively) and returns that row's
// AcceleratorID. When AcceleratorID is blank (a provider whose id equals the
// canonical name) it falls back to the canonical name, so an identity-mapped
// provider needs no per-name data. Because the mapping lives entirely in the
// CSV, a provider whose ids diverge (AWS instance types) does NOT need to
// override this — it just populates the accelerator_id column. Returns ok=false
// when the provider does not offer the accelerator.
func (b Base) MapAccelerator(canonical string) (providerAcceleratorID string, ok bool) {
	for _, o := range b.Catalog.Offerings(b.ProviderName) {
		if strings.EqualFold(o.AcceleratorType, canonical) {
			if o.AcceleratorID != "" {
				return o.AcceleratorID, true
			}
			return o.AcceleratorType, true
		}
	}
	return "", false
}
