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

// Package catalog is the shared price/availability catalog for providers whose
// APIs do not expose a rate card (Modal, and most NeoClouds). It follows
// SkyPilot's community-catalog pattern: prices live in checked-in, git-tracked
// CSV files (pkg/provider/catalog/data/<provider>.csv) rather than hardcoded in
// Go, so a price change is a reviewable data diff.
//
// Two-tier load, override-first:
//
//  1. If an override directory is set (NEBULA_CATALOG_DIR, or LoadFrom(dir)),
//     CSVs there win. This is how the deployed controller consumes a ConfigMap:
//     the CSVs are rendered into a ConfigMap and mounted, so ops can edit prices
//     live (kubectl edit configmap) without rebuilding the image.
//  2. Otherwise the CSVs embedded at build time are used as the default, so the
//     binary always has a working catalog even with nothing mounted.
//
// A provider's Offerings() becomes a lookup into this catalog instead of a
// hardcoded table.
package catalog

import (
	"embed"
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// OverrideDirEnv is the env var pointing at a directory of provider CSVs that
// override the embedded defaults. Set by the manager Deployment to the ConfigMap
// mount path.
const OverrideDirEnv = "NEBULA_CATALOG_DIR"

//go:embed data/*.csv
var embedded embed.FS

// Catalog holds parsed offerings keyed by provider name.
type Catalog struct {
	byProvider map[string][]provider.Offering
}

// Load builds a Catalog, preferring CSVs in the override dir named by
// NEBULA_CATALOG_DIR when that env var is set and the dir exists, otherwise
// falling back to the embedded defaults.
func Load() (*Catalog, error) {
	if dir := os.Getenv(OverrideDirEnv); dir != "" {
		if info, err := os.Stat(dir); err == nil && info.IsDir() {
			return LoadFrom(dir)
		}
	}
	return loadFS(embedded, "data")
}

// LoadFrom builds a Catalog from CSV files in dir (used for the ConfigMap mount
// and in tests). Each file must be named "<provider>.csv".
func LoadFrom(dir string) (*Catalog, error) {
	return loadFS(os.DirFS(dir), ".")
}

// loadFS parses every "<provider>.csv" under root of fsys into a Catalog.
func loadFS(fsys fs.FS, root string) (*Catalog, error) {
	entries, err := fs.ReadDir(fsys, root)
	if err != nil {
		return nil, fmt.Errorf("catalog: read dir: %w", err)
	}
	c := &Catalog{byProvider: make(map[string][]provider.Offering)}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".csv") {
			continue
		}
		providerName := strings.TrimSuffix(name, ".csv")
		f, err := fsys.Open(filepath.Join(root, name))
		if err != nil {
			return nil, fmt.Errorf("catalog: open %s: %w", name, err)
		}
		offerings, err := parseCSV(f)
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("catalog: parse %s: %w", name, err)
		}
		c.byProvider[providerName] = offerings
	}
	return c, nil
}

// Offerings returns the catalog rows for providerName, or nil if the provider
// has no catalog file. The returned slice is a copy, safe for the caller to
// annotate (e.g. a live availability probe) without mutating the catalog.
func (c *Catalog) Offerings(providerName string) []provider.Offering {
	src := c.byProvider[providerName]
	if len(src) == 0 {
		return nil
	}
	out := make([]provider.Offering, len(src))
	copy(out, src)
	return out
}

// Required and optional CSV column names. Parsing is header-driven (by name, not
// position) so a provider can add the optional columns — or reorder — without a
// parser change, and NeoCloud CSVs stay in the original minimal form.
const (
	colAccelerator   = "accelerator_type" // required: canonical Nebula accelerator type
	colAcceleratorID = "accelerator_id"   // optional: provider's own accelerator id (e.g. AWS instance type)
	colCapacityType  = "capacity_type"    // required: Spot | OnDemand
	colPrice         = "price_per_hour"   // required: approximate USD/GPU-hour
	colAvailable     = "available"        // required: whether the provider offers it
	colGPUCount      = "gpu_count"        // optional: accelerators the id provides (AWS: baked into instance type)
	colRegion        = "region"           // optional: provider region this row prices
	colUpdated       = "updated"          // optional: documentation only, ignored
)

// parseCSV reads offering rows. Lines beginning with '#' are comments; the first
// non-comment line is the header, and columns are resolved BY NAME from it, so
// order does not matter and optional columns may be absent. Required columns:
// accelerator_type, capacity_type, price_per_hour, available. Optional columns:
// accelerator_id (a provider's own accelerator id, e.g. AWS "p5.48xlarge", for a
// provider whose MapAccelerator override translates canonical names — for Modal,
// whose mapping is identity, it is simply left blank), gpu_count (how many
// accelerators the id provides, a lookup dimension only where the count is baked
// into the offering — AWS instance types — and blank/0 for providers that take
// the count as a runtime parameter, like Modal), region (the provider region a
// row prices, for region-aware providers), and updated (documentation only).
func parseCSV(r io.Reader) ([]provider.Offering, error) {
	cr := csv.NewReader(r)
	cr.Comment = '#'
	cr.FieldsPerRecord = -1 // rows may carry a different column count than legacy fixed-order
	cr.TrimLeadingSpace = true

	records, err := cr.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}

	// Map header names to column indices.
	idx := make(map[string]int, len(records[0]))
	for i, h := range records[0] {
		idx[strings.TrimSpace(h)] = i
	}
	for _, req := range []string{colAccelerator, colCapacityType, colPrice, colAvailable} {
		if _, ok := idx[req]; !ok {
			return nil, fmt.Errorf("missing required column %q in header", req)
		}
	}

	// field returns the named column's value for a row, or "" if the column is
	// absent (optional) or the row is short.
	field := func(rec []string, name string) string {
		i, ok := idx[name]
		if !ok || i >= len(rec) {
			return ""
		}
		return strings.TrimSpace(rec[i])
	}

	offerings := make([]provider.Offering, 0, len(records)-1)
	for i, rec := range records {
		if i == 0 {
			continue // header
		}
		price, err := strconv.ParseFloat(field(rec, colPrice), 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: price_per_hour %q: %w", i, field(rec, colPrice), err)
		}
		// ParseFloat takes "NaN" and "Inf" as valid floats. Refused at the boundary because a rate
		// is pinned onto the claim and integrated from there — the damage is downstream, in a
		// durable ledger and a counter that cannot be corrected, not in this row.
		if math.IsNaN(price) || math.IsInf(price, 0) || price < 0 {
			return nil, fmt.Errorf("row %d: price_per_hour %q must be a non-negative finite number", i, field(rec, colPrice))
		}
		available, err := strconv.ParseBool(field(rec, colAvailable))
		if err != nil {
			return nil, fmt.Errorf("row %d: available %q: %w", i, field(rec, colAvailable), err)
		}
		// gpu_count is optional; blank => 0 (not a lookup dimension for this
		// provider, e.g. Modal, which takes the count as a runtime parameter). A
		// present-but-unparseable value is an error, not a silent 0.
		var gpuCount int32
		if v := field(rec, colGPUCount); v != "" {
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("row %d: gpu_count %q: %w", i, v, err)
			}
			gpuCount = int32(n)
		}
		offerings = append(offerings, provider.Offering{
			AcceleratorType: field(rec, colAccelerator),
			CapacityType:    nebulav1alpha1.CapacityType(field(rec, colCapacityType)),
			PricePerHour:    price,
			Available:       available,
			Region:          field(rec, colRegion),
			AcceleratorID:   field(rec, colAcceleratorID),
			GPUCount:        gpuCount,
		})
	}
	return offerings, nil
}
