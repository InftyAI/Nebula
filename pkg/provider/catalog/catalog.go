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

// parseCSV reads offering rows. Lines beginning with '#' are comments; the first
// non-comment line is the header. Column order is fixed:
// accelerator,capacity_type,price_per_hour,available,updated. The trailing
// updated column is documentation only and ignored here.
//
// There is deliberately no provider_id column yet: every provider we currently
// support (Modal) names its accelerators identically to Nebula's canonical
// names, so MapAccelerator is pure identity. When a provider whose identifiers
// diverge lands (e.g. RunPod's "NVIDIA H100 80GB HBM3"), add a provider_id
// column here and have MapAccelerator return it — no per-provider Go table.
func parseCSV(r io.Reader) ([]provider.Offering, error) {
	cr := csv.NewReader(r)
	cr.Comment = '#'
	cr.FieldsPerRecord = -1 // tolerate a trailing "updated" column we ignore
	cr.TrimLeadingSpace = true

	records, err := cr.ReadAll()
	if err != nil {
		return nil, err
	}

	offerings := make([]provider.Offering, 0, len(records))
	for i, rec := range records {
		if i == 0 {
			continue // header
		}
		if len(rec) < 4 {
			return nil, fmt.Errorf("row %d: expected >=4 columns, got %d", i, len(rec))
		}
		price, err := strconv.ParseFloat(strings.TrimSpace(rec[2]), 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: price_per_hour %q: %w", i, rec[2], err)
		}
		available, err := strconv.ParseBool(strings.TrimSpace(rec[3]))
		if err != nil {
			return nil, fmt.Errorf("row %d: available %q: %w", i, rec[3], err)
		}
		offerings = append(offerings, provider.Offering{
			AcceleratorType: strings.TrimSpace(rec[0]),
			CapacityType:    nebulav1alpha1.CapacityType(strings.TrimSpace(rec[1])),
			PricePerHour:    price,
			Available:       available,
		})
	}
	return offerings, nil
}
