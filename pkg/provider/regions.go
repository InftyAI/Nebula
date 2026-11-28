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
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"

	"sigs.k8s.io/yaml"
)

// Geographies is Nebula's region vocabulary: the broad geographies a NodePool or a Pod
// may name.
var Geographies = []string{"af", "ap", "ca", "eu", "me", "mx", "sa", "uk", "us"}

// IsGeography reports whether token is part of the vocabulary.
func IsGeography(token string) bool {
	return slices.Contains(Geographies, token)
}

// RegionTables is the --regions file: for each provider listed, its whole geography-to-region
// table, in walk order. A provider listed here uses its table in place of the built-in one,
// so it names every geography it should serve; one left out serves nothing.
type RegionTables map[string]map[string][]string

// regionTableProviders are the providers whose table the --regions file may replace. One not
// listed resolves its regions some other way, so naming it is refused rather than ignored.
var regionTableProviders = []string{ProviderAWS}

// LoadRegionTables reads and validates the --regions file at path. An empty path or a missing
// file is no tables (nil, nil): the ConfigMap is optional, and without it every provider keeps
// its built-in table.
func LoadRegionTables(path string) (RegionTables, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read region tables: %w", err)
	}
	var tables RegionTables
	if err := yaml.UnmarshalStrict(raw, &tables); err != nil {
		return nil, fmt.Errorf("parse region tables %s: %w", path, err)
	}
	for name, table := range tables {
		if !slices.Contains(regionTableProviders, name) {
			return nil, fmt.Errorf("region tables: provider %q has no region table to replace (want one of %v)",
				name, regionTableProviders)
		}
		if err := validateRegionTable(table); err != nil {
			return nil, fmt.Errorf("region tables: %s: %w", name, err)
		}
	}
	return tables, nil
}

// validateRegionTable keeps a table a partition of the vocabulary: a region under two
// geographies would be walked twice by a declaration naming both, and a key outside
// Geographies could never be resolved to.
func validateRegionTable(table map[string][]string) error {
	home := map[string]string{}
	for geography, regions := range table {
		if !IsGeography(geography) {
			return fmt.Errorf("%q is not a geography (want one of %v)", geography, Geographies)
		}
		for _, r := range regions {
			if strings.TrimSpace(r) != r || r == "" {
				return fmt.Errorf("%s: region %q is empty or padded", geography, r)
			}
			if other, dup := home[r]; dup {
				return fmt.Errorf("region %q is under both %q and %q", r, other, geography)
			}
			home[r] = geography
		}
	}
	return nil
}
