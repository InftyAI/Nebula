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
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestIsGeography(t *testing.T) {
	for _, tc := range []struct {
		token string
		want  bool
	}{
		{"us", true},
		{"uk", true}, // not folded into "eu"; see Geographies
		{"af", true},
		// Provider region names are NOT vocabulary, at either provider. This is what makes
		// the Pod-facing path broad-only: an adapter never sees one of these in narrowTo.
		{"us-east-1", false}, // AWS
		{"us-east", false},   // Modal
		{"jp", false},        // Modal
		{"usa", false},
		{"US", false}, // callers lowercase first
		{"", false},
	} {
		if got := IsGeography(tc.token); got != tc.want {
			t.Errorf("IsGeography(%q) = %v, want %v", tc.token, got, tc.want)
		}
	}
}

// TestGeographies_AreFlatAndSorted pins the two properties every provider table leans on: a
// geography is a bare token (anything with a "-" is a provider's own name and belongs in a
// provider table, not here), and the list is sorted so a reader can find one.
func TestGeographies_AreFlatAndSorted(t *testing.T) {
	if !slices.IsSorted(Geographies) {
		t.Errorf("Geographies is not sorted: %v", Geographies)
	}
	for _, g := range Geographies {
		if g == "" {
			t.Error("Geographies holds an empty token")
		}
		for _, c := range g {
			if c == '-' || (c >= 'A' && c <= 'Z') {
				t.Errorf("geography %q is not a bare lowercase token; provider region names "+
					"live in the provider's own table", g)
				break
			}
		}
	}
}

func TestLoadRegionTables(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "regions.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	t.Run("a valid file is the provider's whole table", func(t *testing.T) {
		got, err := LoadRegionTables(write(t, "aws:\n  af: [af-south-1]\n  us: [us-east-1, us-west-2]\n"))
		if err != nil {
			t.Fatal(err)
		}
		want := RegionTables{ProviderAWS: {"af": {"af-south-1"}, "us": {"us-east-1", "us-west-2"}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	// An empty table is a choice, unlike a missing one: AWS serves no geography.
	t.Run("an empty table is kept", func(t *testing.T) {
		got, err := LoadRegionTables(write(t, "aws: {}\n"))
		if err != nil || got[ProviderAWS] == nil || len(got[ProviderAWS]) != 0 {
			t.Fatalf("got %v, %v; want an empty, non-nil aws table", got, err)
		}
	})

	// The ConfigMap is optional, so neither is an error: every provider keeps its built-in table.
	for name, path := range map[string]string{"no flag": "", "no file": filepath.Join(t.TempDir(), "absent.yaml")} {
		t.Run(name, func(t *testing.T) {
			if got, err := LoadRegionTables(path); err != nil || got != nil {
				t.Fatalf("got %v, %v; want nil, nil", got, err)
			}
		})
	}

	for name, tc := range map[string]struct{ body, want string }{
		"provider with no table": {"modal:\n  us: [us-east]\n", `provider "modal" has no region table`},
		"key with no value":      {"aws:\n", "aws has no table"},
		"key set to null":        {"aws: null\n", "aws has no table"},
		"unknown geography":      {"aws:\n  jp: [ap-northeast-1]\n", `"jp" is not a geography`},
		"region under two":       {"aws:\n  eu: [eu-west-2]\n  uk: [eu-west-2]\n", `"eu-west-2" is under both`},
		"empty region":           {"aws:\n  us: [\"\"]\n", "empty or padded"},
		"unknown field":          {"aws:\n  us: [us-east-1]\nregions: []\n", "parse region tables"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := LoadRegionTables(write(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one containing %q", err, tc.want)
			}
		})
	}
}
