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
	"slices"
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
