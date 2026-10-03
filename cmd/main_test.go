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

package main

import (
	"maps"
	"slices"
	"strings"
	"testing"
)

func TestParseProviders(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    []string
		wantErr bool
	}{
		{name: "default enables all", in: strings.Join(knownProviders, ","), want: knownProviders},
		{name: "subset", in: "aws", want: []string{"aws"}},
		{name: "spaces and empties ignored", in: " modal , ,aws ", want: []string{"aws", "modal"}},
		{name: "empty disables all", in: "", want: nil},
		// A typo must not silently leave a provider off.
		{name: "unknown name", in: "modal,moddal", wantErr: true},
		{name: "fake is not selectable", in: "fake", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseProviders(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseProviders(%q) = %v, want an error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseProviders(%q): %v", tc.in, err)
			}
			names := slices.Sorted(maps.Keys(got))
			want := slices.Sorted(slices.Values(tc.want))
			if !slices.Equal(names, want) {
				t.Fatalf("parseProviders(%q) = %v, want %v", tc.in, names, want)
			}
		})
	}
}
