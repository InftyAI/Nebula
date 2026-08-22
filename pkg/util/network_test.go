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

package util

import (
	"slices"
	"testing"
)

// TestSplitEgressTargets covers the one decision a pool's single target list defers to
// the adapter: which axis an entry belongs to.
func TestSplitEgressTargets(t *testing.T) {
	for _, tc := range []struct {
		name        string
		entries     []string
		wantCIDRs   []string
		wantDomains []string
	}{{
		name:      "prefixes stay verbatim",
		entries:   []string{"10.0.0.0/8", "2001:db8::/32"},
		wantCIDRs: []string{"10.0.0.0/8", "2001:db8::/32"},
	}, {
		// The CIDR axis takes prefixes, so a bare IP has to be widened rather than
		// dropped or sent as-is.
		name:      "bare IPs become single-address prefixes",
		entries:   []string{"1.2.3.4", "2001:db8::1"},
		wantCIDRs: []string{"1.2.3.4/32", "2001:db8::1/128"},
	}, {
		name:        "names and wildcards are domains",
		entries:     []string{"api.openai.com", "*.huggingface.co"},
		wantDomains: []string{"api.openai.com", "*.huggingface.co"},
	}, {
		// The mixed list is the reason the split exists at all: users declare one list
		// and never say which kind an entry is.
		name:        "a mixed list divides",
		entries:     []string{"10.0.0.0/8", "*.huggingface.co", "1.2.3.4"},
		wantCIDRs:   []string{"10.0.0.0/8", "1.2.3.4/32"},
		wantDomains: []string{"*.huggingface.co"},
	}, {
		// The provider owns the domain vocabulary, so an unparseable entry is forwarded
		// for it to reject with its own error rather than silently dropped here.
		name:        "an unrecognized entry goes to domains",
		entries:     []string{"not a host"},
		wantDomains: []string{"not a host"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			cidrs, domains := SplitEgressTargets(tc.entries)
			if !slices.Equal(cidrs, tc.wantCIDRs) {
				t.Errorf("cidrs = %v, want %v", cidrs, tc.wantCIDRs)
			}
			if !slices.Equal(domains, tc.wantDomains) {
				t.Errorf("domains = %v, want %v", domains, tc.wantDomains)
			}
		})
	}
}
