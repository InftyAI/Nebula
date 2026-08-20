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
	"fmt"
	"net"
)

// SplitEgressTargets sorts an EgressPolicy.Targets list into prefixes and domain names.
// Which kind an entry is can be decided by parsing, so a pool declares one list (users
// think "let it reach S3 and huggingface", not "which field is this") and adapters that
// take the two separately split here rather than each rolling its own.
func SplitEgressTargets(entries []string) (cidrs, domains []string) {
	for _, e := range entries {
		if _, _, err := net.ParseCIDR(e); err == nil {
			cidrs = append(cidrs, e)
			continue
		}
		if ip := net.ParseIP(e); ip != nil {
			bits := 32
			if ip.To4() == nil {
				bits = 128
			}
			cidrs = append(cidrs, fmt.Sprintf("%s/%d", ip, bits))
			continue
		}
		domains = append(domains, e)
	}
	return cidrs, domains
}
