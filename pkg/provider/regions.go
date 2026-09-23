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

import "slices"

// Geographies is Nebula's region vocabulary: the broad geographies a NodePool or a Pod
// may name.
var Geographies = []string{"af", "ap", "ca", "eu", "me", "mx", "sa", "uk", "us"}

// IsGeography reports whether token is part of the vocabulary.
func IsGeography(token string) bool {
	return slices.Contains(Geographies, token)
}
