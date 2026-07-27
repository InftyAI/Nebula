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

// Package version exposes the build version of the Nebula manager. The value is
// stamped at build time via -ldflags (see the Makefile LDFLAGS and Dockerfile);
// an unstamped build (e.g. `go run`, `go test`) reports the devFallback below.
package version

// gitVersion is overridden at link time with:
//
//	-ldflags "-X github.com/InftyAI/Nebula/pkg/version.gitVersion=<value>"
//
// Left lowercase/unexported so it can only be set via ldflags or read through
// Get(), never reassigned at runtime.
var gitVersion = devFallback

// devFallback is what an unstamped build reports. It is deliberately not a
// version-looking string, so a "nebula-dev" in the field is an obvious signal
// the binary was built without the version ldflag.
const devFallback = "nebula-dev"

// Get returns the build version, e.g. "v0.1.0", "v0.1.0-3-gabc123" (git
// describe of an untagged commit), or "nebula-dev" for an unstamped build.
func Get() string {
	if gitVersion == "" {
		return devFallback
	}
	return gitVersion
}
