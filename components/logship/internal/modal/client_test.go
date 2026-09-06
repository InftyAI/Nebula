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

package modal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSDKVersionMatchesModulePin is what keeps the version we CLAIM to be honest. The header value
// is a const so nothing has to read build info at runtime, and a const is exactly the kind of
// thing a dependency bump forgets.
//
// Read out of go.mod rather than debug.ReadBuildInfo, which reports no deps at all under go test.
func TestSDKVersionMatchesModulePin(t *testing.T) {
	const module = "github.com/modal-labs/modal-client/go"

	gomod, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatalf("reading go.mod: %v", err)
	}
	for line := range strings.Lines(string(gomod)) {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != module {
			continue
		}
		if want := strings.TrimPrefix(fields[1], "v"); want != sdkVersion {
			t.Fatalf("sdkVersion = %q, but go.mod pins %s at %q", sdkVersion, module, want)
		}
		return
	}
	t.Fatalf("%s is not required by go.mod", module)
}

func TestCredentialsFromEnv(t *testing.T) {
	t.Run("defaults the server URL", func(t *testing.T) {
		t.Setenv("MODAL_TOKEN_ID", "ak-1")
		t.Setenv("MODAL_TOKEN_SECRET", "as-1")
		t.Setenv("MODAL_SERVER_URL", "")

		creds, err := CredentialsFromEnv()
		if err != nil {
			t.Fatalf("CredentialsFromEnv: %v", err)
		}
		if creds.ServerURL != defaultServerURL {
			t.Fatalf("ServerURL = %q, want the default", creds.ServerURL)
		}
	})

	// A missing token has to fail HERE rather than as an opaque Unauthenticated on the first
	// stream, because the tokens are the one thing this cannot fall back to a profile file for.
	t.Run("refuses a missing token", func(t *testing.T) {
		t.Setenv("MODAL_TOKEN_ID", "ak-1")
		t.Setenv("MODAL_TOKEN_SECRET", "")

		if _, err := CredentialsFromEnv(); err == nil {
			t.Fatal("CredentialsFromEnv accepted a missing token secret")
		}
	})
}

func TestDialTargetRejectsASchemelessURL(t *testing.T) {
	if _, _, err := dialTarget("api.modal.com:443"); err == nil {
		t.Fatal("dialTarget accepted a URL with no scheme")
	}
}
