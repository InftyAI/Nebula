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
	"strings"
	"testing"
)

// TestSanddEndpoint covers the normalizer that turns SANDD_EXTERNAL_HOST into the URL
// every daemon dials. It is worth testing directly because it is the LAST place a
// mistake is cheap: whatever comes out is baked into a provisioned instance's env
// file, and a wrong address is only discovered as an instance that bills without ever
// connecting.
func TestSanddEndpoint(t *testing.T) {
	cases := []struct {
		name string
		host string
		want string
	}{
		// The configuration an operator most likely writes: just the hostname.
		{"bare host", "sandd.example.com", "wss://sandd.example.com/ws"},
		// A port is preserved as given — an edge on a nonstandard port is normal for a
		// NodePort or a dev tunnel, and silently dropping it would produce an address
		// that resolves but refuses.
		{"host with port", "sandd.example.com:8765", "wss://sandd.example.com:8765/ws"},
		// Surrounding whitespace is the classic YAML/Secret paste artifact; it would
		// otherwise end up inside the hostname.
		{"whitespace trimmed", "  sandd.example.com\n", "wss://sandd.example.com/ws"},
		// A full URL passes through with the path already present.
		{"full wss url", "wss://sandd.example.com/ws", "wss://sandd.example.com/ws"},
		// A rooted URL still gets the WebSocket path: "/" is not a path the operator
		// chose, it is what a copy-pasted base URL ends with.
		{"rooted url gets the ws path", "wss://sandd.example.com/", "wss://sandd.example.com/ws"},
		{"schemed host no path", "wss://sandd.example.com", "wss://sandd.example.com/ws"},
		// An explicit path is the operator saying "the edge mounts the fleet here", so
		// it must survive verbatim — appending /ws would break a prefix-routed ingress.
		{"custom path preserved", "wss://edge.example.com/sandd/ws", "wss://edge.example.com/sandd/ws"},
		// ws:// is opt-in plaintext for a local edge that does not terminate TLS. It is
		// accepted but never defaulted to.
		{"explicit ws is honoured", "ws://localhost:8765", "ws://localhost:8765/ws"},
		{"ipv6 literal", "wss://[2001:db8::1]:8765", "wss://[2001:db8::1]:8765/ws"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sanddEndpoint(tc.host)
			if err != nil {
				t.Fatalf("sanddEndpoint(%q): unexpected error: %v", tc.host, err)
			}
			if got != tc.want {
				t.Errorf("sanddEndpoint(%q) = %q, want %q", tc.host, got, tc.want)
			}
		})
	}
}

// TestSanddEndpoint_DefaultsToTLS is the security assertion, separate from the table
// so it cannot be lost in a refactor of the cases above: a host given WITHOUT a
// scheme must come back wss://, never ws://. The daemon presents a bearer token on
// this connection, so a plaintext default would put every token on the wire in the
// clear across the public internet.
func TestSanddEndpoint_DefaultsToTLS(t *testing.T) {
	for _, host := range []string{"sandd.example.com", "sandd.example.com:8765", "10.0.0.1"} {
		got, err := sanddEndpoint(host)
		if err != nil {
			t.Fatalf("sanddEndpoint(%q): %v", host, err)
		}
		if !strings.HasPrefix(got, "wss://") {
			t.Errorf("sanddEndpoint(%q) = %q, want a wss:// URL: an unschemed host must "+
				"never default to plaintext, which would expose the daemon token", host, got)
		}
	}
}

// TestSanddEndpoint_Rejects: every rejected form is one that would otherwise produce
// a plausible-looking address no daemon can use. Failing here stops the operator
// before any instance is provisioned against it.
func TestSanddEndpoint_Rejects(t *testing.T) {
	cases := []struct {
		name string
		host string
	}{
		// https is the most likely mistake — an operator copies the ingress URL from
		// their browser. It would be a WebSocket-less scheme with a valid-looking host,
		// so it must be named as wrong rather than silently rewritten.
		{"https scheme", "https://sandd.example.com"},
		{"http scheme", "http://sandd.example.com"},
		{"unknown scheme", "tcp://sandd.example.com:8765"},
		// A scheme-less value with a path is ambiguous: "example.com/ws" could be a host
		// or a path-relative URL, and guessing either way risks a wrong address.
		{"bare host with a path", "sandd.example.com/ws"},
		{"scheme with no host", "wss://"},
		{"empty", ""},
		{"whitespace only", "   "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sanddEndpoint(tc.host)
			if err == nil {
				t.Fatalf("sanddEndpoint(%q) = %q, want an error", tc.host, got)
			}
			// The message must quote what the operator actually set; the whole point of
			// failing fast is that they can see which value to fix.
			if tc.host != "" && !strings.Contains(err.Error(), "SANDD_EXTERNAL_HOST") {
				t.Errorf("error should name the variable to fix, got: %v", err)
			}
		})
	}
}
