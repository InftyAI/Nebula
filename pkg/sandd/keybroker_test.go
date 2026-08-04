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

package sandd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewBrokerClient_EmptyURLIsNil(t *testing.T) {
	// An empty URL must yield a nil client so callers can treat "no broker" as "no
	// dynamic minting" without a separate flag.
	if c := NewBrokerClient(""); c != nil {
		t.Fatalf("NewBrokerClient(\"\") = %v, want nil", c)
	}
	if c := NewBrokerClient("   "); c != nil {
		t.Fatalf("NewBrokerClient(whitespace) = %v, want nil", c)
	}
}

func TestMintDaemonKey_Success(t *testing.T) {
	var gotKind, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotKind = r.URL.Query().Get("kind")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"key":"tskey-abc","kind":"daemon"}`))
	}))
	defer srv.Close()

	c := NewBrokerClient(srv.URL)
	key, err := c.MintDaemonKey(context.Background())
	if err != nil {
		t.Fatalf("MintDaemonKey: %v", err)
	}
	if key != "tskey-abc" {
		t.Errorf("key = %q, want tskey-abc", key)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %s, want POST", gotMethod)
	}
	// The daemon path must request the daemon kind (single-use + ephemeral).
	if gotKind != "daemon" {
		t.Errorf("kind = %q, want daemon", gotKind)
	}
}

func TestMintDaemonKey_TrailingSlashBaseURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A doubled slash (//keys) would mean the base URL wasn't trimmed.
		if r.URL.Path != "/keys" {
			t.Errorf("path = %q, want /keys", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"key":"tskey-x"}`))
	}))
	defer srv.Close()

	c := NewBrokerClient(srv.URL + "/")
	if _, err := c.MintDaemonKey(context.Background()); err != nil {
		t.Fatalf("MintDaemonKey: %v", err)
	}
}

func TestMintDaemonKey_Non200IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer srv.Close()

	c := NewBrokerClient(srv.URL)
	if _, err := c.MintDaemonKey(context.Background()); err == nil {
		t.Fatalf("expected error on 502")
	}
}

func TestMintDaemonKey_EmptyKeyIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"key":""}`))
	}))
	defer srv.Close()

	c := NewBrokerClient(srv.URL)
	if _, err := c.MintDaemonKey(context.Background()); err == nil {
		t.Fatalf("expected error on empty key")
	}
}
