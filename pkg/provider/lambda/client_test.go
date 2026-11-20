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

package lambda

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestLaunchUsesBearerAndRejectsAmbiguousSuccess(t *testing.T) {
	for _, ids := range []string{`["vm-1"]`, `[]`, `["vm-1","vm-2"]`} {
		t.Run(ids, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/instance-operations/launch" ||
					r.Header.Get("Authorization") != "Bearer test-key" {
					t.Errorf("unexpected launch request: %s %s", r.Method, r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"data":{"instance_ids":%s}}`, ids)
			}))
			defer server.Close()
			c := testClient(server)
			id, err := c.launch(context.Background(), launchRequest{})
			if ids == `["vm-1"]` {
				if err != nil || id != "vm-1" {
					t.Fatalf("launch = %q, %v", id, err)
				}
			} else if err == nil || definitelyRejected(err) {
				t.Fatalf("ambiguous response = %q, %v", id, err)
			}
		})
	}
}

func TestListDoesNotReturnPartialResults(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("workspace_id") != "test-workspace" {
			t.Error("missing workspace filter")
		}
		if calls.Add(1) == 1 {
			_, _ = fmt.Fprint(w, `{"data":[{"id":"vm-1"}],"page_token":"next"}`)
			return
		}
		if r.URL.Query().Get("page_token") != "next" {
			t.Error("missing continuation token")
		}
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}))
	defer server.Close()
	got, err := testClient(server).list(context.Background(), "test-workspace")
	if err == nil || got != nil || calls.Load() != 2 {
		t.Fatalf("list = %+v, %v, calls %d", got, err, calls.Load())
	}
}

func TestLaunchErrorClassification(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"error":{"code":"instance-operations/launch/insufficient-capacity",`+
			`"message":"credentials from user-data","request_id":"request-123"}}`)
	}))
	defer server.Close()
	_, err := testClient(server).launch(context.Background(), launchRequest{})
	var apiErr *apiError
	if !errors.As(err, &apiErr) || !definitelyRejected(err) ||
		apiErr.code != "instance-operations/launch/insufficient-capacity" {
		t.Fatalf("launch error = %v", err)
	}
	if err.Error() != "lambda API: HTTP 400 (instance-operations/launch/insufficient-capacity, request request-123)" {
		t.Fatalf("unredacted or incomplete error: %v", err)
	}
}

func testClient(server *httptest.Server) *cloudClient {
	return &cloudClient{baseURL: server.URL, token: "test-key", http: server.Client()}
}
