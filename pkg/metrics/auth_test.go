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

package metrics

import (
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/InftyAI/Nebula/pkg/provider"
)

// Auth failures count and raise the gauge, a success lowers it, and any other error leaves
// both alone.
func TestRecordProviderAuth(t *testing.T) {
	const prov = "auth-test"
	failures := ProviderAuthFailures.WithLabelValues(prov)
	failing := ProviderAuthFailing.WithLabelValues(prov)
	before := counterValue(t, failures)

	steps := []struct {
		name        string
		err         error
		wantFailing float64
		wantCount   float64
	}{
		{"auth failure", fmt.Errorf("list: %w", provider.ErrAuth), 1, 1},
		{"timeout is no verdict", errors.New("i/o timeout"), 1, 1},
		{"another auth failure", provider.ErrAuth, 1, 2},
		{"success", nil, 0, 2},
		{"timeout after success", errors.New("i/o timeout"), 0, 2},
	}
	for _, s := range steps {
		RecordProviderAuth(prov, s.err)
		if got := testutil.ToFloat64(failing); got != s.wantFailing {
			t.Errorf("%s: failing = %v, want %v", s.name, got, s.wantFailing)
		}
		if got := counterValue(t, failures) - before; got != s.wantCount {
			t.Errorf("%s: failures = %v, want %v", s.name, got, s.wantCount)
		}
	}
}
