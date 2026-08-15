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
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/InftyAI/Nebula/pkg/provider"
)

// The reason label is a CLOSED set, so this pins every mapping into it. The label is
// what an operator reads to decide whether a provisioning problem is theirs (quota,
// credentials), the provider's (capacity, unreachable) or neither.
func TestFailureReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil has no reason", nil, ""},
		{"auth", provider.ErrAuth, ReasonAuth},
		{"quota", provider.ErrQuota, ReasonQuota},
		{"unsupported", provider.ErrUnsupportedAccelerator, ReasonUnsupported},
		{"capacity", provider.ErrNoCapacity, ReasonCapacity},
		{"wrapped capacity", fmt.Errorf("create sandbox: %w", provider.ErrNoCapacity), ReasonCapacity},

		// A provider that hits its own deadline WHILE sweeping for capacity wraps both;
		// the capacity cause is the more useful of the two, so the sentinels come first.
		{"capacity beats timeout", fmt.Errorf("%w: %w", provider.ErrNoCapacity, context.DeadlineExceeded), ReasonCapacity},
		{"bare timeout", context.DeadlineExceeded, ReasonTimeout},

		// The split that matters operationally: an integration outage must not read as a
		// capacity shortfall, or the failure series points at the wrong problem entirely.
		{"grpc transport", errors.New("rpc error: code = Unavailable desc = transport is closing"), ReasonUnreachable},
		{"connection refused", errors.New("dial tcp: connect: connection refused"), ReasonUnreachable},
		{"unrecognized", errors.New("weird transient blip"), ReasonUnreachable},

		// An unwrapped provider message is recognized as a REJECTION (so not
		// "unreachable"), but its category is unavailable here: FailureReason matches
		// sentinels only, on purpose, rather than re-running the string heuristics. So it
		// lands on "other", which is precisely the signal that an adapter is not wrapping
		// its errors — actionable, unlike a guess at the category.
		{"unwrapped rejection", errors.New("InsufficientInstanceCapacity"), ReasonOther},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FailureReason(tt.err); got != tt.want {
				t.Fatalf("FailureReason(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

// ObserveProvision is the single call site for both outcomes, so the attempt and
// failure counters can never drift: a failure must increment both, a success only the
// attempt counter.
func TestObserveProvision_CountersStayInStep(t *testing.T) {
	l := Labels{Provider: "p", Region: "r", CapacityType: "OnDemand", Accelerator: "H100", AcceleratorCount: 1}

	failureBefore := counterValue(t, ProvisionAttempts.WithLabelValues(l.values(ResultFailure)...))
	reasonBefore := counterValue(t, ProvisionFailures.WithLabelValues(l.values(ReasonCapacity)...))
	successBefore := counterValue(t, ProvisionAttempts.WithLabelValues(l.values(ResultSuccess)...))

	ObserveProvision(l, 0, provider.ErrNoCapacity)
	ObserveProvision(l, 0, nil)

	if got := counterValue(t, ProvisionAttempts.WithLabelValues(l.values(ResultFailure)...)) - failureBefore; got != 1 {
		t.Fatalf("failure attempts delta = %v, want 1", got)
	}
	if got := counterValue(t, ProvisionFailures.WithLabelValues(l.values(ReasonCapacity)...)) - reasonBefore; got != 1 {
		t.Fatalf("capacity failures delta = %v, want 1", got)
	}
	if got := counterValue(t, ProvisionAttempts.WithLabelValues(l.values(ResultSuccess)...)) - successBefore; got != 1 {
		t.Fatalf("success attempts delta = %v, want 1", got)
	}
	// A success must never touch the failure-reason counter, whatever the reason.
	allReasons := []string{
		ReasonCapacity, ReasonAuth, ReasonQuota,
		ReasonUnsupported, ReasonTimeout, ReasonUnreachable, ReasonOther,
	}
	for _, reason := range allReasons {
		if reason == ReasonCapacity {
			continue // asserted above
		}
		if got := counterValue(t, ProvisionFailures.WithLabelValues(l.values(reason)...)); got != 0 {
			t.Fatalf("reason %q counted %v times, want 0", reason, got)
		}
	}
}
