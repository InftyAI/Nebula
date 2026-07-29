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

package failover

import (
	"testing"
	"time"

	testclock "k8s.io/utils/clock/testing"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// baseTime is an arbitrary fixed instant; the fake clock starts here so tests are
// deterministic (package scripts forbid wall-clock reads anyway).
var baseTime = time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)

// ptr is a compact helper for the three-state BlockScope pointer fields.
func ptr(s string) *string { return &s }

func spotH100EastScope() provider.BlockScope {
	return provider.BlockScope{
		AcceleratorID: ptr("H100"),
		CapacityType:  nebulav1alpha1.CapacitySpot,
		Region:        ptr("us-east-1"),
	}
}

// cand builds a query Candidate compactly, keeping the table rows within the
// line-length limit.
func cand(prov, accel string, tier nebulav1alpha1.CapacityType, region string) Candidate {
	return Candidate{Provider: prov, AcceleratorID: accel, CapacityType: tier, Region: region}
}

const (
	spot     = nebulav1alpha1.CapacitySpot
	onDemand = nebulav1alpha1.CapacityOnDemand
)

func TestBlocked_ScopeMatchIsPrecise(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	bl.Record("aws", spotH100EastScope(), 10*time.Minute)

	tests := []struct {
		name string
		c    Candidate
		want bool
	}{
		{
			name: "exact match is blocked",
			c:    cand("aws", "H100", spot, "us-east-1"),
			want: true,
		},
		{
			name: "different tier (OnDemand) survives",
			c:    cand("aws", "H100", onDemand, "us-east-1"),
			want: false,
		},
		{
			name: "different accelerator survives",
			c:    cand("aws", "A100", spot, "us-east-1"),
			want: false,
		},
		{
			name: "different region survives",
			c:    cand("aws", "H100", spot, "us-west-2"),
			want: false,
		},
		{
			name: "different provider survives",
			c:    cand("modal", "H100", spot, "us-east-1"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bl.Blocked(tt.c); got != tt.want {
				t.Errorf("Blocked(%+v) = %v, want %v", tt.c, got, tt.want)
			}
		})
	}
}

func TestBlocked_WildcardFieldsMatchAnyValue(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	// Wildcard AcceleratorID + wildcard Region (&"") => blocks Spot on aws
	// everywhere, any GPU. The pointers are deliberately non-nil-empty: nil would
	// mean "no such axis" and match only an empty candidate field.
	bl.Record("aws", provider.BlockScope{
		AcceleratorID: ptr(""),
		CapacityType:  spot,
		Region:        ptr(""),
	}, 10*time.Minute)

	if !bl.Blocked(cand("aws", "H100", spot, "us-east-1")) {
		t.Error("H100/us-east-1 Spot should be blocked by the wildcard Spot scope")
	}
	if !bl.Blocked(cand("aws", "A100", spot, "eu-west-1")) {
		t.Error("A100/eu-west-1 Spot should also be blocked by the wildcard Spot scope")
	}
	if bl.Blocked(cand("aws", "H100", onDemand, "us-east-1")) {
		t.Error("OnDemand must survive a Spot-only wildcard block")
	}
}

// A nil axis is "not applicable": it matches only a candidate whose field is empty,
// never a populated one. This is how a region-simple provider (Modal: nil Region)
// or a CPU-only Pod (nil AcceleratorID) blocks without widening across an axis it
// never had. Contrast &"" (wildcard), which matches any value — see the test above.
func TestBlocked_NilFieldMatchesOnlyEmptyCandidate(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	// A region-simple provider's block: nil Region, nil AcceleratorID, Spot only.
	bl.Record("modal", provider.BlockScope{CapacityType: spot}, 10*time.Minute)

	// A region-simple candidate carries an empty region and (CPU Pod) empty
	// accelerator, so the nil axes match it.
	if !bl.Blocked(cand("modal", "", spot, "")) {
		t.Error("nil-axis Spot block should match the empty-field candidate")
	}
	// A populated candidate field is NOT matched by a nil axis: it is out of scope,
	// not wildcarded in.
	if bl.Blocked(cand("modal", "H100", spot, "")) {
		t.Error("nil AcceleratorID must not match a populated accelerator")
	}
	if bl.Blocked(cand("modal", "", spot, "us-east-1")) {
		t.Error("nil Region must not match a populated region")
	}
}

func TestBlocked_DenyAllBlocksWholeProvider(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	bl.Record("aws", provider.BlockScope{DenyAll: true}, 10*time.Minute)

	// Every accelerator/tier/region on aws is blocked...
	if !bl.Blocked(cand("aws", "H100", onDemand, "us-west-2")) {
		t.Error("DenyAll must block any aws candidate")
	}
	// ...but a different provider is untouched (a block never spans providers).
	if bl.Blocked(Candidate{Provider: "modal", AcceleratorID: "H100"}) {
		t.Error("DenyAll on aws must not block modal")
	}
}

func TestBlocked_ExpiresAfterTTL(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	bl.Record("aws", spotH100EastScope(), 10*time.Minute)

	c := cand("aws", "H100", spot, "us-east-1")
	if !bl.Blocked(c) {
		t.Fatal("should be blocked immediately after Record")
	}

	// Just before expiry: still blocked.
	fc.Step(10*time.Minute - time.Second)
	if !bl.Blocked(c) {
		t.Error("should still be blocked one second before TTL")
	}

	// Past expiry: cleared.
	fc.Step(2 * time.Second)
	if bl.Blocked(c) {
		t.Error("should be unblocked after TTL lapses")
	}
}

func TestBlockedUntil_ReportsTimeToSoonestFree(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	c := cand("aws", "H100", spot, "us-east-1")

	// Not blocked: retryAfter 0, blocked false.
	if until, blocked := bl.BlockedUntil(c); blocked || until != 0 {
		t.Fatalf("unblocked candidate: got (%v, %v), want (0, false)", until, blocked)
	}

	// Two covering entries; the candidate frees only when the LATER one lapses.
	bl.Record("aws", spotH100EastScope(), 5*time.Minute)
	bl.Record("aws", spotH100EastScope(), 12*time.Minute)
	until, blocked := bl.BlockedUntil(c)
	if !blocked {
		t.Fatal("candidate with live covering entries should be blocked")
	}
	if until != 12*time.Minute {
		t.Fatalf("should free when the last covering entry lapses; got %v want 12m", until)
	}

	// After the earlier entry lapses, the remaining one still bounds it.
	fc.Step(6 * time.Minute)
	if until, blocked := bl.BlockedUntil(c); !blocked || until != 6*time.Minute {
		t.Fatalf("after 6m: got (%v, %v), want (6m, true)", until, blocked)
	}

	// Past the last expiry: unblocked again.
	fc.Step(7 * time.Minute)
	if until, blocked := bl.BlockedUntil(c); blocked || until != 0 {
		t.Fatalf("after all TTLs lapse: got (%v, %v), want (0, false)", until, blocked)
	}
}

func TestRecord_NoOpOnInvalidInput(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)

	// Empty provider and non-positive TTL must not install a block.
	bl.Record("", provider.BlockScope{DenyAll: true}, 10*time.Minute)
	bl.Record("aws", provider.BlockScope{DenyAll: true}, 0)
	bl.Record("aws", provider.BlockScope{DenyAll: true}, -time.Minute)

	if bl.Blocked(Candidate{Provider: "aws", AcceleratorID: "H100"}) {
		t.Error("invalid Record inputs must not block anything")
	}
	if bl.Blocked(Candidate{Provider: "", AcceleratorID: "H100"}) {
		t.Error("empty-provider block must not exist")
	}
}

func TestRecord_CoalescesIdenticalScope(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)

	// Several Pods failing for the SAME (provider, accelerator, tier, region) reason,
	// each allocating its own scope value (distinct *string pointers), must collapse
	// to ONE entry rather than one per failure.
	for i := 0; i < 5; i++ {
		bl.Record("aws", spotH100EastScope(), 10*time.Minute)
	}
	if n := len(bl.entries); n != 1 {
		t.Fatalf("len(entries) = %d, want 1 (identical scopes must coalesce)", n)
	}
	if !bl.Blocked(cand("aws", "H100", spot, "us-east-1")) {
		t.Error("the coalesced entry must still block its scope")
	}
}

func TestRecord_CoalesceExtendsButNeverShortens(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	c := cand("aws", "H100", spot, "us-east-1")

	// A later failure with a LONGER TTL pushes the single entry's expiry out.
	bl.Record("aws", spotH100EastScope(), 5*time.Minute)
	bl.Record("aws", spotH100EastScope(), 12*time.Minute)
	if n := len(bl.entries); n != 1 {
		t.Fatalf("len(entries) = %d, want 1", n)
	}
	if until, _ := bl.BlockedUntil(c); until != 12*time.Minute {
		t.Fatalf("expiry should extend to the longer TTL; got %v want 12m", until)
	}

	// A subsequent SHORTER TTL must not shorten a still-live block (a fresh short
	// failure cannot make an existing longer block lapse early).
	bl.Record("aws", spotH100EastScope(), 3*time.Minute)
	if until, _ := bl.BlockedUntil(c); until != 12*time.Minute {
		t.Fatalf("a shorter later TTL must not shorten the live block; got %v want 12m", until)
	}
}

func TestRecord_DistinctScopesStaySeparate(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)

	// Scopes differing on any single axis are different blocks and must NOT coalesce.
	bl.Record("aws", spotH100EastScope(), 10*time.Minute) // H100/Spot/us-east-1
	bl.Record("aws", provider.BlockScope{                 // different region
		AcceleratorID: ptr("H100"), CapacityType: spot, Region: ptr("us-west-2"),
	}, 10*time.Minute)
	bl.Record("aws", provider.BlockScope{ // different tier
		AcceleratorID: ptr("H100"), CapacityType: onDemand, Region: ptr("us-east-1"),
	}, 10*time.Minute)
	bl.Record("modal", spotH100EastScope(), 10*time.Minute) // different provider

	if n := len(bl.entries); n != 4 {
		t.Fatalf("len(entries) = %d, want 4 (distinct scopes must not coalesce)", n)
	}
}

func TestRecord_NilAndWildcardScopesDoNotCoalesce(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)

	// nil (axis not applicable) and &"" (wildcard) are semantically different scopes,
	// so scopeEqual must keep them as separate entries.
	nilAxes := provider.BlockScope{CapacityType: spot}
	wildcardAxes := provider.BlockScope{AcceleratorID: ptr(""), Region: ptr(""), CapacityType: spot}
	bl.Record("aws", nilAxes, 10*time.Minute)
	bl.Record("aws", wildcardAxes, 10*time.Minute)

	if n := len(bl.entries); n != 2 {
		t.Fatalf("len(entries) = %d, want 2 (nil and wildcard scopes are distinct)", n)
	}
}

func TestRecord_ExpiredEntriesAreGarbageCollected(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	bl.Record("aws", spotH100EastScope(), time.Minute)

	// Let the first entry expire, then record another: the GC on Record should drop
	// the stale one so the slice does not grow unbounded.
	fc.Step(2 * time.Minute)
	bl.Record("aws", provider.BlockScope{DenyAll: true}, time.Minute)

	if n := len(bl.entries); n != 1 {
		t.Fatalf("len(entries) = %d, want 1 (stale entry should be GC'd on Record)", n)
	}
}
