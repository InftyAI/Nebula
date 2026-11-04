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

func spotH100EastScope() provider.BlockScope {
	return provider.BlockScope{
		Deny:         provider.DenyCandidate,
		Accelerator:  "H100",
		CapacityType: nebulav1alpha1.CapacitySpot,
		Region:       "us-east-1",
	}
}

// cand builds a query Candidate compactly, keeping the table rows within the
// line-length limit.
func cand(prov, accel string, tier nebulav1alpha1.CapacityType, region string) Candidate {
	return Candidate{Provider: prov, Accelerator: accel, CapacityType: tier, Region: region}
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

func TestBlocked_DenyRegionBlocksOneRegion(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	bl.Record("kubernetes", provider.BlockScope{Deny: provider.DenyRegion, Region: "us-east-1"}, 10*time.Minute)

	if !bl.Blocked(cand("kubernetes", "H100", spot, "us-east-1")) ||
		!bl.Blocked(cand("kubernetes", "A100", onDemand, "us-east-1")) {
		t.Error("DenyRegion must block every accelerator and tier in its region")
	}
	if bl.Blocked(cand("kubernetes", "H100", spot, "us-west-1")) {
		t.Error("DenyRegion must not block another region")
	}
}

func TestBlocked_EmptyCapacityTypeMatchesEveryTier(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	anyTier := provider.BlockScope{Deny: provider.DenyCandidate, Accelerator: "H100", Region: "us-east-1"}
	bl.Record("aws", anyTier, 10*time.Minute)

	if !bl.Blocked(cand("aws", "H100", spot, "us-east-1")) || !bl.Blocked(cand("aws", "H100", onDemand, "us-east-1")) {
		t.Error("an empty CapacityType must block both tiers")
	}
	if bl.Blocked(cand("aws", "A100", spot, "us-east-1")) {
		t.Error("the accelerator still matches exactly")
	}
}

// Empty Accelerator and Region match only candidates without them: a CPU-only Pod or a
// region-less provider (Modal) blocks without widening to a GPU or a region-pinned one.
func TestBlocked_EmptyFieldsMatchOnlyEmptyCandidate(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	bl.Record("modal", provider.BlockScope{Deny: provider.DenyCandidate, CapacityType: spot}, 10*time.Minute)

	if !bl.Blocked(cand("modal", "", spot, "")) {
		t.Error("the block should match the empty-field candidate")
	}
	if bl.Blocked(cand("modal", "H100", spot, "")) {
		t.Error("an empty Accelerator must not match a populated accelerator")
	}
	if bl.Blocked(cand("modal", "", spot, "us-east-1")) {
		t.Error("an empty Region must not match a populated region")
	}
}

func TestBlocked_DenyAllBlocksWholeProvider(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	bl.Record("aws", provider.BlockScope{Deny: provider.DenyAll}, 10*time.Minute)

	// Every accelerator/tier/region on aws is blocked...
	if !bl.Blocked(cand("aws", "H100", onDemand, "us-west-2")) {
		t.Error("DenyAll must block any aws candidate")
	}
	// ...but a different provider is untouched (a block never spans providers).
	if bl.Blocked(Candidate{Provider: "modal", Accelerator: "H100"}) {
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
	bl.Record("", provider.BlockScope{Deny: provider.DenyAll}, 10*time.Minute)
	bl.Record("aws", provider.BlockScope{Deny: provider.DenyAll}, 0)
	bl.Record("aws", provider.BlockScope{Deny: provider.DenyAll}, -time.Minute)

	if bl.Blocked(Candidate{Provider: "aws", Accelerator: "H100"}) {
		t.Error("invalid Record inputs must not block anything")
	}
	if bl.Blocked(Candidate{Provider: "", Accelerator: "H100"}) {
		t.Error("empty-provider block must not exist")
	}

	// A scope without a known policy covers nothing, so it is not recorded either.
	bl.Record("aws", provider.BlockScope{}, 10*time.Minute)
	bl.Record("aws", provider.BlockScope{Deny: "Everything", Region: "us-east-1"}, 10*time.Minute)
	if n := len(bl.entries); n != 0 {
		t.Errorf("len(entries) = %d, want 0 for scopes without a known policy", n)
	}
}

func TestRecord_CoalescesIdenticalScope(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)

	// Several Pods failing for the SAME (provider, accelerator, tier, region) reason must
	// collapse to ONE entry rather than one per failure.
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
	westScope, onDemandScope := spotH100EastScope(), spotH100EastScope()
	westScope.Region, onDemandScope.CapacityType = "us-west-2", onDemand
	regionScope := provider.BlockScope{Deny: provider.DenyRegion, Region: "us-east-1"}
	bl.Record("aws", westScope, 10*time.Minute)             // different region
	bl.Record("aws", onDemandScope, 10*time.Minute)         // different tier
	bl.Record("aws", regionScope, 10*time.Minute)           // different policy
	bl.Record("modal", spotH100EastScope(), 10*time.Minute) // different provider

	if n := len(bl.entries); n != 5 {
		t.Fatalf("len(entries) = %d, want 5 (distinct scopes must not coalesce)", n)
	}
}

func TestRecord_ExpiredEntriesAreGarbageCollected(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	bl.Record("aws", spotH100EastScope(), time.Minute)

	// Let the first entry expire and its probe pass unrefused, then record another: the GC
	// on Record should drop the stale one so the slice stays bounded.
	fc.Step(time.Minute)
	bl.Admit(cand("aws", "H100", spot, "us-east-1"))
	fc.Step(ProbeWindow)
	bl.Record("aws", provider.BlockScope{Deny: provider.DenyAll}, time.Minute)

	if n := len(bl.entries); n != 1 {
		t.Fatalf("len(entries) = %d, want 1 (stale entry should be GC'd on Record)", n)
	}
}

// A lapsed block admits ONE probe; every other caller is held back for ProbeWindow, so a
// candidate still out costs one attempt per expiry instead of one per waiting Pod.
func TestAdmit_HalfOpenAdmitsOneProbe(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	c := cand("aws", "H100", spot, "us-east-1")
	bl.Record("aws", spotH100EastScope(), 30*time.Second)

	if until, ok := bl.Admit(c); ok || until != 30*time.Second {
		t.Fatalf("while blocked: Admit = (%v, %v), want (30s, false)", until, ok)
	}
	fc.Step(30 * time.Second)
	if _, ok := bl.Admit(c); !ok {
		t.Fatal("the first Admit after the block lapsed must be the probe")
	}
	for i := 0; i < 3; i++ {
		if until, ok := bl.Admit(c); ok || until != ProbeWindow {
			t.Fatalf("Admit %d during the probe = (%v, %v), want (%v, false)", i, until, ok, ProbeWindow)
		}
	}
	if !bl.Blocked(c) {
		t.Error("a probing candidate must read as blocked to everyone else")
	}
}

// A refused probe is recorded like any failure and blocks the candidate again for its TTL.
func TestAdmit_RefusedProbeBlocksAgain(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	c := cand("aws", "H100", spot, "us-east-1")
	bl.Record("aws", spotH100EastScope(), 30*time.Second)
	fc.Step(30 * time.Second)
	if _, ok := bl.Admit(c); !ok {
		t.Fatal("want the probe admitted")
	}

	fc.Step(15 * time.Second) // the node autoscaler refuses the probe
	bl.Record("aws", spotH100EastScope(), 30*time.Second)
	if until, ok := bl.Admit(c); ok || until != 30*time.Second {
		t.Fatalf("after the probe was refused: Admit = (%v, %v), want (30s, false)", until, ok)
	}
	// And the cycle repeats: one probe once that block lapses.
	fc.Step(30 * time.Second)
	if _, ok := bl.Admit(c); !ok {
		t.Fatal("want the next probe admitted once the renewed block lapsed")
	}
}

// A probe not refused within ProbeWindow was let through, so the candidate opens for all.
func TestAdmit_UnrefusedProbeOpensTheCandidate(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	c := cand("aws", "H100", spot, "us-east-1")
	bl.Record("aws", spotH100EastScope(), 30*time.Second)
	fc.Step(30 * time.Second)
	bl.Admit(c)

	fc.Step(ProbeWindow)
	for i := 0; i < 3; i++ {
		if _, ok := bl.Admit(c); !ok {
			t.Fatalf("Admit %d after an unrefused probe = false, want the candidate open", i)
		}
	}
	if n := len(bl.entries); n != 0 {
		t.Fatalf("len(entries) = %d, want 0 once the probe window passed", n)
	}
}

// Only Admit claims the probe: a read-only query of a half-open candidate sees it as not
// blocked and leaves the probe for placement.
func TestBlockedUntil_NeverAdmitsAProbe(t *testing.T) {
	fc := testclock.NewFakeClock(baseTime)
	bl := newWithClock(fc)
	c := cand("aws", "H100", spot, "us-east-1")
	bl.Record("aws", spotH100EastScope(), 30*time.Second)
	fc.Step(30 * time.Second)

	for i := 0; i < 3; i++ {
		if until, blocked := bl.BlockedUntil(c); blocked || until != 0 {
			t.Fatalf("BlockedUntil %d of a half-open candidate = (%v, %v), want (0, false)", i, until, blocked)
		}
	}
	if _, ok := bl.Admit(c); !ok {
		t.Fatal("the probe must still be available to Admit")
	}
	if _, ok := bl.Admit(cand("aws", "A100", spot, "us-east-1")); !ok {
		t.Fatal("a candidate no entry covers is always admitted")
	}
}
