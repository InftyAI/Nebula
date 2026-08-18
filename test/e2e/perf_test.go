//go:build e2e

/*
Copyright 2026.

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

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	// Aliased: "util" and "utils" one letter apart in the same file is a trap. This is the
	// production helper, used so the benchmark derives claim names exactly as the controller
	// does; utils below is the e2e test helper.
	nebulautil "github.com/InftyAI/Nebula/pkg/util"
	"github.com/InftyAI/Nebula/test/utils"
)

// Sync-benchmark fixtures. Own namespace and pool, so the numbers are not perturbed
// by the placement spec's leftovers.
const (
	perfPoolName   = "e2e-perf-pool"
	perfWorkloadNS = "nebula-e2e-perf"
	// perfDeployName is also the prefix every replica's Pod name carries, which is how
	// the poller below picks this batch out of a cluster-wide list.
	perfDeployName   = "e2e-perf-workload"
	perfManifestFile = "/tmp/nebula-perf-workloads.yaml"

	// defaultPerfReplicas is the batch size when NEBULA_E2E_PERF_WORKLOADS is unset.
	// Cheap at this size — the replicas bind to a VIRTUAL node, so the Kind kubelet
	// never runs a container and the cost is control-plane only. `make test-perf`
	// passes a larger default.
	defaultPerfReplicas = 500

	// perfPollInterval is the sleep between polls. Latencies are quantized by it plus
	// the two list calls each poll makes, because an event is attributed to the first
	// poll that OBSERVES it — so every sample overstates a little. It has to stay well
	// under the times being measured: at 500ms the whole distribution collapsed into
	// one bucket and p50 read the same as p95.
	//
	// Note this is unrelated to Gomega's polling interval (this loop is not an
	// Eventually: it stamps a first-seen time per object, and reports before it
	// asserts) and to the provider poll interval in Capabilities(), which paces status
	// refresh rather than placement.
	perfPollInterval = 100 * time.Millisecond

	// perfStallTimeout is the real tripwire: a batch that stops advancing entirely for
	// this long is wedged (a stuck queue, a lost watch, a provider call that never
	// returns), and the spec should say so at once rather than sit out an absolute
	// deadline that has to be sized for the largest N anyone runs. A healthy 500-replica
	// run advances every poll, and even its slowest stage moved every 10s, so a full
	// minute of total silence is not slowness — it is a stop.
	//
	// Failing on the STALL rather than on total time is what lets the budgets below
	// stay loose without making a wedged run cost minutes to report.
	perfStallTimeout = time.Minute
)

// perfReplicas is the batch size, overridable for a bigger run.
func perfReplicas() int {
	raw := os.Getenv("NEBULA_E2E_PERF_WORKLOADS")
	if raw == "" {
		return defaultPerfReplicas
	}
	n, err := strconv.Atoi(raw)
	Expect(err).NotTo(HaveOccurred(), "NEBULA_E2E_PERF_WORKLOADS must be an integer")
	Expect(n).To(BeNumerically(">", 0), "NEBULA_E2E_PERF_WORKLOADS must be positive")
	return n
}

// perfBudget and perfTeardownBudget are absolute BACKSTOPS, not the primary check:
// perfStallTimeout is what actually catches a wedged path, and it does so in about a
// minute whatever N is. So these only need to be generous enough not to flake on a
// loaded machine while still bounding a run that crawls forever — measured at 500
// replicas, the sync took 65s against a 3m ceiling and the drain 103s against 3m30s.
//
// Neither is a latency SLO. Read the reported percentiles for that.
func perfBudget(n int) time.Duration {
	return 30*time.Second + time.Duration(n)*time.Second
}

// perfTeardownBudget is separate, and larger, because draining is measurably the
// slowest stage: every claim carries the terminate finalizer, so N deletes plus N
// finalizer removals go through the same client the controllers are already using.
// Sharing one budget with the sync above is what forced that ceiling to be absurd.
func perfTeardownBudget(n int) time.Duration {
	return time.Minute + time.Duration(n)*300*time.Millisecond
}

// benchmarkWorkloadSync creates ONE Deployment of N gated replicas and measures how
// long the control plane takes to sync all of them: webhook gate → placement →
// NodeClaim Bound → Pod bound to the virtual node → Pod Ready.
//
// It runs to READY, not to bound, before deleting anything: bound is a scheduling
// decision, while Ready is the vnode reporting the instance running, and only the second
// one means a user could use the box (see syncSamples.ready).
//
// A Deployment rather than N Pod manifests, for two reasons: it is the shape a user
// actually scales, and the replicas are then created in-cluster by the ReplicaSet
// controller instead of one-at-a-time by kubectl — so the measurement is not dominated
// by the client's serial creates. The cost of creation is still visible, as its own
// stage, and the per-Pod "created → bound" column isolates Nebula's own contribution
// from how fast the replicas arrived.
//
// Lives in the Manager container's ordered run because the manager is deployed by its
// BeforeAll and undeployed by its AfterAll. Labelled perf so it is excluded from
// `make test-e2e` and selected by `make test-perf`.
func benchmarkWorkloadSync() {
	n := perfReplicas()

	// Written from a DeferCleanup, and the values are filled in as the spec proceeds, so
	// a run that fails half way still leaves a report of how far it got — which is when
	// the numbers are most worth reading.
	var (
		s syncSamples
		// start is t0, the instant before the apply. Declared up here, not at the apply, so
		// the cleanup below can price a run that never reached the end.
		start time.Time
		// total is the whole benchmark: apply until the last claim is gone. It is not
		// drainTotal + the sync window — the gap where the sync numbers are reported and
		// asserted falls inside it too, which is exactly why it is measured rather than
		// added up.
		total      time.Duration
		drainTotal time.Duration
		d          drainSamples
	)
	DeferCleanup(func() {
		// A failed sync assertion aborts the spec before total is assigned, and a report
		// whose headline number is an em dash reads as "the harness broke" rather than "the
		// batch did not finish". So stamp what the run actually consumed: apply until it
		// gave up. A zero start means it failed before t0 — nothing was measured, so the
		// card stays empty rather than reporting cluster setup as a benchmark.
		//
		// drainTotal is deliberately left alone: no teardown ran, and a number there would
		// invent a stage.
		if total == 0 && !start.IsZero() {
			total = time.Since(start)
		}
		writeHTMLReport(n, s, total, drainTotal, d)
	})

	// Everything the batch depends on, before t0 — the webhook that gates the Pods and the
	// node they bind to. Nothing here is timed: waiting for the manager is setup, and
	// folding it into the first stage would report the manager's boot as Nebula's placement
	// cost. See waitForControllerReady for what a half-started manager does to the numbers.
	waitForControllerReady()

	// A claim left over from the placement spec would land in the teardown timing
	// below, which waits on claims cluster-wide.
	By("waiting for prior NodeClaims to drain so the batch starts from a clean ledger")
	Expect(waitForNodeClaimsGone(time.Minute)).To(BeTrue(), "prior NodeClaims did not drain")

	By("creating the perf namespace (webhook-eligible, restricted policy)")
	_, _ = utils.Run(exec.Command("kubectl", "create", "ns", perfWorkloadNS))
	_, err := utils.Run(exec.Command("kubectl", "label", "--overwrite", "ns", perfWorkloadNS,
		"pod-security.kubernetes.io/enforce=restricted"))
	Expect(err).NotTo(HaveOccurred(), "Failed to label the perf namespace")

	Expect(os.WriteFile(perfManifestFile, []byte(perfManifest(n)), 0o644)).To(Succeed())

	By(fmt.Sprintf("creating a NodePool and a Deployment of %d gated replicas", n))
	// The clock starts before the create, so every sample includes the whole path from
	// "the user asked" onward.
	start = time.Now()
	_, err = utils.Run(exec.Command("kubectl", "apply", "-f", perfManifestFile))
	Expect(err).NotTo(HaveOccurred(), "Failed to apply the perf Deployment")
	// The apply's own round trip is deliberately not reported: it is the client's cost,
	// and nothing Nebula does can move it. What matters is that it is t0.

	budget := perfBudget(n)
	By(fmt.Sprintf("waiting for all %d replicas to sync (budget %s)", n, budget))
	s = watchBatchSync(n, start, budget)

	reportBatchSync(n, s)

	// Name which of the two failure modes this was, since they lead different places.
	gaveUp := fmt.Sprintf("within the %s budget", budget)
	if s.stalled {
		gaveUp = fmt.Sprintf("before stalling (nothing advanced for %s)", perfStallTimeout)
	}
	Expect(s.bound).To(HaveLen(n), fmt.Sprintf(
		"only %d/%d Pods bound to %s %s (%d/%d created, %d/%d claims Bound, %d/%d Ready)",
		len(s.bound), n, fakeVirtualNode, gaveUp, len(s.created), n, len(s.claims), n, len(s.ready), n))
	Expect(s.claims).To(HaveLen(n), fmt.Sprintf(
		"only %d/%d NodeClaims reached Bound %s", len(s.claims), n, gaveUp))
	// Asserted BEFORE the delete below, and that order is the point: teardown timed while
	// part of the batch is still booting measures a delete racing provisioning, which is a
	// different thing from draining a settled fleet and cannot be told apart afterwards.
	Expect(s.ready).To(HaveLen(n), fmt.Sprintf(
		"only %d/%d Pods became Ready %s (%d/%d bound, so the rest were placed but never "+
			"reported running)", len(s.ready), n, gaveUp, len(s.bound), n))

	By("deleting the batch and timing the teardown")
	teardownBudget := perfTeardownBudget(n)
	teardown := time.Now()
	_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", perfManifestFile,
		"--ignore-not-found=true", "--wait=false"))
	d = drainPerfClaims(teardownBudget, s.podNames)
	drainTotal = time.Since(teardown)
	total = time.Since(start)

	reportDrain(total, drainTotal, d)

	drainGaveUp := fmt.Sprintf("within the %s budget", teardownBudget)
	if d.stalled {
		drainGaveUp = fmt.Sprintf("and the count stopped falling for %s", perfStallTimeout)
	}
	Expect(d.remaining).To(BeZero(), fmt.Sprintf(
		"%d/%d NodeClaims still present after deleting the batch %s", d.remaining, n, drainGaveUp))
}

// syncSamples holds the per-workload latencies of each stage, ascending. All are
// measured from the moment the Deployment was created, except sync — see below. A
// slice shorter than n means the watch gave up — stalled, or out of budget — before
// that stage finished.
type syncSamples struct {
	created []time.Duration // Pod object exists
	claims  []time.Duration // NodeClaim reached Bound
	bound   []time.Duration // Pod bound to the virtual node
	// ready is the Pod's Ready condition turning True, which is the only stage here that
	// says the workload can actually be USED: bound means the scheduler is done with it,
	// and a NodeClaim reaches Bound while the instance is merely known to exist (see
	// desiredPhase — Initializing counts). So a batch can clear every other stage with
	// every Pod still booting, and the teardown below would then be timing a delete that
	// races provisioning rather than one against a settled fleet.
	//
	// It is also the stage most likely to be slow, because it is the only one that waits on
	// a status write per Pod from the virtual kubelet rather than on a one-off decision.
	ready []time.Duration
	// placement and provision are the two PER-OBJECT windows, the only samples here not
	// measured from the apply. Dividing the arrival rate out is what keeps them comparable
	// between runs of different sizes.
	//
	// placement is per Pod, bound − created: the gate coming off plus the bind.
	placement []time.Duration
	// provision is per NodeClaim, Bound − created: created until an instance is known to
	// exist. Not the provider call alone — a claim goes Bound off its served POD's status
	// (desiredPhase: Running, or the Initializing reason the vnode sets), so this window
	// contains the bind and overlaps placement rather than following it.
	provision []time.Duration
	// podNames is every Pod this batch was OBSERVED to have, which the teardown poll needs:
	// deletion is fast at the head of the queue, so a Pod (and its claim) can be gone before
	// the drain's first list, and an object discovered only by that list is invisible to it.
	// Seeded from here, the drain knows the whole batch up front and counts against it.
	//
	// Observed, never assumed to be n: if the sync gave up early this is short, and the
	// teardown then reports against what provably existed rather than against a replica count
	// nothing confirmed.
	podNames []string
	// stalled distinguishes the two ways of giving up: nothing advanced for
	// perfStallTimeout (wedged), versus the budget running out while still making
	// progress (merely slow). Only the failure message differs, but that is the
	// difference between "go find the stuck queue" and "the numbers regressed".
	stalled bool
}

// watchBatchSync polls until every replica has synced, nothing advances for
// perfStallTimeout, or the budget runs out — in that order of preference, since the
// stall is the diagnosis and the budget is only the backstop.
//
// Two list calls per poll, whatever N is, so the measurement's own cost does not grow
// with the batch and inflate what it is measuring.
func watchBatchSync(n int, start time.Time, budget time.Duration) syncSamples {
	createdSeen := map[string]time.Duration{}
	boundSeen := map[string]time.Duration{}
	readySeen := map[string]time.Duration{}
	claimSeen := map[string]time.Duration{}
	// When each claim first EXISTED, whatever phase. Not a stage of its own, just the left
	// edge of the per-claim provisioning window.
	claimCreatedSeen := map[string]time.Duration{}
	claimPrefix := perfWorkloadNS + "-" + perfDeployName

	deadline := start.Add(budget)
	nextLog := start.Add(10 * time.Second)

	// Progress is the sum of the three stage counts, which only ever rises (first-seen
	// stamps are never dropped), so "did anything at all happen" is one comparison.
	progress := 0
	lastProgress := start
	stalled := false

	for {
		// util.ClaimName(namespace, name) == "<namespace>-<name>", so this batch's
		// claims are the ones under that prefix.
		out, err := utils.RunQuiet(exec.Command("kubectl", "get", "nodeclaims",
			"-o", "go-template={{range .items}}{{.metadata.name}} {{.status.phase}}{{\"\\n\"}}{{end}}"))
		if err == nil {
			at := time.Since(start)
			for name, phase := range batchRows(out, claimPrefix) {
				// Every row, whatever its phase: a claim is listed from the instant it exists
				// (phase empty for the first tick, rendered "-"), and that is the window's start.
				firstSeen(claimCreatedSeen, name, at)
				if phase == "Bound" {
					firstSeen(claimSeen, name, at)
				}
			}
		}

		// Node name and readiness in ONE token, joined by a separator, so this stays two
		// list calls per poll and batchRows keeps its two-field contract (a third field
		// would be dropped by its count check, and its <no value> collapse relies on the
		// same rule). Both facts then also come off the same object at the same instant.
		out, err = utils.RunQuiet(exec.Command("kubectl", "get", "pods", "-n", perfWorkloadNS,
			"-o", "go-template={{range .items}}{{.metadata.name}} {{.spec.nodeName}}|"+
				"{{range .status.conditions}}{{if eq .type \"Ready\"}}{{.status}}{{end}}{{end}}"+
				"{{\"\\n\"}}{{end}}"))
		if err == nil {
			at := time.Since(start)
			for name, val := range batchRows(out, perfDeployName) {
				node, ready := podRow(val)
				firstSeen(createdSeen, name, at)
				if node == fakeVirtualNode {
					firstSeen(boundSeen, name, at)
				}
				// The literal the Ready condition carries when the vnode has observed the
				// instance running (see pkg/vnode setReady). A Pod with no conditions yet
				// renders as empty, which matches nothing.
				if ready == "True" {
					firstSeen(readySeen, name, at)
				}
			}
		}

		if len(createdSeen) >= n && len(claimSeen) >= n && len(boundSeen) >= n && len(readySeen) >= n {
			break
		}
		if got := len(createdSeen) + len(claimSeen) + len(boundSeen) + len(readySeen); got > progress {
			progress, lastProgress = got, time.Now()
		}
		if time.Since(lastProgress) > perfStallTimeout {
			stalled = true
			break
		}
		if time.Now().After(deadline) {
			break
		}
		if time.Now().After(nextLog) {
			// Naming the node rather than saying "bound" twice: the NodeClaim's phase and
			// the Pod's .spec.nodeName are different objects reaching different states, and
			// one line carrying "bound" for both reads as a single stage counted twice.
			_, _ = fmt.Fprintf(GinkgoWriter,
				"  t=%s  pods created %d/%d  claims Bound %d/%d  pods on %s %d/%d  pods Ready %d/%d\n",
				time.Since(start).Round(time.Second), len(createdSeen), n, len(claimSeen), n,
				fakeVirtualNode, len(boundSeen), n, len(readySeen), n)
			nextLog = time.Now().Add(10 * time.Second)
		}
		time.Sleep(perfPollInterval)
	}

	// Only objects observed at BOTH ends contribute: one still short of its end has no window
	// yet, and counting it as zero would flatter the result. So on a run that gave up these
	// cover only the subset that finished, which is why the spread rows show no count.
	placement := make([]time.Duration, 0, len(boundSeen))
	for name, at := range boundSeen {
		if c, ok := createdSeen[name]; ok {
			placement = append(placement, at-c)
		}
	}
	provision := make([]time.Duration, 0, len(claimSeen))
	for name, at := range claimSeen {
		if c, ok := claimCreatedSeen[name]; ok {
			provision = append(provision, at-c)
		}
	}

	names := make([]string, 0, len(createdSeen))
	for name := range createdSeen {
		names = append(names, name)
	}

	return syncSamples{
		created:   ascending(createdSeen),
		claims:    ascending(claimSeen),
		bound:     ascending(boundSeen),
		ready:     ascending(readySeen),
		placement: sortDurations(placement),
		provision: sortDurations(provision),
		podNames:  names,
		stalled:   stalled,
	}
}

// drainSamples is what the teardown poll observed.
type drainSamples struct {
	// gone is one latency per claim that DISAPPEARED, measured from the delete, ascending,
	// counted against known — the batch the sync watch observed, seeded in up front, plus
	// anything the drain poll discovered on its own.
	//
	// A claim already gone at the first poll is stamped at that poll rather than dropped:
	// the fact is "gone by T", an upper bound in the same direction every other sample here
	// leans, and dropping it instead made the count read 496/496 while 500 claims provably
	// existed — a blind spot that looked like a smaller batch.
	gone  []time.Duration
	known int
	// podsGone is the same measurement for this batch's Pods, on the same clock. It is what
	// says how much of the teardown is Kubernetes' own: the Pod waits on graceful termination
	// through the virtual kubelet, and the claim cannot go until that finishes, so this curve
	// is the floor under the one above. Do not read the ORDER off the two curves: their
	// percentiles are over different objects, so a claim at p50 and a Pod at p50 are not the
	// same workload.
	//
	// Counted against podsKnown, seeded and stamped exactly as gone is.
	podsGone  []time.Duration
	podsKnown int
	// remaining is how many of this batch's claims were still present when the poll
	// gave up; 0 means drained.
	remaining int
	// stalled means the count stopped falling (a finalizer that will never run) as
	// opposed to the budget expiring while it was still falling.
	stalled bool
}

// drainPerfClaims polls until none of this batch's NodeClaims are left. Like the watch
// above it fails fast on a stall — a count falling steadily is progress however slow,
// while a count that stops falling is wedged.
//
// ONE list call per poll, covering Pods and claims together, unlike the sync watch. A claim
// follows the Pod it serves in tens of milliseconds (see NodeClaimReconciler.Reconcile — it
// self-deletes once the served Pod reads absent), which is smaller than the gap between two
// kubectl invocations, so two calls put the two curves on measurably different clocks and made
// their order unreadable. One snapshot stamps a Pod and its claim from the same instant.
//
// podNames is the batch the sync watch observed (syncSamples.podNames). Both ledgers are
// seeded from it so the counts are against the batch that provably existed, not against
// whatever survived long enough for the first list to catch it — deletion is quickest at the
// head of the queue, so the objects most likely to be missed are the fastest ones.
//
// Deliberately separate from waitForNodeClaimsGone: that one is shared with AfterAll,
// where a plain cluster-wide poll with no stall logic is the right thing, and it must
// keep working even when the CRD is already gone.
func drainPerfClaims(budget time.Duration, podNames []string) drainSamples {
	claimPrefix := perfWorkloadNS + "-" + perfDeployName
	start := time.Now()
	deadline := start.Add(budget)

	claimsKnown := map[string]struct{}{}
	claimsGoneSeen := map[string]time.Duration{}
	podsKnown := map[string]struct{}{}
	podsGoneSeen := map[string]time.Duration{}
	// Seeding only asserts these objects EXISTED. The Pods were observed directly by the sync
	// watch, and the claims are derived from them — sound because the spec asserts all N
	// claims reached Bound before the delete, so it never gets here with a claim that was
	// never real. It does not assert they are still here: one already gone is stamped by the
	// first poll that fails to see it, the same rule every other name follows.
	for _, pod := range podNames {
		podsKnown[pod] = struct{}{}
		claimsKnown[nebulautil.ClaimName(perfWorkloadNS, pod)] = struct{}{}
	}
	remaining := -1 // no observation yet, so the first one always counts as progress
	lastProgress := start

	// observeBatch stamps whichever of this batch's Pods and claims have gone, both from ONE
	// list call and against ONE clock reading, and returns how many claims are left. The Pods
	// do not gate the loop: only the claim count decides drained, stalled, or out of budget,
	// because that is what the spec asserts on — hence the second return, which reports
	// whether the list itself succeeded. A list that errors is skipped rather than read as
	// "they are all gone": a transient failure would otherwise stamp the whole batch at once.
	//
	// Cluster-scoped nodeclaims ignore -n, so one call covers both kinds.
	observeBatch := func() (int, bool) {
		out, err := utils.RunQuiet(exec.Command("kubectl", "get", "pods,nodeclaims", "-n", perfWorkloadNS,
			"-o", "go-template={{range .items}}{{.metadata.name}} {{.status.phase}}{{\"\\n\"}}{{end}}"))
		if err != nil {
			return 0, false
		}
		at := time.Since(start)
		observeGone(podsKnown, podsGoneSeen, batchRows(out, perfDeployName), at)
		claims := batchRows(out, claimPrefix)
		observeGone(claimsKnown, claimsGoneSeen, claims, at)
		return len(claims), true
	}

	result := func(remaining int, stalled bool) drainSamples {
		return drainSamples{
			gone:      ascending(claimsGoneSeen),
			known:     len(claimsKnown),
			podsGone:  ascending(podsGoneSeen),
			podsKnown: len(podsKnown),
			remaining: remaining,
			stalled:   stalled,
		}
	}

	for {
		left, ok := observeBatch()
		// A failed list says nothing about what is left, so it is neither progress nor a stall
		// by itself: keep polling, and let the stall timeout below catch a failure that persists.
		if ok {
			if remaining < 0 || left < remaining {
				remaining, lastProgress = left, time.Now()
			}
			if remaining == 0 {
				return result(0, false)
			}
		}
		if time.Since(lastProgress) > perfStallTimeout {
			return result(remaining, true)
		}
		if time.Now().After(deadline) {
			return result(remaining, false)
		}
		time.Sleep(perfPollInterval)
	}
}

// batchRows parses "<name> <value>" lines, keeping those whose name carries prefix.
//
// The placeholder is collapsed FIRST, and that one substitution is load-bearing:
// go-template renders an unset field as "<no value>", which is TWO space-separated tokens,
// so such a row has three fields and the count check below silently DROPS it. That is not
// "the value never matches" — an unscheduled Pod has no .spec.nodeName, so it was missing
// from the parse entirely and the "created" stage could not stamp it until the scheduler
// had already bound it, which is why creation and binding used to report identical
// percentiles. One token keeps the row, with a value no caller looks for.
func batchRows(out, prefix string) map[string]string {
	rows := map[string]string{}
	for _, line := range utils.GetNonEmptyLines(strings.ReplaceAll(out, "<no value>", "-")) {
		fields := strings.Fields(line)
		if len(fields) != 2 || !strings.HasPrefix(fields[0], prefix) {
			continue
		}
		rows[fields[0]] = fields[1]
	}
	return rows
}

// podRow splits the composite value the Pods poll asks for into the node the Pod is
// bound to and its Ready condition. Either half can be empty — an unscheduled Pod has
// no nodeName (rendered "-", see batchRows) and a fresh one has no conditions.
func podRow(v string) (node, ready string) {
	node, ready, _ = strings.Cut(v, "|")
	return node, ready
}

// firstSeen keeps the earliest observation of a name; later polls are ignored.
func firstSeen(seen map[string]time.Duration, name string, at time.Duration) {
	if _, dup := seen[name]; !dup {
		seen[name] = at
	}
}

// observeGone records what is present and stamps at against every name known from an
// earlier poll but absent from this one — that name's teardown is done. Both maps are
// updated in place; known only ever grows, so an object that reappears (it cannot, once
// deleted) would keep its first disappearance.
func observeGone(
	known map[string]struct{}, goneSeen map[string]time.Duration,
	present map[string]string, at time.Duration,
) {
	for name := range present {
		known[name] = struct{}{}
	}
	for name := range known {
		if _, still := present[name]; !still {
			firstSeen(goneSeen, name, at)
		}
	}
}

func ascending(seen map[string]time.Duration) []time.Duration {
	out := make([]time.Duration, 0, len(seen))
	for _, d := range seen {
		out = append(out, d)
	}
	return sortDurations(out)
}

func sortDurations(ds []time.Duration) []time.Duration {
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return ds
}

// reportBatchSync writes the numbers to the Ginkgo output. Reporting is most of this
// spec's value: the assertion only catches a stall, while the table is what shows a
// path getting slower.
func reportBatchSync(n int, s syncSamples) {
	_, _ = fmt.Fprintf(GinkgoWriter,
		"\nworkload sync benchmark: Deployment %s, %d replicas (NEBULA_E2E_PERF_WORKLOADS)\n",
		perfDeployName, n)
	// Not "by the ReplicaSet": that is only true while the batch is a Deployment, and the
	// stage means the same thing for any workload shape.
	// Pods first, then the claim ledger, and each group's derived per-object row last: it is
	// computed from the cumulative ones above it, and unlike them it does not move with how
	// fast the replicas were created. Ready ends the Pod group because it is the one the
	// others are only a step toward — this is when the workloads were usable.
	_, _ = fmt.Fprintf(GinkgoWriter, "  Pods created                         %s\n", stageLine(s.created, n))
	_, _ = fmt.Fprintf(GinkgoWriter, "  Pods bound to %-22s %s\n", fakeVirtualNode, stageLine(s.bound, n))
	_, _ = fmt.Fprintf(GinkgoWriter, "  Pods Ready                           %s\n", stageLine(s.ready, n))
	_, _ = fmt.Fprintf(GinkgoWriter, "  placement (Pod created → bound)      %s\n", spreadLine(s.placement))
	_, _ = fmt.Fprintf(GinkgoWriter, "  NodeClaims Bound                     %s\n", stageLine(s.claims, n))
	_, _ = fmt.Fprintf(GinkgoWriter, "  provisioning (NC created → Bound)    %s\n", spreadLine(s.provision))
	if len(s.bound) == n && s.bound[n-1] > 0 {
		_, _ = fmt.Fprintf(GinkgoWriter, "  throughput                           %.1f workloads/s\n",
			float64(n)/s.bound[n-1].Seconds())
	}
	_, _ = fmt.Fprintf(GinkgoWriter,
		"  (samples quantized by the %s poll interval plus the two list calls per poll)\n", perfPollInterval)
}

// reportDrain prints teardown with the same spread the sync stages get. The wall clock
// alone cannot separate a steady trickle (a throughput ceiling) from most claims going
// fast plus one finalizer hanging — same total, different bug.
func reportDrain(total, drainTotal time.Duration, d drainSamples) {
	_, _ = fmt.Fprintf(GinkgoWriter, "  teardown (delete → all claims gone)  %s\n", fmtDuration(drainTotal))
	// Both against known, not n: see drainSamples.gone.
	_, _ = fmt.Fprintf(GinkgoWriter, "  Pods gone                            %s\n",
		stageLine(d.podsGone, d.podsKnown))
	_, _ = fmt.Fprintf(GinkgoWriter, "  NodeClaims gone                      %s\n",
		stageLine(d.gone, d.known))
	if len(d.gone) > 0 && drainTotal > 0 {
		_, _ = fmt.Fprintf(GinkgoWriter, "  drain rate                           %.1f claims/s\n",
			float64(len(d.gone))/drainTotal.Seconds())
	}
	// Measured, not summed: see the note on total in benchmarkWorkloadSync.
	_, _ = fmt.Fprintf(GinkgoWriter, "  total (apply → all claims gone)      %s\n", fmtDuration(total))
}

// stageLine formats one stage: how many got there, and the spread of when.
func stageLine(sorted []time.Duration, n int) string {
	if len(sorted) == 0 {
		return fmt.Sprintf("0/%d", n)
	}
	return fmt.Sprintf("%d/%d  %s", len(sorted), n, spreadLine(sorted))
}

func spreadLine(sorted []time.Duration) string {
	if len(sorted) == 0 {
		return "no samples"
	}
	// Every sample landed inside one poll. Printing "p50 0s" would claim a precision
	// this loop does not have: all it knows is that the stage finished faster than it
	// can look, which at 500 replicas is a list call, not the interval.
	if sorted[len(sorted)-1] == 0 {
		return "under one poll (too fast to resolve)"
	}
	// fmtDuration, the same formatter the HTML report uses, so the two never print the same
	// number two ways: Go's own String() trims trailing zeros, which is what made 10.58s and
	// 10.717s sit in one column at different precisions.
	return fmt.Sprintf("p50 %s  p95 %s  max %s",
		fmtDuration(percentile(sorted, 50)),
		fmtDuration(percentile(sorted, 95)),
		fmtDuration(lastSample(sorted)))
}

// percentile is nearest-rank on an ascending slice: index ceil(p/100 * len) - 1, so
// p95 of 20 samples is the 19th. No interpolation — against a poll interval this
// coarse it would only invent precision.
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := (p*len(sorted) + 99) / 100
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// perfManifest is the pool plus one Deployment of n replicas. The Pod template carries
// the same labels and shape as the placement spec's Pod, so the two specs measure the
// same path at different widths.
func perfManifest(n int) string {
	return fmt.Sprintf(`apiVersion: nebula.inftyai.com/v1alpha1
kind: NodePool
metadata:
  name: %[1]s
spec:
  providers:
  - name: %[2]s
  capacityTypes:
  - OnDemand
  strategy: Ordered
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[3]s
  namespace: %[4]s
spec:
  replicas: %[5]d
  selector:
    matchLabels:
      app: %[3]s
  template:
    metadata:
      labels:
        app: %[3]s
        nebula.inftyai.com/enabled: "true"
        nebula.inftyai.com/nodepool: %[1]s
    spec:
      securityContext:
        runAsNonRoot: true
        seccompProfile:
          type: RuntimeDefault
      containers:
      - name: main
        image: registry.k8s.io/pause:3.10
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop:
            - ALL
`, perfPoolName, fakeProviderName, perfDeployName, perfWorkloadNS, n)
}
