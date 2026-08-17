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
	"html/template"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/InftyAI/Nebula/test/utils"
)

// The HTML report is deliberately ONE self-contained file: no CDN, no JavaScript, and
// the chart is inline SVG computed here. A perf report gets opened from a laptop with no
// network, mailed around, and attached to CI artifacts, and any of those breaks the
// moment it needs to fetch a charting library.
const (
	// perfReportDir sits under the repo root, which .gitignore already covers
	// ("artifacts"), so reports cannot be committed by accident.
	perfReportDir  = "artifacts"
	perfReportFile = "perf-report.html"
)

// Stage colours are shared by the table swatches and the chart curves, so a row and its
// curve are the same colour and the eye can move between them. Two colours do double duty,
// once per section, so they never share a chart: colourCreated for "Pods creation" and "Pods
// gone" (the Pod object moving at Kubernetes' own pace) and colourSync for the two derived
// per-workload rows, which are the only rows that isolate Nebula's own cost.
const (
	colourCreated = "#94a3b8"
	colourClaims  = "#f59e0b"
	colourBound   = "#3b82f6"
	colourSync    = "#10b981"
	colourGone    = "#ef4444"
)

// perfReportPath is where the report lands, overridable so concurrent runs (or CI
// artifact collection) do not overwrite each other's report.
func perfReportPath() string {
	if p := os.Getenv("NEBULA_E2E_PERF_REPORT"); p != "" {
		return p
	}
	// Falls back to the working directory (test/e2e) if the project root cannot be
	// resolved — a report in an odd place beats losing it.
	if root, err := utils.GetProjectDir(); err == nil {
		return filepath.Join(root, perfReportDir, perfReportFile)
	}
	return perfReportFile
}

// writeHTMLReport renders the run and returns the path, or "" if it could not be
// written. Never fails the spec: losing the report is not a result, and this runs from a
// DeferCleanup where an assertion would mask the real failure.
func writeHTMLReport(n int, s syncSamples, total, drainTotal time.Duration, d drainSamples) string {
	path := perfReportPath()
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "  (could not create the report directory: %v)\n", err)
			return ""
		}
	}

	page, err := renderHTMLReport(n, s, total, drainTotal, d)
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "  (could not render the HTML report: %v)\n", err)
		return ""
	}
	if err := os.WriteFile(path, []byte(page), 0o644); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "  (could not write the HTML report: %v)\n", err)
		return ""
	}

	_, _ = fmt.Fprintf(GinkgoWriter, "\n  HTML report: file://%s\n", path)
	return path
}

// stage is one measured stage: the single source for both its table row and its curve,
// so the two can never disagree about colour, label, or samples.
type stage struct {
	Label   string
	Colour  string
	Note    string
	Samples []time.Duration // ascending
	Total   int             // denominator for the count column; 0 hides it
	Curve   bool            // false for a derived stage with no meaningful timeline
}

// reportRow is one line of a stage table.
type reportRow struct {
	Label  string
	Colour string
	Note   string
	Count  string
	P50    string
	P95    string
	Max    string
	P50Pct float64 // solid part of the spread bar
	MaxPct float64 // translucent extension to max
	Muted  bool    // a stage with nothing to show
}

// reportSeries is one curve: cumulative workloads past a stage over time.
type reportSeries struct {
	Label  string
	Colour string
	Points string // SVG polyline "x,y x,y ..."
	// Dash makes an exactly-covered curve visible without moving or fattening anything.
	// Two stages CAN be identical, point for point — a Pod already bound the first time the
	// poller sees it stamps "created" and "bound" from one timestamp — and the curve drawn
	// first would otherwise vanish completely, which reads as missing data rather than as
	// agreement. So the curve on TOP is dashed and the one underneath shows through the
	// gaps: same width, same path, just interrupted.
	//
	// Drawing the covered curve as a wide translucent band was the previous attempt. These
	// curves are staircases, and a 9px stroke on a near-vertical run protrudes to both
	// sides of the 2.25px line on top of it, which reads as a second line offset sideways —
	// the width itself became a misleading signal. Nudging a curve off its real position was
	// never an option: that would be a lie about the numbers.
	Dash string // stroke-dasharray; empty for a solid curve
	// Same names the curve this one duplicates, for the legend.
	Same string
}

// chart is a fully precomputed plot, so the template does no arithmetic at all.
type chart struct {
	W, H         int
	PadL, PadT   int
	PlotR, PlotB int // right and bottom edges of the plot area
	XLabel       string
	XTicks       []axisTick
	YTicks       []axisTick
	Series       []reportSeries
}

type axisTick struct {
	Pos   float64
	Label string
}

type reportPage struct {
	Replicas   int
	Generated  string
	Verdict    string
	VerdictBad bool
	Cards      []reportCard
	SyncRows   []reportRow
	DrainRows  []reportRow
	SyncChart  *chart
	DrainChart *chart
	Summary    string // the plain-text table, for copy-paste and diffing between runs
	PollNote   string
}

type reportCard struct {
	Key   string
	Value string
	// Note becomes the card's tooltip. A six-card strip has no room for prose, but a
	// two-word key does not say what the number covers — "total" alone does not reveal
	// that cluster setup is outside it — so the definition has to live somewhere.
	Note string
}

func renderHTMLReport(
	n int, s syncSamples, total, drainTotal time.Duration, d drainSamples,
) (string, error) {
	// Measured from the apply. Ordered as the path runs, not by duration.
	syncStages := []stage{{
		Label: "Pods creation", Colour: colourCreated, Samples: s.created, Total: n, Curve: true,
		Note: "Kubernetes' own cost: how fast the Pods are created.",
	}, {
		Label: "Pods bound to " + fakeVirtualNode, Colour: colourBound, Samples: s.bound, Total: n, Curve: true,
		Note: "Ungated and scheduled onto the virtual node — the workload is live.",
	}, {
		Label: "NodeClaims Bound", Colour: colourClaims, Samples: s.claims, Total: n, Curve: true,
		Note: "The claim ledger catching up: placement decided and recorded.",
	}, {
		Label: "Per-Pod sync (created → bound)", Colour: colourSync, Samples: s.sync,
		Note: "Nebula's own contribution, with the creation rate factored out.",
	}}
	// Measured from the delete, so these get their own clock — and their own chart. Pods
	// first because the delete lands on the workload first, not because they finish first:
	// which curve trails is the result, and it has gone both ways.
	drainStages := []stage{{
		Label: "Pods gone", Colour: colourCreated, Samples: d.podsGone, Total: d.podsKnown, Curve: true,
		Note: "Graceful termination through the virtual kubelet.",
	}, {
		Label: "NodeClaims gone", Colour: colourGone, Samples: d.gone, Total: d.known, Curve: true,
		Note: "Self-deleted once the served Pod is gone, then the terminate finalizer releases " +
			"the instance. Both rows count against the batch the sync watch observed; anything " +
			"already gone at the first drain poll is stamped there, an upper bound.",
	}, {
		Label: "Per-claim release (pod → claim gone)", Colour: colourSync, Samples: d.release,
		Total: d.podsKnown,
		Note: "Nebula's own contribution to teardown, with the Pod deletion rate factored out. " +
			"The two curves above cannot show this: their percentiles are over different objects. " +
			"Both stamps come from one snapshot, so most pairs land inside a single poll and read " +
			"as 0. A count below the batch is pairs with no end yet, or a claim seen gone first.",
	}}

	page := reportPage{
		Replicas:  n,
		Generated: time.Now().Format("Mon, 02 Jan 2006 15:04:05 MST"),
		Cards: []reportCard{
			{Key: "total", Value: fmtDuration(total),
				Note: "The whole benchmark: apply until the last NodeClaim was gone, so sync plus " +
					"teardown plus the short gap where the sync numbers are reported and asserted. " +
					"Cluster setup and the manager's deploy are outside it."},
			{Key: "all synced", Value: lastOf(s.bound, n),
				Note: "From the apply until the last Pod was bound to the virtual node. The apply " +
					"itself is not reported: its round trip is the client's cost, not Nebula's."},
			{Key: "teardown", Value: fmtDuration(drainTotal),
				Note: "From the delete until every NodeClaim in the batch was gone."},
			{Key: "replicas", Value: fmt.Sprintf("%d", n),
				Note: "Batch size. Override with NEBULA_E2E_PERF_WORKLOADS."},
			{Key: "sync throughput", Value: rate(len(s.bound), n, lastSample(s.bound), "workloads/s"),
				Note: "Replicas bound per second across that window."},
			{Key: "drain rate", Value: rate(len(d.gone), len(d.gone), drainTotal, "claims/s"),
				Note: "NodeClaims removed per second, finalizers included."},
		},
		SyncRows:  buildRows(syncStages),
		DrainRows: buildRows(drainStages),
		Summary:   plainSummary(n, s, total, drainTotal, d),
		PollNote: fmt.Sprintf(
			"Every sample is attributed to the first %s poll that observed it, so all of them "+
				"overstate slightly; two list calls per sync poll and one per drain poll, whatever "+
				"the batch size.",
			perfPollInterval),
	}

	page.SyncChart = buildChart(syncStages, n, "elapsed since apply")
	// The two drain stages share one y scale, so the Pods curve and the claims curve are
	// read against the same count — that is the whole reason to plot them together.
	page.DrainChart = buildChart(drainStages, max(d.known, d.podsKnown), "elapsed since delete")

	switch {
	case len(s.bound) < n || len(s.claims) < n:
		page.Verdict = fmt.Sprintf("INCOMPLETE — %d/%d pods bound, %d/%d claims Bound%s",
			len(s.bound), n, len(s.claims), n, stalledSuffix(s.stalled))
		page.VerdictBad = true
	case d.remaining > 0:
		page.Verdict = fmt.Sprintf("Synced, but %d claim(s) never drained%s",
			d.remaining, stalledSuffix(d.stalled))
		page.VerdictBad = true
	default:
		page.Verdict = fmt.Sprintf("All %d workloads synced and drained.", n)
	}

	var b strings.Builder
	if err := reportTemplate.Execute(&b, page); err != nil {
		return "", err
	}
	return b.String(), nil
}

func stalledSuffix(stalled bool) string {
	if stalled {
		return fmt.Sprintf(" — stalled: nothing advanced for %s", perfStallTimeout)
	}
	return ""
}

// lastSample is when the final workload cleared a stage, or 0 if none did.
func lastSample(sorted []time.Duration) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[len(sorted)-1]
}

// lastOf reports the whole batch's wall time for a stage, and says so only if the
// stage actually completed — a partial run's last sample is not a completion time.
func lastOf(sorted []time.Duration, n int) string {
	if len(sorted) < n || n == 0 {
		return "—"
	}
	return fmtDuration(lastSample(sorted))
}

func rate(count, want int, over time.Duration, unit string) string {
	if count == 0 || count < want || over <= 0 {
		return "—"
	}
	return fmt.Sprintf("%.1f %s", float64(count)/over.Seconds(), unit)
}

func buildRows(stages []stage) []reportRow {
	// Bars are scaled within a table, never across the two: the sync stages and the
	// drain are measured from different starts, so a shared scale would invite a
	// comparison that means nothing.
	scale := time.Duration(0)
	for _, st := range stages {
		if v := lastSample(st.Samples); v > scale {
			scale = v
		}
	}

	rows := make([]reportRow, 0, len(stages))
	for _, st := range stages {
		row := reportRow{Label: st.Label, Colour: st.Colour, Note: st.Note}
		switch {
		case len(st.Samples) == 0:
			row.Count, row.P50, row.P95, row.Max, row.Muted = "—", "—", "—", "—", true
		case lastSample(st.Samples) == 0:
			// Same guard as the terminal report: all-zero means faster than one poll, and
			// printing 0s would claim precision this measurement does not have.
			row.P50, row.P95, row.Max = "< 1 poll", "< 1 poll", "< 1 poll"
		default:
			row.P50 = fmtDuration(percentile(st.Samples, 50))
			row.P95 = fmtDuration(percentile(st.Samples, 95))
			row.Max = fmtDuration(lastSample(st.Samples))
			if scale > 0 {
				row.P50Pct = percentile(st.Samples, 50).Seconds() / scale.Seconds() * 100
				row.MaxPct = lastSample(st.Samples).Seconds()/scale.Seconds()*100 - row.P50Pct
			}
		}
		if st.Total > 0 {
			row.Count = fmt.Sprintf("%d/%d", len(st.Samples), st.Total)
		} else if len(st.Samples) > 0 {
			row.Count = fmt.Sprintf("%d", len(st.Samples))
		}
		rows = append(rows, row)
	}
	return rows
}

func fmtDuration(d time.Duration) string {
	switch {
	case d == 0:
		return "—"
	case d < time.Second:
		return d.Round(time.Millisecond).String()
	case d < time.Minute:
		return fmt.Sprintf("%.2fs", d.Seconds())
	default:
		return d.Round(100 * time.Millisecond).String()
	}
}

// buildChart turns the samples into cumulative-completion curves, or returns nil if
// there is nothing to draw. Sorted ascending, sample i IS the moment the (i+1)-th
// workload cleared that stage, so no bucketing is needed: steepness is the rate, a flat
// stretch is a stall, and a curve hugging another means that stage is keeping up.
func buildChart(stages []stage, want int, xLabel string) *chart {
	const w, h, padL, padR, padT, padB = 880, 300, 54, 24, 16, 44
	c := &chart{
		W: w, H: h, PadL: padL, PadT: padT,
		PlotR: w - padR, PlotB: h - padB, XLabel: xLabel,
	}

	maxSeconds, maxCount := 0.0, want
	for _, st := range stages {
		if !st.Curve {
			continue
		}
		if v := lastSample(st.Samples).Seconds(); v > maxSeconds {
			maxSeconds = v
		}
		if len(st.Samples) > maxCount {
			maxCount = len(st.Samples)
		}
	}
	if maxCount <= 0 || maxSeconds <= 0 {
		return nil // nothing measurable: no axes worth drawing
	}

	plotW := float64(c.PlotR - c.PadL)
	plotH := float64(c.PlotB - c.PadT)
	x := func(sec float64) float64 { return float64(c.PadL) + sec/maxSeconds*plotW }
	y := func(count int) float64 { return float64(c.PlotB) - float64(count)/float64(maxCount)*plotH }

	for _, st := range stages {
		if !st.Curve || len(st.Samples) == 0 {
			continue
		}
		// Start at (first sample, 0) so a curve that begins late reads as beginning late
		// rather than as rising out of the origin.
		pts := make([]string, 0, len(st.Samples)+1)
		pts = append(pts, fmt.Sprintf("%.1f,%.1f", x(st.Samples[0].Seconds()), y(0)))
		for i, at := range st.Samples {
			pts = append(pts, fmt.Sprintf("%.1f,%.1f", x(at.Seconds()), y(i+1)))
		}
		c.Series = append(c.Series, reportSeries{
			Label: st.Label, Colour: st.Colour, Points: strings.Join(pts, " "),
		})
	}
	if len(c.Series) == 0 {
		return nil
	}

	// Mark exact overlaps: dash the curve on top so the one beneath shows through it, and
	// name the pairing in the legend, so "one line is missing" reads as "these two are the
	// same line". Both curves keep the same width and position — see reportSeries.Dash.
	for i := range c.Series {
		for j := 0; j < i; j++ {
			if c.Series[i].Points == c.Series[j].Points {
				c.Series[i].Dash, c.Series[i].Same = "7 5", c.Series[j].Label
				break
			}
		}
	}

	for i := 0; i <= 4; i++ {
		sec := maxSeconds * float64(i) / 4
		c.XTicks = append(c.XTicks, axisTick{Pos: x(sec), Label: fmt.Sprintf("%.1fs", sec)})
		count := maxCount * i / 4
		c.YTicks = append(c.YTicks, axisTick{Pos: y(count), Label: fmt.Sprintf("%d", count)})
	}
	return c
}

// plainSummary is the same table the terminal prints, embedded so one report can be
// diffed against another run without re-deriving anything.
func plainSummary(n int, s syncSamples, total, drainTotal time.Duration, d drainSamples) string {
	var b strings.Builder
	fmt.Fprintf(&b, "replicas                         %d\n", n)
	fmt.Fprintf(&b, "Pods created                     %s\n", stageLine(s.created, n))
	fmt.Fprintf(&b, "NodeClaims Bound                 %s\n", stageLine(s.claims, n))
	fmt.Fprintf(&b, "Pods bound to %-18s %s\n", fakeVirtualNode, stageLine(s.bound, n))
	fmt.Fprintf(&b, "per-Pod sync (created → bound)   %s\n", spreadLine(s.sync))
	fmt.Fprintf(&b, "teardown total                   %s\n", fmtDuration(drainTotal))
	fmt.Fprintf(&b, "Pods gone                        %s\n", stageLine(d.podsGone, d.podsKnown))
	fmt.Fprintf(&b, "NodeClaims gone                  %s\n", stageLine(d.gone, d.known))
	fmt.Fprintf(&b, "per-claim release (pod → claim)  %s\n", releaseLine(d))
	fmt.Fprintf(&b, "total (apply → drained)          %s\n", fmtDuration(total))
	fmt.Fprintf(&b, "poll interval                    %s\n", perfPollInterval)
	fmt.Fprintf(&b, "stall timeout                    %s\n", perfStallTimeout)
	fmt.Fprintf(&b, "sync budget                      %s\n", perfBudget(n))
	fmt.Fprintf(&b, "teardown budget                  %s\n", perfTeardownBudget(n))
	return b.String()
}

// html/template escapes every interpolation, so nothing computed above can break the
// page even if a label or provider name ever carries markup. Two template blocks are
// shared by the sync and teardown sections, which is what keeps the two honest about
// being the same measurement on different clocks.
var reportTemplate = template.Must(template.New("perf").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Nebula workload sync — {{.Replicas}} replicas</title>
<style>
  :root {
    color-scheme: light dark;
    --fg: #101828; --fg-soft: #344054; --dim: #667085; --faint: #98a2b3;
    --line: #e7eaee; --hair: #f1f3f6; --panel: #fff; --bg: #f7f8fa;
    --ok-bg: #edfcf4; --ok-fg: #05603a; --bad-bg: #fef3f2; --bad-fg: #912018;
    --radius: 12px;
    --shadow: 0 1px 2px rgba(16,24,40,.05), 0 1px 3px rgba(16,24,40,.04);
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --fg: #e9edf3; --fg-soft: #c3cbd8; --dim: #8d99ab; --faint: #6b7789;
      --line: #232c3a; --hair: #1a2230; --panel: #121924; --bg: #0a0e15;
      --ok-bg: #052e20; --ok-fg: #75e0a7; --bad-bg: #2b1316; --bad-fg: #fda29b;
      --shadow: none;
    }
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; padding: 44px 24px 72px; background: var(--bg); color: var(--fg);
    font: 400 13px/1.6 "Inter var", Inter, ui-sans-serif, -apple-system,
          BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", sans-serif;
    font-feature-settings: "cv05" 1, "ss01" 1;
    -webkit-font-smoothing: antialiased; text-rendering: optimizeLegibility;
  }
  main { max-width: 860px; margin: 0 auto; }
  /* Header: title and run stamp on one baseline, so the page opens with a single line
     of context rather than a stacked block. */
  header { display: flex; flex-wrap: wrap; align-items: baseline; justify-content: space-between;
           gap: 6px 24px; margin-bottom: 22px; }
  h1 { font-size: 20px; line-height: 1.3; letter-spacing: -0.018em; margin: 0;
       font-weight: 640; }
  h1 em { font-style: normal; color: var(--dim); font-weight: 500; }
  h2 { display: flex; align-items: center; gap: 12px; font-size: 10px; text-transform: uppercase;
       letter-spacing: .1em; color: var(--faint); font-weight: 620; margin: 40px 0 12px; }
  h2::after { content: ""; flex: 1; height: 1px; background: var(--line); }
  .stamp { color: var(--faint); font-size: 11.5px; font-variant-numeric: tabular-nums;
           white-space: nowrap; }
  .lede { color: var(--dim); font-size: 12.5px; margin: -16px 0 22px; max-width: 66ch; }
  .verdict { display: flex; gap: 10px; align-items: baseline; border-radius: var(--radius);
             padding: 11px 15px; font-size: 12.5px; font-weight: 570; line-height: 1.5;
             background: var(--ok-bg); color: var(--ok-fg); }
  .verdict.bad { background: var(--bad-bg); color: var(--bad-fg); }
  .verdict .dot { width: 7px; height: 7px; border-radius: 50%; background: currentColor;
                  flex: none; transform: translateY(-3px); }
  /* Hairline grid: 1px gaps filled by the container's background, so the cards read as one
     block instead of six floating boxes. */
  .cards { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1px;
           background: var(--line); border: 1px solid var(--line);
           border-radius: var(--radius); overflow: hidden; box-shadow: var(--shadow);
           margin-top: 18px; }
  @media (max-width: 640px) { .cards { grid-template-columns: repeat(2, 1fr); } }
  .card { background: var(--panel); padding: 12px 15px 14px; }
  .card .k { color: var(--faint); font-size: 9.5px; font-weight: 580; text-transform: uppercase;
             letter-spacing: .08em; }
  .card .v { font-size: 17px; font-weight: 600; font-variant-numeric: tabular-nums;
             letter-spacing: -0.02em; line-height: 1.3; margin-top: 2px; }
  .panel { background: var(--panel); border: 1px solid var(--line); border-radius: var(--radius);
           overflow: hidden; box-shadow: var(--shadow); }
  table { border-collapse: collapse; width: 100%; table-layout: fixed; }
  col.c-stage { width: auto; }
  col.c-num { width: 11.5%; }
  col.c-bar { width: 15%; }
  th { font-size: 9.5px; text-transform: uppercase; letter-spacing: .08em; color: var(--faint);
       font-weight: 580; text-align: right; padding: 11px 14px 9px;
       border-bottom: 1px solid var(--line); white-space: nowrap; }
  th:first-child { text-align: left; }
  td { padding: 12px 14px; border-top: 1px solid var(--hair); text-align: right;
       font-size: 12.5px; font-variant-numeric: tabular-nums; letter-spacing: -0.005em;
       white-space: nowrap; vertical-align: top; color: var(--fg-soft); }
  td:first-child { text-align: left; white-space: normal; color: var(--fg); }
  tbody tr:first-child td { border-top: 0; }
  tr.muted td, tr.muted td:first-child { color: var(--faint); }
  .stage { display: flex; gap: 9px; align-items: baseline; font-size: 12.5px; font-weight: 570;
           letter-spacing: -0.003em; }
  .swatch { width: 7px; height: 7px; border-radius: 2px; flex: none; transform: translateY(-1px); }
  .note { color: var(--dim); font-size: 11.5px; font-weight: 400; line-height: 1.5;
          margin: 2px 0 0 16px; max-width: 48ch; }
  .bar .track { height: 5px; border-radius: 3px; background: var(--hair); display: flex;
                overflow: hidden; margin-top: 7px; }
  .bar .track i { display: block; height: 100%; }
  /* The chart shares its panel with the table above it, separated by a hairline: one
     section, one card. */
  .divider { border-top: 1px solid var(--line); }
  .chart { padding: 16px 16px 4px; }
  svg { display: block; width: 100%; height: auto; overflow: visible; }
  .grid { stroke: var(--hair); }
  .axis { stroke: var(--line); }
  /* The SVG is scaled down to the panel width, so tick text set at these sizes lands a
     little smaller again on screen — hence 11 rather than matching the table's 9.5. */
  .tick { fill: var(--faint); font-size: 11px; font-variant-numeric: tabular-nums; }
  .tick.title { fill: var(--dim); font-size: 10px; text-transform: uppercase;
                letter-spacing: .08em; }
  .legend { display: flex; flex-wrap: wrap; gap: 7px 20px; padding: 2px 16px 16px;
            color: var(--dim); font-size: 11.5px; }
  .legend span { display: flex; gap: 7px; align-items: center; }
  .legend em { font-style: normal; color: var(--faint); font-size: 11px; }
  .caption { color: var(--faint); font-size: 11.5px; line-height: 1.55; margin: 10px 2px 0;
             max-width: 80ch; }
  details { margin-top: 40px; }
  summary { cursor: pointer; color: var(--dim); font-size: 11.5px; width: fit-content; }
  summary:hover { color: var(--fg); }
  pre { background: var(--panel); border: 1px solid var(--line); border-radius: var(--radius);
        padding: 16px 18px; overflow-x: auto; font-size: 11px; line-height: 1.65;
        margin-top: 12px; color: var(--fg-soft); box-shadow: var(--shadow); }
  footer { color: var(--faint); font-size: 11.5px; line-height: 1.6; margin-top: 36px;
           padding-top: 18px; border-top: 1px solid var(--line); max-width: 80ch; }
  code { font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, monospace; font-size: .9em; }
  @media print {
    body { background: #fff; padding: 0; }
    .panel, .cards { break-inside: avoid; box-shadow: none; }
    details { display: none; }
  }
</style>
</head>
<body>
<main>
  <header>
    <h1>Workload sync benchmark</h1>
    <span class="stamp">{{.Generated}}</span>
  </header>
  <p class="lede">Fake provider on Kind, so this is control-plane cost only: Pods bind to a
     virtual node and no container is ever started.</p>

  <div class="verdict{{if .VerdictBad}} bad{{end}}"><span class="dot"></span><span>{{.Verdict}}</span></div>

  <div class="cards">
    {{range .Cards}}<div class="card" title="{{.Note}}"><div class="k">{{.Key}}</div><div
      class="v">{{.Value}}</div></div>{{end}}
  </div>

  <h2>Sync &middot; from the apply</h2>
  <div class="panel">
    {{template "stageTable" .SyncRows}}
    {{with .SyncChart}}<div class="divider">{{template "chart" .}}</div>{{end}}
  </div>
  <p class="caption">{{.PollNote}}</p>

  {{if .DrainChart}}
  <h2>Teardown &middot; from the delete</h2>
  <div class="panel">
    {{template "stageTable" .DrainRows}}
    <div class="divider">{{template "chart" .DrainChart}}</div>
  </div>
  {{end}}

  <details>
    <summary>Plain-text summary (for diffing runs)</summary>
    <pre>{{.Summary}}</pre>
  </details>

  <footer>
    Generated by <code>make test-perf</code> (<code>test/e2e/perf_test.go</code>).
    Set <code>NEBULA_E2E_PERF_WORKLOADS</code> to change the batch size and
    <code>NEBULA_E2E_PERF_REPORT</code> to move this file &mdash; two runs sharing the
    default path overwrite each other.
  </footer>
</main>
</body>
</html>

{{define "stageTable"}}
<table>
  <colgroup>
    <col class="c-stage"><col class="c-num"><col class="c-num"><col class="c-num">
    <col class="c-num"><col class="c-bar">
  </colgroup>
  <thead><tr>
    <th>stage</th><th>done</th><th>p50</th><th>p95</th><th>max</th><th>spread</th>
  </tr></thead>
  <tbody>
  {{range .}}
    <tr class="{{if .Muted}}muted{{end}}">
      <td>
        <div class="stage"><span class="swatch" style="background:{{.Colour}}"></span>
          <span>{{.Label}}</span></div>
        <div class="note">{{.Note}}</div>
      </td>
      <td>{{if .Count}}{{.Count}}{{else}}&mdash;{{end}}</td>
      <td>{{.P50}}</td><td>{{.P95}}</td><td>{{.Max}}</td>
      <td class="bar">{{if .MaxPct}}<div class="track"
          title="p50 solid, extending to max"><i style="width:{{printf "%.1f" .P50Pct}}%;background:{{.Colour}}"></i><i
          style="width:{{printf "%.1f" .MaxPct}}%;background:{{.Colour}};opacity:.35"></i></div>{{end}}</td>
    </tr>
  {{end}}
  </tbody>
</table>
{{end}}

{{define "chart"}}
  <div class="chart">
  <svg viewBox="0 0 {{.W}} {{.H}}" role="img"
       aria-label="cumulative workloads completed per stage, {{.XLabel}}">
    {{range .YTicks}}
      <line class="grid" x1="{{$.PadL}}" y1="{{.Pos}}" x2="{{$.PlotR}}" y2="{{.Pos}}"/>
      <text class="tick" x="{{$.PadL}}" y="{{.Pos}}" dx="-9" dy="4" text-anchor="end">{{.Label}}</text>
    {{end}}
    {{range .XTicks}}
      <text class="tick" x="{{.Pos}}" y="{{$.PlotB}}" dy="19" text-anchor="middle">{{.Label}}</text>
    {{end}}
    <line class="axis" x1="{{.PadL}}" y1="{{.PlotB}}" x2="{{.PlotR}}" y2="{{.PlotB}}"/>
    <text class="tick title" x="{{.PlotR}}" y="{{.PlotB}}" dy="34" text-anchor="end">{{.XLabel}}</text>
    <!-- Axis titles sit outside the plot: the y title at the top-left corner, above the
         tick column it labels, and the x title at the end of the x ticks. Right-aligning
         the y title to the ticks would push it off the left edge of the panel. -->
    <text class="tick title" x="0" y="{{.PadT}}" dy="-6">workloads</text>
    <!-- Every curve is stroked identically; only stroke-dasharray ever differs, so a
         difference in weight or position on this chart is always a difference in the
         numbers. Butt caps rather than round: a round cap adds half the stroke width to
         each end of every dash, which eats the gaps the dashes exist to leave. -->
    {{range .Series}}
      <polyline fill="none" stroke="{{.Colour}}" stroke-width="2.25"
                {{if .Dash}}stroke-dasharray="{{.Dash}}" {{end}}stroke-linecap="butt"
                stroke-linejoin="round" points="{{.Points}}"/>
    {{end}}
  </svg>
  </div>
  <div class="legend">
    {{range .Series}}<span><span class="swatch" style="background:{{.Colour}}"></span>{{.Label}}{{if
      .Same}}{{end}}</span>{{end}}
  </div>
{{end}}
`))
