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

package aws

import (
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"

	smithy "github.com/aws/smithy-go"

	"github.com/InftyAI/Nebula/pkg/provider"
)

// These are the pure (AWS-SDK-free) translations the SDK client relies on:
// building the cloud-init user-data that starts the container, and mapping raw
// EC2 API errors onto the shared provider sentinels. They live apart from
// client.go so they can be unit-tested without any AWS calls.

// buildUserData renders the base64 cloud-init user-data that boots the workload
// container on a GPU AMI. The chosen execution model (see the package doc) is a
// prebuilt GPU AMI — NVIDIA driver + a container runtime already installed — plus
// this user-data that pulls spec.Image and runs it with the Pod's command/env and
// all GPUs attached. The script is intentionally minimal and idempotent-ish: it
// is the seam where a richer bootstrap (systemd unit, log shipping, health
// reporting) would later grow.
//
// spec.Image is required; a spec with no image is a programmer error (the adapter
// reads it off the Pod's first container) and yields an error rather than a
// container-less instance.
func buildUserData(spec InstanceSpec) (string, error) {
	if spec.Image == "" {
		return "", errors.New("aws: empty image in instance spec")
	}

	var b strings.Builder
	b.WriteString("#!/bin/bash\n")
	b.WriteString("set -euo pipefail\n")

	// SandD daemon (opt-in): install and start it on the HOST, BEFORE the workload,
	// so commands and interactive shells can be run on the box while the container
	// runs. It goes first because the workload's `docker run` is a foreground process
	// the script blocks on until it exits (that is by design — cloud-init "finishes"
	// when the workload does); a daemon started after it would only come up once the
	// workload was already gone. See writeSanddBootstrap for the fail-open guarantee.
	if spec.Sandd.Enabled() {
		writeSanddBootstrap(&b, spec.Sandd, spec.Tags[ClaimTagKey])
	}

	// Pull first so an image error surfaces before we try to run.
	fmt.Fprintf(&b, "docker pull %s\n", shellQuote(spec.Image))

	// docker run --rm --gpus all, with env, then the image, then the workload's
	// command/args. Kubernetes container semantics are preserved by mapping the two
	// Pod fields the way the kubelet does, NOT by concatenating them:
	//   - Command (Pod command) overrides the image ENTRYPOINT => Docker --entrypoint,
	//     which takes a single program; Command[0] is the entrypoint and Command[1:]
	//     become leading arguments (Docker only lets --entrypoint carry the program).
	//   - Args (Pod args) are appended after as CMD arguments.
	// When Command is empty the image's own ENTRYPOINT runs; when Args is empty the
	// image's own CMD runs — so an image with a baked-in entrypoint launches exactly
	// as it would under a kubelet. Env keys are sorted so the rendered script is
	// deterministic (stable across reconciles and easy to assert in tests).
	b.WriteString("docker run --rm --gpus all")
	for _, k := range sortedKeys(spec.Env) {
		fmt.Fprintf(&b, " -e %s", shellQuote(k+"="+spec.Env[k]))
	}
	// runArgs are everything that follows the image: the entrypoint's own arguments
	// (Command[1:]) then the CMD arguments (Args), in that order.
	var runArgs []string
	if len(spec.Command) > 0 {
		fmt.Fprintf(&b, " --entrypoint %s", shellQuote(spec.Command[0]))
		runArgs = append(runArgs, spec.Command[1:]...)
	}
	runArgs = append(runArgs, spec.Args...)

	b.WriteString(" " + shellQuote(spec.Image))
	for _, arg := range runArgs {
		b.WriteString(" " + shellQuote(arg))
	}
	b.WriteString("\n")

	return base64.StdEncoding.EncodeToString([]byte(b.String())), nil
}

// sanddInstallURL is the SandD daemon install script. The bootstrap pipes it to
// bash with --tunnel so Tailscale is installed alongside the daemon (see the
// SandD install.sh --tunnel path).
const sanddInstallURL = "https://raw.githubusercontent.com/InftyAI/SandD/main/hack/scripts/install.sh"

// writeSanddBootstrap appends the SandD sandbox-daemon bootstrap to the user-data
// script. daemonID is the NodeClaim name (from ClaimTagKey), so a daemon that dials
// home is identifiable as this exact instance.
//
// Two properties are load-bearing:
//   - FAIL-OPEN: the workload runs under `set -euo pipefail`, and the daemon runs
//     alongside it (it does not run the workload), so a failure to install or start
//     the daemon must never abort the workload. The whole block runs inside a
//     `( set +e ; ... ) || true` subshell so a failed install/curl/tailscale step
//     cannot trip pipefail and kill the instance before `docker run`.
//   - BACKGROUNDED & DETACHED: the daemon is a long-running process, so it is run
//     with `setsid ... &` and its output redirected to a log file. If it blocked in
//     the foreground, cloud-init would never reach `docker run` and the workload
//     would never start.
//
// The auth key is written to a root-only file rather than passed on the command
// line so it does not linger in the process table (`ps`) for any workload the
// container can see.
func writeSanddBootstrap(b *strings.Builder, cfg provider.SanddConfig, daemonID string) {
	b.WriteString("# --- SandD daemon (opt-in; fail-open, never aborts the workload) ---\n")
	b.WriteString("(\n")
	b.WriteString("  set +e\n")
	// Install the daemon + Tailscale. Piped to bash; the subshell's `set +e` means a
	// non-zero exit here is swallowed rather than tripping the outer pipefail.
	fmt.Fprintf(b, "  curl -fsSL %s | bash -s -- --tunnel\n", shellQuote(sanddInstallURL))
	// Keep the auth key off the command line: stash it in a root-only file the daemon
	// reads via env, so it is not visible in `ps`.
	b.WriteString("  install -m 600 /dev/null /etc/sandd.authkey\n")
	fmt.Fprintf(b, "  printf '%%s' %s > /etc/sandd.authkey\n", shellQuote(cfg.AuthKey))
	// Launch detached & backgrounded so cloud-init proceeds to the workload. Output
	// goes to a log for post-hoc debugging via the daemon itself.
	b.WriteString("  setsid sandd \\\n")
	fmt.Fprintf(b, "    --server-url %s \\\n", shellQuote(cfg.ServerURL))
	fmt.Fprintf(b, "    --daemon-id %s \\\n", shellQuote(daemonID))
	b.WriteString("    --tunnel \\\n")
	b.WriteString("    --tunnel-authkey \"$(cat /etc/sandd.authkey)\" \\\n")
	fmt.Fprintf(b, "    --tunnel-server %s \\\n", shellQuote(cfg.ControlServer))
	b.WriteString("    >/var/log/sandd.log 2>&1 &\n")
	b.WriteString(") || true\n")
}

// sortedKeys returns m's keys in sorted order, for deterministic rendering.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// shellQuote wraps s in single quotes, escaping any embedded single quote, so an
// image/env/command value with spaces or shell metacharacters cannot break out of
// its argument or inject commands into the bootstrap script.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// errZoneLocal is an INTERNAL marker wrapped onto a capacity failure that is
// specific to the availability zone of the attempted subnet, not the whole region
// — a sibling AZ's default subnet may still satisfy the launch. RunInstance reads
// it to decide whether to keep sweeping subnets (zone-local) or stop immediately
// and bubble a region-scoped block up to control-plane failover. It always rides
// ALONGSIDE provider.ErrNoCapacity, so the control plane's ClassifyProvisionError
// derives the same BlockScope regardless of scope; only the adapter's inner sweep
// distinguishes the two.
var errZoneLocal = errors.New("aws: zone-local capacity shortfall")

// apiErrorFromFleet reconstructs a smithy.APIError from a CreateFleet per-attempt
// error so an instant fleet's failure (reported in out.Errors, not as a returned Go
// error) can be run through classifyEC2Error alongside the RunInstances-style
// errors. The fleet reuses the same EC2 error codes (InsufficientInstanceCapacity,
// SpotMaxPriceTooLow, ...), so the same classifier applies once the code is wrapped
// back into the smithy.APIError shape it reads. AWS's human-readable message is
// preserved (falling back to the code when absent) so the surfaced error names the
// ACTUAL reason — e.g. "g6.48xlarge is not supported in us-east-1e" — instead of
// echoing the bare code twice.
func apiErrorFromFleet(code, message string) error {
	if message == "" {
		message = code
	}
	return &smithy.GenericAPIError{Code: code, Message: message}
}

// classifyEC2Error maps a raw EC2 API error onto the shared provider sentinels so
// the adapter's ClassifyProvisionError can derive a BlockScope uniformly. It
// recognizes EC2's error codes (smithy.APIError) first, falling back to the
// original error otherwise. spot indicates whether the failing request was Spot,
// so a capacity failure can additionally carry ErrSpotCapacity and confine the
// block to the Spot tier.
//
// Capacity failures split by SCOPE, which drives the adapter's per-zone sweep:
//   - ZONE-LOCAL (InsufficientInstanceCapacity/InsufficientHostCapacity, and
//     "Unsupported" — how EC2 reports the instance type is not offered in the AZ of
//     the chosen subnet): a sibling AZ may still satisfy the launch, so these carry
//     errZoneLocal and RunInstance advances to the next subnet. If every AZ fails
//     it collapses to a region no-capacity that hands off to region-level failover.
//   - REGION/ACCOUNT-SCOPED (SpotMaxPriceTooLow, MaxSpotInstanceCountExceeded): a
//     Spot price ceiling or per-region Spot limit that applies across the whole
//     region, not one AZ. Sweeping sibling AZs is futile, so these OMIT errZoneLocal
//     — RunInstance stops immediately and lets region/tier failover take over. They
//     still carry ErrSpotCapacity (they are Spot-only), confining the block to Spot.
//
// Returning the wrapped sentinel (not a BlockScope) keeps the
// error-category → scope rule in one place (provider.ClassifyError, via the
// adapter), so this function only has to recognize AWS's vocabulary.
func classifyEC2Error(err error, spot bool) error {
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return err // not an API error; let string heuristics in ClassifyError handle it
	}

	switch apiErr.ErrorCode() {
	case "InsufficientInstanceCapacity", "InsufficientHostCapacity", "Unsupported",
		// InvalidFleetConfiguration, in a CreateFleet per-override error, is how EC2
		// reports "this instance type is not offered in the AZ of this subnet" (e.g.
		// g6.48xlarge in us-east-1e) — an availability gap in ONE zone, exactly like
		// Unsupported. A sibling AZ may still satisfy the launch, so it is zone-local
		// no-capacity, not a malformed request: treat it as capacity so region/tier
		// failover routes around it rather than surfacing an opaque config error.
		"InvalidFleetConfiguration":
		return wrapNoCapacity(err, spot, true /* zoneLocal */)
	case "SpotMaxPriceTooLow", "MaxSpotInstanceCountExceeded":
		// Region/account-scoped Spot limits: NOT zone-local, so the sweep stops.
		// These only arise on Spot requests, so ErrSpotCapacity always applies.
		return wrapNoCapacity(err, true /* spot */, false /* zoneLocal */)
	case "InstanceLimitExceeded", "VcpuLimitExceeded", "RequestLimitExceeded":
		return fmt.Errorf("%w: %w", err, provider.ErrQuota)
	case "UnauthorizedOperation", "AuthFailure", "Blocked",
		"OptInRequired", "PendingVerification":
		return fmt.Errorf("%w: %w", err, provider.ErrAuth)
	default:
		return err
	}
}

// wrapNoCapacity wraps err with provider.ErrNoCapacity plus, conditionally, the
// Spot-tier marker (ErrSpotCapacity) and the zone-local sweep marker
// (errZoneLocal). It builds a single multi-%w chain (Go 1.20+) rather than
// errors.Join: errors.Is still finds every marker, but the rendered message stays
// on ONE line (": "-separated) instead of errors.Join's one-error-per-line form,
// which is far more readable when the chain is surfaced as a Pod Event / log line.
func wrapNoCapacity(err error, spot, zoneLocal bool) error {
	errs := []error{err, provider.ErrNoCapacity}
	if spot {
		errs = append(errs, ErrSpotCapacity)
	}
	if zoneLocal {
		errs = append(errs, errZoneLocal)
	}
	// fmt.Errorf with N "%w" verbs wraps every error (so errors.Is/As unwrap them
	// all) while joining their messages with ": " on a single line.
	format := strings.TrimPrefix(strings.Repeat(": %w", len(errs)), ": ")
	args := make([]any, len(errs))
	for i, e := range errs {
		args[i] = e
	}
	return fmt.Errorf(format, args...)
}
