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

	// Pull first so an image error surfaces before we try to run.
	fmt.Fprintf(&b, "docker pull %s\n", shellQuote(spec.Image))

	// SandD (opt-in): fetch the daemon + Tailscale binaries on the HOST now, before
	// the container starts. They are bind-mounted read-only into the container by
	// writeSanddEntrypoint, so the user's image needs no fetcher. Fail-open: a failed
	// download just leaves the mount empty and the shim skips the daemon.
	if spec.Sandd.Enabled() {
		fmt.Fprintf(&b, sanddHostFetchScript, sanddHostDir, sanddBinaryURL, tailscaleTarballURL)
	}

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

	// SandD daemon (opt-in): run it INSIDE the container so its shells/commands see
	// the user's own env, cwd, filesystem and code (a host-side daemon would only see
	// the host). We cannot assume the user's image has sandd, so a shell entrypoint
	// shim fetches the static musl binary at boot, starts it backgrounded, then execs
	// the user's real program — see writeSanddEntrypoint. This overrides the image's
	// ENTRYPOINT with the shim, so the shim (not Docker) is responsible for launching
	// the workload with the right Command/Args; runArgs is left empty in that case.
	var runArgs []string
	if spec.Sandd.Enabled() {
		runArgs = writeSanddEntrypoint(&b, spec)
	} else {
		// No shim: map Command/Args straight onto Docker's --entrypoint/CMD as before.
		// runArgs are everything that follows the image: the entrypoint's own arguments
		// (Command[1:]) then the CMD arguments (Args), in that order.
		if len(spec.Command) > 0 {
			fmt.Fprintf(&b, " --entrypoint %s", shellQuote(spec.Command[0]))
			runArgs = append(runArgs, spec.Command[1:]...)
		}
		runArgs = append(runArgs, spec.Args...)
	}

	b.WriteString(" " + shellQuote(spec.Image))
	for _, arg := range runArgs {
		b.WriteString(" " + shellQuote(arg))
	}
	b.WriteString("\n")

	return base64.StdEncoding.EncodeToString([]byte(b.String())), nil
}

// sanddBinaryURL is the statically-linked (musl) SandD daemon release asset. Being
// static, this one binary runs in any container image regardless of its libc, so the
// shim can fetch it into an arbitrary user image at boot. It is pinned to the same
// asset name install.sh resolves; amd64 matches the x86_64 GPU instance types.
const sanddBinaryURL = "https://github.com/InftyAI/SandD/releases/latest/download/sandd-linux-amd64"

// tailscaleTarballURL is the static (no-libc) Tailscale bundle. sandd --tunnel does
// NOT install Tailscale — it requires `tailscale`/`tailscaled` already on PATH (see
// setup_tunnel in sandd) — so the shim must fetch them too. The static build runs in
// any image, and tailscaled's userspace-networking mode (below) needs no TUN device
// or NET_ADMIN, so this works in an unprivileged container. Pinned: unlike sandd, a
// tarball has no /latest/ redirect, and pinning the mesh client is prudent anyway.
const tailscaleTarballURL = "https://pkgs.tailscale.com/stable/tailscale_1.78.1_amd64.tgz"

// sanddHostDir is where the host fetches the sandd + tailscale binaries and where
// they are bind-mounted (read-only) into the workload container. Fetching on the
// HOST — the AL2 GPU AMI, which has curl+tar — instead of inside the container means
// the user's image needs no fetcher or package manager (works on distroless too),
// and the download happens once per instance rather than per container start.
const sanddHostDir = "/opt/sandd"

// sanddHostFetchScript downloads the static sandd + Tailscale binaries into
// sanddHostDir on the HOST, as a fragment of the user-data run BEFORE `docker run`.
// It is a fmt format string: %[1]s=dir, %[2]s=sandd URL, %[3]s=tailscale URL (all
// trusted package constants, so no shell-injection concern).
//
// FAIL-OPEN: wrapped in `( set +e ... ) || true` so a failed download never aborts
// the (set -euo pipefail) user-data — the container-side shim then simply finds no
// binary and skips the daemon, leaving the workload untouched. Every step logs to
// stderr with a `[sandd]` prefix, which reaches the instance CONSOLE
// (get-console-output) — the place to debug bring-up on a Nebula virtual node, since
// kubectl logs/exec do not work against it.
const sanddHostFetchScript = `( set +e
  log() { echo "[sandd] $*" >&2; }
  mkdir -p %[1]s
  if curl -fsSL %[2]s -o %[1]s/sandd; then chmod +x %[1]s/sandd; log "fetched sandd binary"
  else log "FAILED to fetch sandd binary from %[2]s — SandD disabled"; fi
  if curl -fsSL %[3]s -o %[1]s/ts.tgz && tar -xzf %[1]s/ts.tgz -C %[1]s --strip-components=1; then
    log "fetched tailscale bundle"
  else log "FAILED to fetch/extract tailscale bundle from %[3]s — SandD disabled"; fi
) || true
`

// sanddShimScript is the container ENTRYPOINT shim (run as `/bin/sh -c <script>`). It
// puts the HOST-fetched, bind-mounted binaries (sanddHostDir, mounted :ro) on PATH,
// starts sandd backgrounded in tunnel mode (which brings up tailscaled in
// userspace-networking mode and joins the mesh), then execs the user's real program
// so the daemon shares the WORKLOAD's namespace — its shells and commands see the
// user's env, cwd, filesystem and code. It is a fmt format string: %[1]s=sanddHostDir.
//
// Load-bearing properties:
//   - NO FETCH/INSTALL in the container: binaries come from the read-only bind mount,
//     so the user's image needs only /bin/sh (works on distroless/minimal images too).
//   - FAIL-OPEN: the whole bring-up is a `( set +e ; ... ) || true` subshell, so a
//     missing binary or failed start can never stop the workload from launching.
//   - BACKGROUNDED: sandd is a long-lived process started with `&`; the shim then
//     `exec "$@"` REPLACES the shell with the user's program, so the workload is PID 1
//     (signals/exit codes behave as without the shim) and the shell adds no overhead.
//   - UNPRIVILEGED: tailscaled runs with --tun=userspace-networking, so no /dev/net/tun
//     or NET_ADMIN is required — the container needs no extra capabilities.
//
// Config arrives as container env so no secret is embedded in this script literal.
// Logs to stderr with a `[sandd]` prefix (reaches the console, as above); the daemon's
// OWN output goes to /tmp/sandd.log inside the container (reachable via SandD once the
// mesh is up). /var/lib/tailscale is created in-container (per-container state — never
// shared, unlike the read-only binaries).
const sanddShimScript = `( set +e
  log() { echo "[sandd] $*" >&2; }
  mkdir -p /var/lib/tailscale
  export PATH="%[1]s:$PATH"
  if [ -x "%[1]s/sandd" ] && command -v tailscaled >/dev/null 2>&1; then
    log "starting sandd --tunnel (daemon log: /tmp/sandd.log)"
    "%[1]s/sandd" --tunnel --tunnel-authkey "$SANDD_TUNNEL_AUTHKEY" \
      --tunnel-server "$SANDD_TUNNEL_SERVER" >/tmp/sandd.log 2>&1 &
  else
    log "sandd binary not mounted at %[1]s (host fetch failed?); not starting daemon (workload continues normally)"
  fi
) || true
unset SANDD_TUNNEL_AUTHKEY SANDD_TUNNEL_SERVER SERVER_URL DAEMON_ID
exec "$@"
`

// writeSanddEntrypoint appends the in-container SandD shim to a `docker run` line: it
// emits the `-e` env carrying daemon config, overrides the image ENTRYPOINT with
// `/bin/sh` running sanddShimScript, and returns the post-image argv (the shim's
// `-c` payload plus the user's effective command) for buildUserData to render.
//
// Because the shim commandeers --entrypoint, the image's OWN ENTRYPOINT is bypassed:
// the shim can only relaunch the workload from what Nebula can see, i.e. the Pod's
// Command+Args. A Pod that sets neither (relying on the image's baked-in ENTRYPOINT)
// cannot be faithfully reconstructed here — a known limitation of the in-container
// model; such Pods should set an explicit command.
//
// The auth key is delivered as container env, so it is visible to the workload
// itself (the tenant owns this container). That is inherent to running the daemon
// INSIDE the container — the container must hold the credential to dial home — and is
// acceptable only because the daemon is single-tenant (one per container).
func writeSanddEntrypoint(b *strings.Builder, spec InstanceSpec) []string {
	cfg := spec.Sandd
	// Daemon config as env: SERVER_URL/DAEMON_ID are the names sandd reads natively;
	// the rest the shim references. Rendered via the same shellQuote path as user env
	// so hostile values cannot break out. (Not sorted with user env: these are fixed.)
	for _, kv := range [][2]string{
		{"SERVER_URL", cfg.ServerURL},
		{"DAEMON_ID", spec.Tags[ClaimTagKey]},
		{"SANDD_TUNNEL_AUTHKEY", cfg.AuthKey},
		{"SANDD_TUNNEL_SERVER", cfg.ControlServer},
	} {
		fmt.Fprintf(b, " -e %s", shellQuote(kv[0]+"="+kv[1]))
	}
	// Bind-mount the HOST-fetched binaries read-only (see buildUserData / sanddHostDir).
	// :ro is safe to share across containers — the files are immutable executables; the
	// daemon's writable state lives in the container's own /var/lib/tailscale and /tmp.
	fmt.Fprintf(b, " -v %s", shellQuote(sanddHostDir+":"+sanddHostDir+":ro"))
	fmt.Fprintf(b, " --entrypoint %s", shellQuote("/bin/sh"))

	// `sh -c <script> <arg0> <argv...>`: arg0 ("sandd-shim") becomes $0, and the
	// user's effective argv fills $@, which the shim `exec "$@"`s. Command[0] is the
	// program and Command[1:]+Args its arguments — the same effective argv the kubelet
	// would run, just launched by the shim instead of Docker's --entrypoint/CMD.
	runArgs := []string{"-c", fmt.Sprintf(sanddShimScript, sanddHostDir), "sandd-shim"}
	runArgs = append(runArgs, spec.Command...)
	runArgs = append(runArgs, spec.Args...)
	return runArgs
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
