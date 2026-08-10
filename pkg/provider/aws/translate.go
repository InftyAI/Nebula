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
	"path"
	"sort"
	"strings"

	smithy "github.com/aws/smithy-go"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// These are the pure (AWS-SDK-free) translations the SDK client relies on:
// building the cloud-init user-data that starts the container, and mapping raw
// EC2 API errors onto the shared provider sentinels. They live apart from
// client.go so they can be unit-tested without any AWS calls.

// sanddEnvFile is where the bootstrap writes the daemon's credentials for docker
// to read with --env-file. Under /run because it is tmpfs on any modern Linux: the
// token never touches persistent disk, and it is gone after a reboot (by which
// time it has expired anyway). Created 0600 root-owned before anything is written
// to it, so the secret is never briefly world-readable.
const sanddEnvFile = "/run/nebula-sandd.env"

// SandD env keys Nebula OWNS. A Pod that sets one of these has it dropped rather
// than passed to the container: they are the daemon's identity, and letting the
// workload choose its own would defeat the point of minting a per-instance token.
const (
	sanddControllerURLEnv = "SANDD_CONTROLLER_URL"
	sanddTokenEnv         = "SANDD_TOKEN"
	// SANDD_DAEMON_ID is deliberately NOT rendered: the controller registers an
	// authenticated daemon under its token's `sub` and ignores the id in the Register
	// message, so the token is the single source of the identity and a second copy
	// could only drift from it. Reserved all the same — Nebula owns the SANDD_* names,
	// and a Pod-chosen daemon id is exactly the impersonation attempt the controller's
	// sub-wins rule closes.
	sanddDaemonIDEnv = "SANDD_DAEMON_ID"
)

// isReservedSandDEnv reports whether k is a Nebula-owned SandD variable that must
// not be settable from the Pod.
func isReservedSandDEnv(k string) bool {
	return k == sanddControllerURLEnv || k == sanddTokenEnv || k == sanddDaemonIDEnv
}

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

	// SandD: when the Pod's command IS the daemon, fetch the binary onto the HOST
	// now so the bind-mount below can put it inside the user's image. See
	// needsSandd / sanddHostFetchScript.
	if needsSandd(spec) {
		fmt.Fprintf(&b, sanddHostFetchScript, sanddHostDir, sanddBinaryURL)
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
	// SandD's dial-out credentials go into a root-only env file that docker reads with
	// --env-file, NOT onto the `docker run` command line like the user's env below.
	// The token is a bearer credential, and an argument is world-readable through
	// /proc/<pid>/cmdline — any process on the instance, including the workload
	// itself, could `ps` it out and impersonate this daemon. The file is 0600 and
	// owned by root, so only the bootstrap and the daemon see it.
	if spec.SanddAuth.Token != "" {
		b.WriteString("install -m 600 /dev/null " + sanddEnvFile + "\n")
		// A quoted heredoc ('EOF') so the shell performs NO expansion on the token: a
		// $ or backtick in a base64url JWT segment would otherwise be interpreted.
		fmt.Fprintf(&b, "cat > %s <<'NEBULA_SANDD_EOF'\n", sanddEnvFile)
		// Docker's --env-file format is bare KEY=VALUE lines, NOT shell — no quoting,
		// and a quote character would become part of the value.
		fmt.Fprintf(&b, "%s=%s\n", sanddControllerURLEnv, spec.SanddAuth.Endpoint)
		fmt.Fprintf(&b, "%s=%s\n", sanddTokenEnv, spec.SanddAuth.Token)
		b.WriteString("NEBULA_SANDD_EOF\n")
	}

	b.WriteString("docker run --rm --gpus all")
	if spec.SanddAuth.Token != "" {
		fmt.Fprintf(&b, " --env-file %s", sanddEnvFile)
	}
	for _, k := range sortedKeys(spec.Env) {
		// Reserved names are dropped rather than passed through, so a Pod cannot set
		// SANDD_TOKEN itself and hand its daemon a forged identity. Done by explicit
		// filtering rather than by relying on docker's -e / --env-file precedence,
		// which is easy to get backwards and would silently flip this guarantee.
		if isReservedSandDEnv(k) {
			continue
		}
		fmt.Fprintf(&b, " -e %s", shellQuote(k+"="+spec.Env[k]))
	}

	// SandD: bind-mount the HOST-fetched binary read-only into the container, which
	// is what makes the daemon appear inside an arbitrary user image (the image is
	// something like ubuntu:24.04 and does not ship it). Mounting from the host —
	// rather than fetching inside the container — means the image needs no curl, wget
	// or package manager (so distroless works), and the download happens once per
	// instance instead of once per container start.
	//
	// The DIRECTORY is mounted, not the file: Docker creates a missing bind-mount
	// target, and for a file source it would still create the target as a directory,
	// leaving the daemon path unexecutable. Mounting the dir also keeps the source
	// and target paths identical, so the Pod's command needs no rewriting.
	// :ro is safe to share — an immutable executable; the daemon's writable state
	// lives in the container's own filesystem.
	if needsSandd(spec) {
		fmt.Fprintf(&b, " -v %s", shellQuote(sanddHostDir+":"+sanddHostDir+":ro"))
	}

	// Map Command/Args straight onto Docker's --entrypoint/CMD. runArgs are everything
	// that follows the image: the entrypoint's own arguments (Command[1:]) then the CMD
	// arguments (Args), in that order.
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

// sanddBinaryURL is the statically-linked (musl) SandD daemon release asset. Being
// static, this one binary runs in any container image regardless of its libc —
// load-bearing, because it is fetched on the host (Amazon Linux 2) and executed
// inside an arbitrary user image, so a dynamically-linked build would break on any
// image whose libc differs from the AMI's.
//
// Tracks `latest` deliberately while SandD and Nebula move in lockstep pre-1.0, so a
// SandD fix reaches new instances without a Nebula deploy.
//
// The COST, since it is not obvious: an instance resolves this at boot, so two instances
// launched from the same Nebula build can run different daemons, and a breaking SandD
// release changes every instance launched after it with nothing in this repo's history to
// explain it. Pin it (to a `releases/download/<tag>/` URL) once SandD is stable or once a
// daemon change can break a running fleet.
//
// Whatever it resolves to must understand SANDD_TOKEN — v0.0.7 or later, which `latest`
// satisfies as of 2026-08-09. The bootstrap delivers the daemon's identity through that
// env var; an older binary ignores it, dials in with no Authorization header, and is
// rejected with a 401 by the controller's verifier.
const sanddBinaryURL = "https://github.com/InftyAI/SandD/releases/latest/download/sandd-linux-amd64"

// sanddHostDir is where the host fetches the SandD binary, and the path it is
// bind-mounted at inside the container. It is the DIRECTORY of the shared
// nebulav1alpha1.SanddPath contract, so the mounted binary lands exactly where the
// Pod's command says it is — the host and container paths are identical, which is
// why nothing has to rewrite the command.
var sanddHostDir = path.Dir(nebulav1alpha1.SanddPath)

// needsSandd reports whether this instance must have the SandD binary staged. The
// signal is the Pod's own command naming SanddPath: that command IS the contract
// (see nebulav1alpha1.SanddPath), so the bootstrap stages the binary exactly when
// the container is going to execute it.
//
// Keying on the command rather than on SandD.Token is deliberate — the two are
// independent. A sandbox whose token mint was skipped still needs the binary (it
// would otherwise exit 127 instead of starting and retrying its dial), and a plain
// GPU workload that carries a token still runs the user's own command and must not
// have a mount injected.
func needsSandd(spec InstanceSpec) bool {
	return len(spec.Command) > 0 && spec.Command[0] == nebulav1alpha1.SanddPath
}

// sanddHostFetchScript downloads the static SandD binary into sanddHostDir on the
// HOST, as a fragment of the user-data that runs BEFORE `docker run`. It is a fmt
// format string: %[1]s=dir, %[2]s=binary URL (both trusted package constants, so
// there is no shell-injection concern).
//
// FAIL-LOUD, unlike the headscale-era bootstrap this restores: back then the daemon
// ran ALONGSIDE the workload, so a failed fetch had to be swallowed to leave the
// workload untouched. Now the daemon IS the container's command, so a missing binary
// means the container has nothing to run — better to abort the bootstrap here, where
// the reason reaches the console, than to let `docker run` fail with a bare exit 127.
// The enclosing user-data runs under `set -euo pipefail`, so a failure of any command
// below stops the script before the instance starts billing for a useless container.
//
// Progress logs carry a `[sandd]` prefix and go to stderr, which reaches the instance
// CONSOLE (get-console-output) — the only way to debug bring-up on a Nebula virtual
// node, since kubectl logs/exec do not work against it.
//
// DOWNLOADER: curl is expected (the ECS GPU AMI ships it) but not assumed. The AMI is
// resolved at runtime as "newest image matching gpuAMINameFilter" (see resolveGPUAMI),
// so its contents can drift under us and a user-supplied AMI may differ; wget is tried
// as a fallback and the absence of BOTH is reported as itself. Without this the script
// would die on a bare "curl: command not found" under `set -e` — a message that names
// the symptom but not the cause, on the one debug channel a virtual node has.
//
// The fetch is verified rather than trusted: a 0-byte or truncated download would
// otherwise be chmod'd, mounted, and fail as a cryptic exec error inside the
// container, far from the actual cause. `-s` catches the empty/partial case, and a
// non-executable file is caught before the container is ever started.
const sanddHostFetchScript = `echo "[sandd] fetching daemon binary" >&2
mkdir -p %[1]s
if command -v curl >/dev/null 2>&1; then
  curl -fsSL --retry 3 --retry-connrefused %[2]s -o %[1]s/sandd
elif command -v wget >/dev/null 2>&1; then
  wget -q --tries=3 -O %[1]s/sandd %[2]s
else
  echo "[sandd] FATAL: neither curl nor wget on the instance; cannot fetch the daemon from %[2]s" >&2
  exit 1
fi
if [ ! -s %[1]s/sandd ]; then
  echo "[sandd] FATAL: downloaded daemon is empty or missing (%[1]s/sandd) from %[2]s" >&2
  exit 1
fi
chmod +x %[1]s/sandd
echo "[sandd] daemon binary staged at %[1]s/sandd" >&2
`

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
