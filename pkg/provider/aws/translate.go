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

	// docker run --gpus all -d, with env and command appended. Env keys are sorted
	// so the rendered script is deterministic (stable across reconciles and easy to
	// assert in tests).
	b.WriteString("docker run --rm --gpus all")
	for _, k := range sortedKeys(spec.Env) {
		fmt.Fprintf(&b, " -e %s", shellQuote(k+"="+spec.Env[k]))
	}
	b.WriteString(" " + shellQuote(spec.Image))
	for _, arg := range spec.Command {
		b.WriteString(" " + shellQuote(arg))
	}
	b.WriteString("\n")

	return base64.StdEncoding.EncodeToString([]byte(b.String())), nil
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
	case "InsufficientInstanceCapacity", "InsufficientHostCapacity", "Unsupported":
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
