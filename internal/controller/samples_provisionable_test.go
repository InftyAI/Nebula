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

package controller

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/InftyAI/Nebula/pkg/provider/catalog"
)

// The GPU shape in a sample must be one the sample's own NodePool can actually
// provision. This is not a style check: on EC2 the GPU COUNT is baked into the
// instance type rather than being a free knob, so (acceleratorType, nvidia.com/gpu)
// is a single lookup key into the catalog. A pair with no row maps to nothing and
// Provision fails with "no EC2 instance type for <type> x<n>" — after the Sandbox is
// admitted and placed, so the only symptom is a box that never becomes Ready.
//
// The samples shipped exactly that bug: acceleratorType a100-40gb with
// nvidia.com/gpu 1, against a NodePool listing aws alone. AWS's smallest A100-40GB
// instance is p4d.24xlarge, which has 8 — so `kubectl apply -f config/samples/` could
// never provision, and the SandboxSet sample multiplied it by three replicas.
//
// It reads the real YAML and the real embedded catalog rather than restating the
// shapes, so editing a sample to an unprovisionable pair fails here instead of on a
// user's cluster.

// sampleGPUShape is the fragment of a Sandbox/SandboxSet sample this test needs: the
// accelerator type, the GPU count, and the pool the shape must be servable by.
type sampleGPUShape struct {
	file        string
	accelerator string
	count       int32
	nodePoolRef string
}

// sandboxSample mirrors the two sample kinds' spec shape. SandboxSet nests the same
// spec under template, so one struct with an optional Template covers both.
type sandboxSample struct {
	Spec struct {
		NodePoolRef     string `json:"nodePoolRef"`
		AcceleratorType string `json:"acceleratorType"`
		Resources       struct {
			Limits   map[string]string `json:"limits"`
			Requests map[string]string `json:"requests"`
		} `json:"resources"`
		Template *struct {
			Spec struct {
				NodePoolRef     string `json:"nodePoolRef"`
				AcceleratorType string `json:"acceleratorType"`
				Resources       struct {
					Limits   map[string]string `json:"limits"`
					Requests map[string]string `json:"requests"`
				} `json:"resources"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
}

// nodePoolSample is the providers list of the sample NodePool, which decides which
// catalogs a shape has to be servable from.
type nodePoolSample struct {
	Spec struct {
		Providers []struct {
			Name string `json:"name"`
		} `json:"providers"`
	} `json:"spec"`
}

func TestSampleGPUShapesAreProvisionable(t *testing.T) {
	const samplesDir = "../../config/samples"

	cat, err := catalog.Load()
	if err != nil {
		t.Fatalf("loading the embedded catalog: %v", err)
	}

	// Which providers the samples' NodePool actually permits. A shape only has to be
	// servable by one of them (placement tries each), so an unlisted provider's
	// catalog must not be consulted — that is precisely how the a100-40gb x1 bug hid:
	// Modal serves it, AWS does not, and the sample pool lists only AWS.
	poolProviders := map[string][]string{}
	pools, err := filepath.Glob(filepath.Join(samplesDir, "*nodepool.yaml"))
	if err != nil {
		t.Fatalf("globbing nodepool samples: %v", err)
	}
	for _, f := range pools {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("reading %s: %v", f, err)
		}
		var np nodePoolSample
		if err := yaml.Unmarshal(raw, &np); err != nil {
			t.Fatalf("parsing %s: %v", f, err)
		}
		var names []string
		for _, p := range np.Spec.Providers {
			names = append(names, p.Name)
		}
		poolProviders[nodePoolNameFromFile(t, raw)] = names
	}

	var shapes []sampleGPUShape
	for _, kind := range []string{"*sandbox.yaml", "*sandboxset.yaml"} {
		files, err := filepath.Glob(filepath.Join(samplesDir, kind))
		if err != nil {
			t.Fatalf("globbing %s: %v", kind, err)
		}
		for _, f := range files {
			raw, err := os.ReadFile(f)
			if err != nil {
				t.Fatalf("reading %s: %v", f, err)
			}
			var s sandboxSample
			if err := yaml.Unmarshal(raw, &s); err != nil {
				t.Fatalf("parsing %s: %v", f, err)
			}

			accel, pool, limits := s.Spec.AcceleratorType, s.Spec.NodePoolRef, s.Spec.Resources.Limits
			if s.Spec.Template != nil {
				accel = s.Spec.Template.Spec.AcceleratorType
				pool = s.Spec.Template.Spec.NodePoolRef
				limits = s.Spec.Template.Spec.Resources.Limits
			}
			if accel == "" {
				continue // a CPU-only sample has no shape to check
			}
			shapes = append(shapes, sampleGPUShape{
				file:        filepath.Base(f),
				accelerator: accel,
				count:       gpuCount(t, limits),
				nodePoolRef: pool,
			})
		}
	}

	if len(shapes) == 0 {
		t.Fatal("no GPU-requesting samples found; this guard would silently pass forever")
	}

	for _, sh := range shapes {
		providers, ok := poolProviders[sh.nodePoolRef]
		if !ok {
			t.Errorf("%s: nodePoolRef %q matches no NodePool sample", sh.file, sh.nodePoolRef)
			continue
		}
		if len(providers) == 0 {
			t.Errorf("%s: NodePool %q lists no providers", sh.file, sh.nodePoolRef)
			continue
		}

		served := false
		for _, name := range providers {
			base := catalog.Base{ProviderName: name, Catalog: cat}
			if ids, ok := base.MapAccelerator(sh.accelerator, sh.count); ok && len(ids) > 0 {
				served = true
				t.Logf("%s: %s x%d -> %s %v", sh.file, sh.accelerator, sh.count, name, ids)
				break
			}
		}
		if !served {
			t.Errorf("%s: %s x%d is not servable by any provider in NodePool %q (%v). "+
				"On EC2 the GPU count is part of the instance type, so the (type, count) pair "+
				"must exist in pkg/provider/catalog/data/<provider>.csv; this sample would be "+
				"admitted and placed, then fail to provision.",
				sh.file, sh.accelerator, sh.count, sh.nodePoolRef, providers)
		}
	}
}

// nodePoolNameFromFile pulls metadata.name out of a NodePool sample, so a shape's
// nodePoolRef is matched against the real pool name rather than an assumed one.
func nodePoolNameFromFile(t *testing.T, raw []byte) string {
	t.Helper()
	var meta struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
	}
	if err := yaml.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("parsing NodePool metadata: %v", err)
	}
	return meta.Metadata.Name
}

// gpuCount reads the nvidia.com/gpu limit as the requested accelerator count. The
// samples spell it as a quoted string, which is why this is a map[string]string and
// not a resource.Quantity — the value is compared as the catalog's lookup key, not
// as a quantity.
func gpuCount(t *testing.T, limits map[string]string) int32 {
	t.Helper()
	raw, ok := limits["nvidia.com/gpu"]
	if !ok {
		return 0
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		t.Fatalf("parsing nvidia.com/gpu %q: %v", raw, err)
	}
	return int32(n)
}
