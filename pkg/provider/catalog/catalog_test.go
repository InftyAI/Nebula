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

package catalog

import (
	"os"
	"path/filepath"
	"testing"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// fakeLookup is a trivial catalog.Lookup for exercising Base's promoted methods
// without loading CSVs.
type fakeLookup struct{ rows []provider.Offering }

func (l fakeLookup) Offerings(string) []provider.Offering { return l.rows }

func TestLoad_EmbeddedModal(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	offs := c.Offerings("modal")
	if len(offs) == 0 {
		t.Fatal("expected embedded modal offerings")
	}

	// H200 must be present (proto lists it; it was missing from the old map).
	var haveH200 bool
	for _, o := range offs {
		if o.CapacityType != nebulav1alpha1.CapacityOnDemand {
			t.Fatalf("modal offering %q must be OnDemand, got %v", o.AcceleratorType, o.CapacityType)
		}
		if o.PricePerHour <= 0 {
			t.Fatalf("offering %q has non-positive price %v", o.AcceleratorType, o.PricePerHour)
		}
		if o.AcceleratorType == "H200" {
			haveH200 = true
		}
	}
	if !haveH200 {
		t.Fatal("expected H200 in modal catalog")
	}
}

func TestLoad_EmbeddedAWS(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	offs := c.Offerings("aws")
	if len(offs) == 0 {
		t.Fatal("expected embedded aws offerings")
	}

	// AWS requests by instance type, so every row must carry an accelerator_id (the
	// EC2 instance type). The embedded catalog is REGION-LESS — region is stamped by
	// the adapter's live probe, not hand-maintained here — so rows have no region.
	// Both capacity tiers appear.
	var haveSpot, haveOnDemand bool
	for _, o := range offs {
		if o.Region != "" {
			t.Fatalf("embedded aws catalog must be region-less, got region %q for %q", o.Region, o.AcceleratorType)
		}
		if o.AcceleratorID == "" {
			t.Fatalf("aws offering %q has no accelerator_id (EC2 instance type)", o.AcceleratorType)
		}
		switch o.CapacityType {
		case nebulav1alpha1.CapacitySpot:
			haveSpot = true
		case nebulav1alpha1.CapacityOnDemand:
			haveOnDemand = true
		}
	}
	if !haveSpot || !haveOnDemand {
		t.Fatalf("expected both Spot and OnDemand aws rows, got spot=%v ondemand=%v", haveSpot, haveOnDemand)
	}

	// gpu_count is a lookup dimension for AWS: the same accelerator must appear at
	// more than one count (e.g. T4 x1 and T4 x8 are different instance types).
	counts := map[int32]string{}
	for _, o := range offs {
		if o.AcceleratorType == "T4" && o.CapacityType == nebulav1alpha1.CapacityOnDemand {
			counts[o.GPUCount] = o.AcceleratorID
		}
	}
	if counts[1] == counts[8] || counts[1] == "" || counts[8] == "" {
		t.Fatalf("T4 must map to distinct instance types per count, got %+v", counts)
	}
}

func TestLoadFrom_OptionalColumnsAndComments(t *testing.T) {
	dir := t.TempDir()
	// Header-driven parsing: columns are located by name, so a file may include
	// accelerator_id/region and interleave comment/blank lines. A blank
	// accelerator_id is allowed (identity mapping) and a blank region is allowed
	// (region-simple provider).
	csv := "# a comment line\n" +
		"accelerator_type,accelerator_id,capacity_type,price_per_hour,available,region,updated\n" +
		"\n" +
		"H100,p5.48xlarge,Spot,34.41,true,us-east-1,2026-07-27\n" +
		"# another comment\n" +
		"T4,,OnDemand,0.50,true,,2026-07-27\n"
	if err := os.WriteFile(filepath.Join(dir, "modal.csv"), []byte(csv), 0o644); err != nil {
		t.Fatal(err)
	}

	c, err := LoadFrom(dir)
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	offs := c.Offerings("modal")
	if len(offs) != 2 {
		t.Fatalf("expected 2 rows (comments/blanks skipped), got %d: %+v", len(offs), offs)
	}

	byType := map[string]provider.Offering{}
	for _, o := range offs {
		byType[o.AcceleratorType] = o
	}
	h := byType["H100"]
	if h.AcceleratorID != "p5.48xlarge" || h.Region != "us-east-1" || h.CapacityType != nebulav1alpha1.CapacitySpot {
		t.Fatalf("H100 row parsed wrong: %+v", h)
	}
	if t4 := byType["T4"]; t4.AcceleratorID != "" || t4.Region != "" {
		t.Fatalf("T4 row: blank accelerator_id/region must stay blank, got %+v", t4)
	}
}

func TestLoadFrom_OverrideDir(t *testing.T) {
	dir := t.TempDir()
	csv := "accelerator_type,capacity_type,price_per_hour,available,updated\n" +
		"H100,OnDemand,9.99,true,2026-07-25\n"
	if err := os.WriteFile(filepath.Join(dir, "modal.csv"), []byte(csv), 0o644); err != nil {
		t.Fatal(err)
	}

	c, err := LoadFrom(dir)
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	offs := c.Offerings("modal")
	if len(offs) != 1 || offs[0].AcceleratorType != "H100" || offs[0].PricePerHour != 9.99 {
		t.Fatalf("override not applied: %+v", offs)
	}
}

func TestLoad_OverrideEnvWins(t *testing.T) {
	dir := t.TempDir()
	csv := "accelerator_type,capacity_type,price_per_hour,available,updated\n" +
		"T4,OnDemand,0.11,true,2026-07-25\n"
	if err := os.WriteFile(filepath.Join(dir, "modal.csv"), []byte(csv), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(OverrideDirEnv, dir)

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	offs := c.Offerings("modal")
	if len(offs) != 1 || offs[0].PricePerHour != 0.11 {
		t.Fatalf("env override not honored: %+v", offs)
	}
}

func TestBaseMapAccelerator_CaseInsensitive(t *testing.T) {
	base := Base{
		ProviderName: "modal",
		Catalog: fakeLookup{rows: []provider.Offering{
			{AcceleratorType: "H100", CapacityType: nebulav1alpha1.CapacityOnDemand},
			{AcceleratorType: "A100-80GB", CapacityType: nebulav1alpha1.CapacityOnDemand},
		}},
	}

	// A canonical accelerator supplied in any case must resolve, and the returned
	// id is always the catalog's canonical casing — so a lowercase
	// accelerator-type label ("h100") maps cleanly to the provider's accelerator ("H100").
	cases := map[string]struct {
		in     string
		want   string
		wantOK bool
	}{
		"exact":     {"H100", "H100", true},
		"all lower": {"h100", "H100", true},
		"mixed":     {"a100-80gb", "A100-80GB", true},
		"unknown":   {"tpu-v4", "", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok := base.MapAccelerator(tc.in)
			if ok != tc.wantOK || got != tc.want {
				t.Fatalf("MapAccelerator(%q) = (%q, %v), want (%q, %v)", tc.in, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestOfferings_CopyIsolation(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	a := c.Offerings("modal")
	if len(a) == 0 {
		t.Skip("no modal rows")
	}
	a[0].PricePerHour = -1 // mutate the returned copy
	b := c.Offerings("modal")
	if b[0].PricePerHour == -1 {
		t.Fatal("Offerings returned a shared slice; caller mutation leaked into catalog")
	}
}
