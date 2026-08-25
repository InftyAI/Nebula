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

package v1alpha1

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestNodePoolCRDHasReadyStatusPrinterColumn(t *testing.T) {
	t.Parallel()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}
	manifestPath := filepath.Join(filepath.Dir(thisFile), "..", "..", "config", "crd", "bases", "nebula.inftyai.com_nodepools.yaml")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read NodePool CRD: %v", err)
	}

	var manifest struct {
		Spec struct {
			Versions []struct {
				Name                     string `yaml:"name"`
				AdditionalPrinterColumns []struct {
					Name     string `yaml:"name"`
					Type     string `yaml:"type"`
					JSONPath string `yaml:"jsonPath"`
				} `yaml:"additionalPrinterColumns"`
			} `yaml:"versions"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parse NodePool CRD: %v", err)
	}

	const wantJSONPath = `.status.conditions[?(@.type=="Ready")].status`
	found := 0
	for _, version := range manifest.Spec.Versions {
		if version.Name != "v1alpha1" {
			continue
		}
		for _, column := range version.AdditionalPrinterColumns {
			if column.Name != "Status" {
				continue
			}
			found++
			if column.Type != "string" || column.JSONPath != wantJSONPath {
				t.Fatalf("Status column = type %q, JSONPath %q; want string, %q", column.Type, column.JSONPath, wantJSONPath)
			}
		}
	}
	if found != 1 {
		t.Fatalf("found %d Status columns in v1alpha1; want 1", found)
	}
}
