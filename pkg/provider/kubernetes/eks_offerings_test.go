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

package kubernetes

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/InftyAI/Nebula/pkg/provider"
)

// fakeEC2 answers DescribeInstanceTypeOfferings from types, recording the filter it was given.
type fakeEC2 struct {
	types  []string
	err    error
	filter []string
}

func (f *fakeEC2) DescribeInstanceTypeOfferings(_ context.Context, in *ec2.DescribeInstanceTypeOfferingsInput,
	_ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypeOfferingsOutput, error) {
	f.filter = in.Filters[0].Values
	if f.err != nil {
		return nil, f.err
	}
	out := &ec2.DescribeInstanceTypeOfferingsOutput{}
	for _, t := range f.types {
		out.InstanceTypeOfferings = append(out.InstanceTypeOfferings,
			ec2types.InstanceTypeOffering{InstanceType: ec2types.InstanceType(t)})
	}
	return out, nil
}

func TestOfferedFamilies(t *testing.T) {
	// us-west-1 as it is: g4dn and g6, but no p4d.
	api := &fakeEC2{types: []string{"g4dn.xlarge", "g4dn.metal", "g6.xlarge"}}
	got, err := offeredFamilies(context.Background(), api, []string{"p4d", "g4dn"})
	if err != nil {
		t.Fatal(err)
	}
	if got["p4d"] || !got["g4dn"] {
		t.Fatalf("offered = %v, want g4dn and not p4d", got)
	}
	if !slices.Equal(api.filter, []string{"p4d.*", "g4dn.*"}) {
		t.Fatalf("filter = %v, want one pattern per family", api.filter)
	}
}

// Each cluster keeps only what its region offers (H100 is p5, L4 g6 or gr6 here), which is
// what lets placement skip a region rather than fail there. A failed lookup keeps all.
func TestNewKeepsTheFamiliesEachRegionOffers(t *testing.T) {
	clients := map[string]clusterClients{
		"eks-use1": {kube: kubefake.NewClientset(), ec2: &fakeEC2{types: []string{"p5.48xlarge", "g6.xlarge"}}},
		"eks-eun1": {kube: kubefake.NewClientset(), ec2: &fakeEC2{types: []string{"g6.xlarge"}}},
		"eks-usw2": {kube: kubefake.NewClientset(), ec2: &fakeEC2{err: errors.New("throttled")}},
	}
	p := newTestProvider(clients)

	for _, tc := range []struct {
		accel, region string
		want          bool
	}{
		{"H100", "us-east-1", true},
		{"H100", "eu-north-1", false}, // no p5 there
		{"H100", "us-west-2", true},   // the lookup failed, so nothing was dropped
		{"L4", "eu-north-1", true},    // g6 alone is enough
		{"L4", "nowhere-1", false},    // no cluster in that region
	} {
		if got := p.ServesAcceleratorIn(tc.accel, tc.region); got != tc.want {
			t.Errorf("ServesAcceleratorIn(%s, %s) = %v, want %v", tc.accel, tc.region, got, tc.want)
		}
	}
	if fs := p.clusters["eu-north-1"].families["L4"]; !slices.Equal(fs, []string{"g6"}) {
		t.Errorf("eu-north-1 L4 families = %v, want [g6]: gr6 is not offered there", fs)
	}
	// Provider-wide, H100 is still offered, by the clusters that have it.
	if _, ok := p.MapAccelerator("H100", 1); !ok {
		t.Error("MapAccelerator(H100) = false, want true while some cluster offers it")
	}

	none := newTestProvider(map[string]clusterClients{
		"eks-use1": {kube: kubefake.NewClientset(), ec2: &fakeEC2{}},
		"eks-eun1": {kube: kubefake.NewClientset(), ec2: &fakeEC2{}},
		"eks-usw2": {kube: kubefake.NewClientset(), ec2: &fakeEC2{}},
	})
	if _, ok := none.MapAccelerator("H100", 1); ok {
		t.Error("MapAccelerator(H100) = true with no region offering it, want false")
	}
	var _ provider.RegionAccelerators = none
}
