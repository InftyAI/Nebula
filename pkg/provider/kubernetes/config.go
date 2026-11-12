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
	"errors"
	"fmt"
	"net/netip"
	"os"
	"slices"

	"sigs.k8s.io/yaml"

	"github.com/InftyAI/Nebula/pkg/provider"
)

// ClusterProviderEKS is the only cluster provider supported today.
const ClusterProviderEKS = "eks"

// clusterCatalogs maps a cluster provider to the price catalog its GPU mapping comes from:
// an EKS cluster's nodes are EC2 instances, so the AWS catalog's instance types say which
// families carry which GPU (see familiesByAccelerator). Supporting another cloud starts with
// an entry here.
var clusterCatalogs = map[string]string{
	ClusterProviderEKS: provider.ProviderAWS,
}

// Config is the cluster table, read from the file --kubernetes-clusters names. It holds no
// credentials: an EKS cluster is reached with Nebula's own AWS identity (see newEKSClients).
type Config struct {
	Clusters []ClusterConfig `json:"clusters"`
}

// ClusterConfig is one placement target. A NodePool selects clusters by Region or Geography,
// both stated here rather than derived from each other.
type ClusterConfig struct {
	// Name is the cluster's own name: for EKS, what DescribeCluster and the token take.
	Name     string `json:"name"`
	Provider string `json:"provider"`
	// Region is the cluster's AWS region: where DescribeCluster is called, and the token
	// placement, the NodeClaim and the blocklist carry for this cluster.
	Region string `json:"region"`
	// Geography is the Nebula geography the cluster counts as in (see provider.Geographies).
	Geography string `json:"geography"`
	// NodeAutoscaler is what adds nodes to the cluster (see nodeAutoscalers). Empty is Karpenter.
	NodeAutoscaler string `json:"nodeAutoscaler,omitempty"`
	// CIDRs are the ranges the cluster's pod IPs come from (with the VPC CNI, its VPC's CIDR
	// blocks; never the service CIDR). Namespace isolation is built from them, so ranges of
	// different clusters must not overlap: an address in both could not be told apart.
	CIDRs []string `json:"cidrs,omitempty"`
	// Host marks the cluster Nebula itself runs in, whose pods reach the others' remote pods.
	// Listed like any other entry, it may also be a placement target.
	Host bool `json:"host,omitempty"`
}

// nodeAutoscalerName defaults an empty NodeAutoscaler to Karpenter.
func (c ClusterConfig) nodeAutoscalerName() string {
	if c.NodeAutoscaler == "" {
		return NodeAutoscalerKarpenter
	}
	return c.NodeAutoscaler
}

// LoadConfig reads and validates the cluster table at path.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("kubernetes: read cluster config: %w", err)
	}
	var cfg Config
	if err := yaml.UnmarshalStrict(raw, &cfg); err != nil {
		return nil, fmt.Errorf("kubernetes: parse cluster config %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate rejects a table the provider could not act on. A region holds one cluster because
// the region is the token claims and blocks carry: two clusters in one would make teardown
// search the wrong one. Names are unique so each one is a single EKS cluster.
func (c *Config) Validate() error {
	if len(c.Clusters) == 0 {
		return errors.New("kubernetes: cluster config lists no clusters")
	}
	seen := make(map[string]bool, len(c.Clusters))
	regions := make(map[string]string, len(c.Clusters))
	type owned struct {
		prefix  netip.Prefix
		cluster string
	}
	var cidrs []owned
	for i, cl := range c.Clusters {
		if cl.Name == "" {
			return fmt.Errorf("kubernetes: cluster %d has no name", i)
		}
		if seen[cl.Name] {
			return fmt.Errorf("kubernetes: duplicate cluster name %q", cl.Name)
		}
		seen[cl.Name] = true
		if _, ok := clusterCatalogs[cl.Provider]; !ok {
			return fmt.Errorf("kubernetes: cluster %q: unsupported provider %q (want %q)",
				cl.Name, cl.Provider, ClusterProviderEKS)
		}
		if _, ok := nodeAutoscalers[cl.nodeAutoscalerName()]; !ok {
			return fmt.Errorf("kubernetes: cluster %q: unsupported node autoscaler %q", cl.Name, cl.NodeAutoscaler)
		}
		if cl.Region == "" {
			return fmt.Errorf("kubernetes: cluster %q: region is required", cl.Name)
		}
		if other, dup := regions[cl.Region]; dup {
			return fmt.Errorf("kubernetes: clusters %q and %q are both in %s; a region holds one cluster",
				other, cl.Name, cl.Region)
		}
		regions[cl.Region] = cl.Name
		if !provider.IsGeography(cl.Geography) {
			return fmt.Errorf("kubernetes: cluster %q: geography %q is not one of %v",
				cl.Name, cl.Geography, provider.Geographies)
		}
		for _, s := range cl.CIDRs {
			p, err := netip.ParsePrefix(s)
			if err != nil {
				return fmt.Errorf("kubernetes: cluster %q: cidr %q: %w", cl.Name, s, err)
			}
			if p != p.Masked() {
				return fmt.Errorf("kubernetes: cluster %q: cidr %q has host bits set (want %s)", cl.Name, s, p.Masked())
			}
			for _, o := range cidrs {
				if o.prefix.Overlaps(p) {
					return fmt.Errorf("kubernetes: cidr %s of cluster %q overlaps %s of cluster %q",
						p, cl.Name, o.prefix, o.cluster)
				}
			}
			cidrs = append(cidrs, owned{prefix: p, cluster: cl.Name})
		}
	}
	// cidrs exist only for namespace isolation, so giving any cluster some turns it on, and a
	// half-filled table would fence some clusters and leave the rest open.
	hosts, ranged := 0, 0
	for _, cl := range c.Clusters {
		if cl.Host {
			hosts++
		}
		if len(cl.CIDRs) > 0 {
			ranged++
		}
	}
	if hosts > 1 {
		return fmt.Errorf("kubernetes: %d clusters are marked host; at most one is", hosts)
	}
	if ranged > 0 && ranged < len(c.Clusters) {
		return fmt.Errorf("kubernetes: %d of %d clusters have cidrs; namespace isolation needs them on every cluster",
			ranged, len(c.Clusters))
	}
	if ranged > 0 && hosts == 0 {
		return errors.New("kubernetes: namespace isolation (cidrs are set) needs one cluster marked host")
	}
	return nil
}

// isolation is the namespace fence Provision builds once the table has cidrs, or nil. The
// other clusters' ranges are the chart's to deny (templates/cluster-network-policy.yaml).
func (c *Config) isolation() *isolationConfig {
	if !slices.ContainsFunc(c.Clusters, func(cl ClusterConfig) bool { return len(cl.CIDRs) > 0 }) {
		return nil
	}
	for _, cl := range c.Clusters {
		if cl.Host {
			return &isolationConfig{host: cl}
		}
	}
	return nil // unreachable after Validate, which requires a host once cidrs are set
}

// checkHost fails a table whose host entry is not the cluster podIP is in, which would fence
// the wrong ranges. An empty podIP (run outside a pod) is not checked.
func (iso *isolationConfig) checkHost(podIP string) error {
	if podIP == "" {
		return nil
	}
	ip, err := netip.ParseAddr(podIP)
	if err != nil {
		return fmt.Errorf("kubernetes: POD_IP %q: %w", podIP, err)
	}
	for _, s := range iso.host.CIDRs {
		if netip.MustParsePrefix(s).Contains(ip) {
			return nil
		}
	}
	return fmt.Errorf("kubernetes: this pod's IP %s is in none of host cluster %q's cidrs %v",
		ip, iso.host.Name, iso.host.CIDRs)
}
