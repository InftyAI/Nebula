package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NodePoolSpec is the placement policy for a set of workloads. It is the
// long-lived, user-facing object: editing it changes behaviour for every Pod
// that selects the pool, without touching any workload.
//
// Placement resolves along two orthogonal axes, and the ORDER between them is
// fixed: capacity type first, provider second.
//
//	FOR each capacityType in CapacityTypes (in listed order):   // outer: hard tier
//	    candidates = Providers x (each provider's Regions) x {this capacityType},
//	                 available now, minus blocklist                // region nests per provider
//	    IF candidates non-empty:
//	        pick one via Strategy (LowestPrice | Ordered | Weighted) // inner: rank candidates
//	        DONE
//	    // else fall through to the next capacity tier
//
// Region is a per-provider axis (see ProviderSpec.Regions), nested under each
// provider because a region name only means something to one provider. It
// widens the candidate key to {provider, region, accelerator, capacityType}
// without changing the tier-first ordering above.
//
// So CapacityTypes is a hard preference: every provider's Spot is tried before
// ANY provider's OnDemand. This is deliberate — "spot everywhere before any
// on-demand" is the least surprising behaviour, even if a different provider's
// on-demand were momentarily cheaper. Strategy only ranks providers *within*
// the active capacity tier; it never crosses tiers.
//
// The Weighted strategy requires a weight on every provider ref. This is a
// static property of the spec, so it is enforced at admission by the CEL rule
// below rather than surfaced as a status condition after the fact.
// +kubebuilder:validation:XValidation:rule="self.strategy != 'Weighted' || self.providers.all(p, has(p.weight))",message="strategy Weighted requires a weight on every provider"
// +kubebuilder:validation:XValidation:rule="self.providers.all(p, p.name != 'aws' || (has(p.regions) && size(p.regions) > 0))",message="provider aws requires at least one region"
type NodePoolSpec struct {
	// Providers is the ordered set of NeoClouds this pool is allowed to use.
	// A Pod bound to this pool can only ever be placed on a provider in this
	// list. Order is significant only for the Ordered strategy (it is the
	// inner, provider-ranking axis). A pool can list at most 8 providers so the
	// candidate set remains bounded while larger configurations are unproven.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	Providers []ProviderSpec `json:"providers"`

	// CapacityTypes is the OUTER axis: the purchase models to try, in fallback
	// order. e.g. [Spot, OnDemand] means "use spot on any provider first; only
	// when spot is exhausted everywhere, drop to on-demand". A single-element
	// list pins the pool to that type. This replaces a spot on/off flag.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:default={OnDemand,Spot}
	CapacityTypes []CapacityType `json:"capacityTypes,omitempty"`

	// Strategy is the INNER axis: how to rank providers within the active
	// capacity tier. It never overrides the capacity tier ordering.
	// +kubebuilder:validation:Enum=LowestPrice;Ordered;Weighted
	// +kubebuilder:default=Ordered
	Strategy PlacementStrategy `json:"strategy,omitempty"`

	// Failover controls how a provider that fails at provision time (e.g.
	// RunPod reports no capacity) is temporarily excluded and re-tried.
	// +optional
	Failover *FailoverPolicy `json:"failover,omitempty"`
}

// ProviderSpec is one provider's entry in a pool: which provider, and the
// per-provider placement policy (Weighted share, allowed regions). It is not a
// mere reference — it carries config — so it is a Spec, not a Ref.
type ProviderSpec struct {
	// Name is the provider identifier, matching the ProviderLabel value on that
	// provider's virtual node (e.g. "runpod", "modal", "kubernetes").
	Name string `json:"name"`

	// Weight is the relative share of new placements for the Weighted strategy.
	// Ignored by other strategies.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Weight *int32 `json:"weight,omitempty"`

	// Regions constrains this provider to a subset of its regions, in the
	// provider's OWN vocabulary (e.g. ["us-east-1","eu-west-2"] for AWS). Region
	// is provider-namespaced — there is no cross-provider region vocabulary — so
	// it lives here per provider, not on the pool. Two cases:
	//   - omitted/empty => the provider's configured default region (the region
	//     its client resolved from env/config/instance metadata at startup). This
	//     is the no-surprise default for region-simple providers (Modal, RunPod),
	//     which have a single region and ignore this field.
	//   - explicit list => exactly those regions.
	// AWS is the exception: it is region-aware with no meaningful single default,
	// so a `- name: aws` entry MUST list at least one region. That is enforced at
	// admission by the CEL rule on NodePoolSpec (a per-provider requirement, so it
	// belongs on the spec where all provider entries are visible, not as a blanket
	// MinItems that would burden region-simple providers). An "all regions"
	// wildcard is intentionally NOT supported yet: it only makes sense once the
	// price-ranking optimizer can expand it against the provider's catalog and
	// choose among the results, so it is reserved for then. At most 8 regions;
	// maxLength bounds each entry.
	// +optional
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:items:MaxLength=32
	Regions []string `json:"regions,omitempty"`
}

// PlacementStrategy ranks providers WITHIN a capacity tier (the inner axis).
type PlacementStrategy string

const (
	// StrategyLowestPrice picks the lowest $/hr provider in the active tier.
	StrategyLowestPrice PlacementStrategy = "LowestPrice"
	// StrategyOrdered uses the Providers list order as strict priority.
	StrategyOrdered PlacementStrategy = "Ordered"
	// StrategyWeighted spreads placements to match per-provider weights.
	StrategyWeighted PlacementStrategy = "Weighted"
)

// CapacityType is the purchase model (the outer axis). Each provider maps it to
// its own concept — e.g. RunPod Spot -> interruptible/podRentInterruptable.
// +kubebuilder:validation:Enum=Spot;OnDemand
type CapacityType string

const (
	// CapacitySpot is interruptible/preemptible capacity (cheapest, reclaimable).
	CapacitySpot CapacityType = "Spot"
	// CapacityOnDemand is standard pay-as-you-go capacity.
	CapacityOnDemand CapacityType = "OnDemand"
)

// FailoverPolicy tunes capacity-error failover. Failover is always on — backing
// off a placement that just failed is correct behaviour, not a toggle (to avoid
// a whole provider, use a single-element Providers list instead). The blocklist
// itself is derived, high-churn runtime state held in controller memory — NOT in
// the API: when a provision fails, the controller excludes the failing placement
// (keyed at the granularity of the error, e.g. a specific provider+GPU+capacity
// type, so a failed H100 request does not block A100 requests on the same
// provider) for BlocklistTTL, then reconsiders it. This spec only tunes that.
type FailoverPolicy struct {
	// BlocklistTTL is the BASE duration a failed placement is excluded before the
	// provider becomes a candidate for it again. The controller adds a random jitter
	// (up to 30s) on top so Pods that failed for the same reason do not all retry the
	// just-freed candidate in lockstep, so the effective exclusion is this value plus
	// that jitter.
	// +kubebuilder:default="30s"
	BlocklistTTL metav1.Duration `json:"blocklistTTL,omitempty"`
}

// NodePool condition types (standard Kubernetes condition convention).
const (
	// NodePoolConditionReady is True when the pool's policy is valid and usable:
	// every referenced provider is registered and the strategy is well-formed.
	// It flips to False (with a reason) on a configuration error, so an operator
	// sees the problem on the pool rather than as silent placement failures.
	NodePoolConditionReady = "Ready"
)

// NodePool condition reasons.
const (
	// ReasonPoolValid: the pool passed validation.
	ReasonPoolValid = "Valid"
	// ReasonUnknownProvider: a spec.providers[] entry names a provider with no
	// registered adapter, so the pool cannot place onto it. This is an
	// environmental check (it depends on the provider registry, populated at
	// controller startup), so it lives here as a status condition rather than as
	// an admission rule. Static spec validation (e.g. Weighted requires weights)
	// is enforced at admission by a CEL rule on NodePoolSpec instead.
	ReasonUnknownProvider = "UnknownProvider"
)

// NodePoolStatus surfaces the current placement picture for observability.
type NodePoolStatus struct {
	// Placed counts running instances per provider, for at-a-glance balance.
	// +optional
	Placed map[string]int32 `json:"placed,omitempty"`

	// Conditions follows the standard Kubernetes condition convention.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=np
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Strategy",type=string,JSONPath=`.spec.strategy`
// +kubebuilder:printcolumn:name="Providers",type=string,JSONPath=`.spec.providers[*].name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NodePool is the placement policy for GPU workloads across NeoClouds.
type NodePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NodePoolSpec   `json:"spec,omitempty"`
	Status NodePoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NodePoolList contains a list of NodePool.
type NodePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodePool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NodePool{}, &NodePoolList{})
}
