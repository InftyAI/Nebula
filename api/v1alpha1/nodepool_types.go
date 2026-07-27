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
//	    candidates = Providers x {this capacityType}, available now, minus blocklist
//	    IF candidates non-empty:
//	        pick one via Strategy (LowestPrice | Ordered | Weighted) // inner: rank providers
//	        DONE
//	    // else fall through to the next capacity tier
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
type NodePoolSpec struct {
	// Providers is the ordered set of NeoClouds this pool is allowed to use.
	// A Pod bound to this pool can only ever be placed on a provider in this
	// list. Order is significant only for the Ordered strategy (it is the
	// inner, provider-ranking axis).
	// +kubebuilder:validation:MinItems=1
	Providers []ProviderRef `json:"providers"`

	// CapacityTypes is the OUTER axis: the purchase models to try, in fallback
	// order. e.g. [Spot, OnDemand] means "use spot on any provider first; only
	// when spot is exhausted everywhere, drop to on-demand". A single-element
	// list pins the pool to that type. This replaces a spot on/off flag and
	// extends to Reserved without new fields.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:default={Reserved,OnDemand,Spot}
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

// ProviderRef names a provider and, for the Weighted strategy, its share.
type ProviderRef struct {
	// Name is the provider identifier, matching the ProviderLabel value on that
	// provider's virtual node (e.g. "runpod", "modal", "kubernetes").
	Name string `json:"name"`

	// Weight is the relative share of new placements for the Weighted strategy.
	// Ignored by other strategies.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Weight *int32 `json:"weight,omitempty"`
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
// +kubebuilder:validation:Enum=Spot;OnDemand;Reserved
type CapacityType string

const (
	// CapacitySpot is interruptible/preemptible capacity (cheapest, reclaimable).
	CapacitySpot CapacityType = "Spot"
	// CapacityOnDemand is standard pay-as-you-go capacity.
	CapacityOnDemand CapacityType = "OnDemand"
	// CapacityReserved is pre-committed/reserved capacity (not all providers
	// support it; reserved for future use).
	CapacityReserved CapacityType = "Reserved"
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
	// BlocklistTTL is how long a failed placement is excluded before the
	// provider becomes a candidate for it again.
	// +kubebuilder:default="10m"
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
