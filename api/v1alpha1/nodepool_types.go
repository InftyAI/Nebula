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
//	        pick one via Strategy (Ordered today; see Strategy)      // inner: rank candidates
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
//
// The rule is currently UNREACHABLE — Strategy's enum admits only Ordered, so no
// object can carry Weighted for it to check. It is retained rather than deleted so
// that widening the enum is a one-line change that cannot silently ship without its
// weight validation; the cost is one always-true CEL evaluation per admission.
// +kubebuilder:validation:XValidation:rule="self.strategy != 'Weighted' || self.providers.all(p, has(p.weight))",message="strategy Weighted requires a weight on every provider"
// (AWS once required at least one region here, because an omitted list meant "the
// client's default region" and its client has none. Omitted now means "every region
// the provider serves", which is a valid — if broad — AWS policy, so the rule is gone.
// See ProviderSpec.Regions.)
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
	//
	// Only Ordered is accepted today. LowestPrice and Weighted are defined as
	// constants (and the Weighted weight rule is already enforced above) but are
	// deliberately kept OUT of the enum until the ranking is implemented: admitting
	// a value the placement walk silently ignores would let a pool claim a policy it
	// does not get, which is worse than rejecting it at admission. Widening the enum
	// is the one change needed to enable them once selectPlacement ranks.
	// +kubebuilder:validation:Enum=Ordered
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
	// Ignored by other strategies, which today means ignored entirely: Strategy
	// accepts only Ordered, so setting this has no effect until Weighted is enabled.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Weight *int32 `json:"weight,omitempty"`

	// Regions constrains where this provider may place, in the provider's OWN
	// vocabulary. Region is provider-namespaced — there is no cross-provider region
	// vocabulary — so it lives here per provider, not on the pool. It is a
	// CONSTRAINT, not a list of regions to use, and it has three levels:
	//   - omitted/empty => unconstrained: every region the provider serves. For a
	//     region-simple provider (Modal) this means "send no region and let the
	//     provider place freely", which is also its widest and cheapest mode.
	//   - a geography GROUP token ("us", "eu", "ap", ...) => that geography's
	//     regions. This is the recommended way to ask for breadth with a data
	//     residency boundary.
	//   - a literal region name ("us-east-1" on AWS, "us-east" on Modal) => exactly
	//     that region.
	// The provider resolves which level a value is, since only it knows its own
	// geography (see provider.Provider's ExpandRegions). Group tokens are shared
	// across providers but the regions behind them are not: "eu" is eu-west-1 and
	// friends on AWS, while London is eu-west-2 there and there is no "uk" group.
	//
	// A value that is not a group token is passed to the provider UNVALIDATED.
	// Region names change faster than Nebula ships, so an unrecognized one is
	// forwarded rather than rejected: a genuinely bad name fails at provision time
	// with the provider's own error, which is better than refusing a region that
	// launched last week. It is also the escape hatch for AWS opt-in regions, which
	// no group contains.
	//
	// Unconstrained on a region-aware provider is the widest setting and costs
	// something: every region becomes a placement candidate to walk on failover, and
	// every region is swept by the observability poll loop. Prefer a group unless the
	// workload genuinely needs global reach. There is no cap on the number of entries
	// (a group already expands to many, so capping the declaration would be
	// arbitrary); maxLength bounds each entry.
	// +optional
	// +kubebuilder:validation:items:MaxLength=32
	Regions []string `json:"regions,omitempty"`
}

// PlacementStrategy ranks providers WITHIN a capacity tier (the inner axis).
//
// Only StrategyOrdered is admitted by NodePoolSpec.Strategy's enum today. The other
// two are declared here so the vocabulary is stable and testable ahead of the
// ranking implementation, NOT because they can be requested — see Strategy.
type PlacementStrategy string

const (
	// StrategyLowestPrice picks the lowest $/hr provider in the active tier.
	// NOT YET ACCEPTED by the Strategy enum.
	StrategyLowestPrice PlacementStrategy = "LowestPrice"
	// StrategyOrdered uses the Providers list order as strict priority. The only
	// strategy accepted today, and the default.
	StrategyOrdered PlacementStrategy = "Ordered"
	// StrategyWeighted spreads placements to match per-provider weights.
	// NOT YET ACCEPTED by the Strategy enum.
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
	// Placed counts existing instances per provider (booting included), for
	// at-a-glance balance.
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
