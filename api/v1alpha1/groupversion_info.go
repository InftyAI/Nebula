// Package v1alpha1 contains the Nebula API types.
//
// Nebula is an operator that orchestrates GPU workloads across NeoClouds
// (RunPod, Modal, Kubernetes, ...). It follows a Karpenter-style split:
//
//	NodePool  - policy: which providers are allowed, how to choose between
//	            them (cost/availability), failover behaviour, and the GPU shape.
//	NodeClaim - one provisioned external instance and its lifecycle. Owns the
//	            terminate finalizer so a paid instance is never leaked.
//
// +kubebuilder:object:generate=true
// +groupName=nebula.inftyai.com
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group/version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "nebula.inftyai.com", Version: "v1alpha1"}

	// SchemeBuilder registers the types with a Scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
