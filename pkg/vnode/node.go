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

package vnode

import (
	"context"
	"fmt"
	"time"

	vknode "github.com/virtual-kubelet/virtual-kubelet/node"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/version"
)

// informerResync is the full-resync period for the virtual node's informers.
const informerResync = time.Minute

// NodeName returns the virtual node name for a provider: "nebula-<provider>".
// One static node per provider (see docs/architecture.md §3); the scheduler
// routes an ungated Pod to it via the ProviderLabel nodeSelector.
func NodeName(providerName string) string {
	return "nebula-" + providerName
}

// RBAC for the virtual kubelet. The VK pod controller reports Pod status back to
// the API server and reacts to the config/secret/service objects a Pod
// references; the node controller creates/maintains the virtual Node and its
// node lease and emits events. These grants back the node/pod controllers wired
// in Start.
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps;secrets;services,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

// Runner is a manager.Runnable that owns one provider's virtual node. It starts
// the VK node/pod controllers and blocks until the manager's context is
// cancelled, so it participates in the manager's lifecycle (and, if enabled,
// leader election) like any other runnable.
//
// It deliberately wires the lower-level virtual-kubelet `node` package directly
// rather than the `nodeutil` convenience wrapper: nodeutil pulls in a kubelet
// HTTP/auth stack whose apiserver dependency does not compile against the k8s
// 0.33 line this project pins. Nebula does not serve the kubelet API (logs/exec
// are unsupported — see handler.go), so none of that surface is needed.
type Runner struct {
	prov     provider.Provider
	client   kubernetes.Interface
	nodeName string
}

// NewRunner builds the virtual-node runner for one provider.
func NewRunner(prov provider.Provider, client kubernetes.Interface) *Runner {
	return &Runner{
		prov:     prov,
		client:   client,
		nodeName: NodeName(prov.Name()),
	}
}

var _ manager.Runnable = (*Runner)(nil)

// Start builds and runs the virtual node until ctx is cancelled. It is invoked
// by the controller-runtime manager.
func (r *Runner) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithValues("virtualNode", r.nodeName, "provider", r.prov.Name())

	handler := NewHandler(r.prov)
	nodeSpec := nodeSpec(r.nodeName, r.prov.Name())

	// Pod informer scoped to this node only (spec.nodeName == nodeName), so the
	// pod controller reacts to exactly the Pods the scheduler bound here.
	podFactory := informers.NewSharedInformerFactoryWithOptions(
		r.client, informerResync,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", r.nodeName).String()
		}),
	)
	// Cluster-wide factory for the config/secret/service informers the pod
	// controller needs to resolve pod references.
	scmFactory := informers.NewSharedInformerFactoryWithOptions(r.client, informerResync)

	podInformer := podFactory.Core().V1().Pods()
	secretInformer := scmFactory.Core().V1().Secrets()
	configMapInformer := scmFactory.Core().V1().ConfigMaps()
	serviceInformer := scmFactory.Core().V1().Services()

	eb := record.NewBroadcaster()
	recorder := eb.NewRecorder(scheme.Scheme, corev1.EventSource{Component: r.nodeName + "/pod-controller"})

	pc, err := vknode.NewPodController(vknode.PodControllerConfig{
		PodClient:         r.client.CoreV1(),
		EventRecorder:     recorder,
		Provider:          handler,
		PodInformer:       podInformer,
		SecretInformer:    secretInformer,
		ConfigMapInformer: configMapInformer,
		ServiceInformer:   serviceInformer,
	})
	if err != nil {
		return fmt.Errorf("build pod controller for %q: %w", r.nodeName, err)
	}

	// A NaiveNodeProvider marks the node Ready and answers pings; our Handler
	// owns only pod lifecycle, not node health.
	np := vknode.NewNaiveNodeProvider()
	leases := r.client.CoordinationV1().Leases(corev1.NamespaceNodeLease)
	nc, err := vknode.NewNodeController(
		np, nodeSpec, r.client.CoreV1().Nodes(),
		vknode.WithNodeEnableLeaseV1(leases, vknode.DefaultLeaseDuration),
	)
	if err != nil {
		return fmt.Errorf("build node controller for %q: %w", r.nodeName, err)
	}

	eb.StartLogging(func(format string, args ...interface{}) { log.Info(fmt.Sprintf(format, args...)) })
	defer eb.Shutdown()

	go podFactory.Start(ctx.Done())
	go scmFactory.Start(ctx.Done())

	log.Info("starting virtual node")
	if err := r.run(ctx, pc, nc, nodeSpec, np); err != nil && ctx.Err() == nil {
		return err
	}
	log.Info("virtual node stopped")
	return nil
}

// run starts the pod and node controllers in the required order (pods ready
// before the node advertises Ready) and blocks until ctx is cancelled or a
// controller exits.
func (r *Runner) run(
	ctx context.Context, pc *vknode.PodController, nc *vknode.NodeController,
	nodeSpec *corev1.Node, np *vknode.NaiveNodeProviderV2,
) error {
	go pc.Run(ctx, 1) //nolint:errcheck

	select {
	case <-ctx.Done():
		return nil
	case <-pc.Ready():
	case <-pc.Done():
		return pc.Err()
	}

	go nc.Run(ctx) //nolint:errcheck

	select {
	case <-ctx.Done():
		return nil
	case <-nc.Ready():
	case <-nc.Done():
		return nc.Err()
	}

	// Mark the node Ready now that both controllers are up.
	markNodeReady(nodeSpec)
	if err := np.UpdateStatus(ctx, nodeSpec); err != nil {
		return fmt.Errorf("mark virtual node ready: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil
	case <-nc.Done():
		return nc.Err()
	case <-pc.Done():
		return pc.Err()
	}
}

// nodeSpec produces the Node object for a provider's virtual node: the
// ProviderLabel the placement controller selects on, and a NoSchedule taint so
// only Nebula-placed Pods (which the placement controller tolerates) ever land
// here.
func nodeSpec(nodeName, providerName string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Labels: map[string]string{
				// hostname is a well-known label the scheduler's default topology
				// spread keys off. ProviderLabel routes a Pod to this provider's node
				// (the placement controller's nodeSelector, paired with the NoSchedule
				// taint of the same key below). ManagedByLabel is the management-scoped
				// "this object is Nebula's" marker for tooling, policies, and queries.
				corev1.LabelHostname:          nodeName,
				nebulav1alpha1.ProviderLabel:  providerName,
				nebulav1alpha1.ManagedByLabel: nebulav1alpha1.ManagedByValue,
				// Populate the ROLES column of `kubectl get nodes`, which is derived
				// solely from node-role.kubernetes.io/<role> label KEYS (the value is
				// ignored, so it is conventionally ""). Two roles are advertised:
				// "worker" so the node reads as a schedulable worker like any other (it
				// runs Pods), and "nebula" so Nebula's virtual nodes are identifiable at
				// a glance rather than blending in with real workers.
				"node-role.kubernetes.io/worker": "",
				"node-role.kubernetes.io/nebula": "",
			},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{{
				Key:    nebulav1alpha1.ProviderLabel,
				Value:  providerName,
				Effect: corev1.TaintEffectNoSchedule,
			}},
		},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{
				// Surfaces in the VERSION column of `kubectl get nodes`. Stamped at
				// build time via -ldflags (see pkg/version); an unstamped build reports
				// "nebula-dev".
				KubeletVersion:  version.Get(),
				OperatingSystem: "linux",
				// Architecture feeds the well-known kubernetes.io/arch label, which
				// workloads pin via nodeSelector/affinity. amd64 is correct for every
				// currently-wired provider (Modal/RunPod are x86). The true arch isn't
				// known at node-creation time (no instance exists yet), so this is a
				// per-provider default, not a universal truth.
				// TODO: source arch from the provider (e.g. via Capabilities) when an
				// arm64 provider (e.g. Grace-Hopper/GH200) lands, or a Pod pinning
				// kubernetes.io/arch=arm64 will fail to schedule onto this node.
				Architecture: "amd64",
			},
			// Advertise generous virtual capacity; the external provider, not the
			// kubelet, enforces the real shape. Placement is driven by the
			// nodeSelector, not by these numbers.
			Capacity:    virtualCapacity(),
			Allocatable: virtualCapacity(),
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady}},
		},
	}
}

// markNodeReady flips the node's Ready condition to True. Called once both
// controllers are up so the scheduler starts binding Pods to it.
func markNodeReady(n *corev1.Node) {
	n.Status.Phase = corev1.NodeRunning
	for i := range n.Status.Conditions {
		if n.Status.Conditions[i].Type != corev1.NodeReady {
			continue
		}
		n.Status.Conditions[i].Status = corev1.ConditionTrue
		n.Status.Conditions[i].Reason = "KubeletReady"
		n.Status.Conditions[i].Message = "virtual node ready"
		return
	}
	n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{
		Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady",
	})
}

// nvidiaGPUResource is the extended-resource key GPU Pods may request under. A
// Nebula Pod expresses its accelerator type+count on the accelerators annotation
// (the provider reads it from there, not from limits), but a Pod author may
// still set nvidia.com/gpu limits for portability; if they do, the virtual node
// must advertise the resource or the scheduler rejects the Pod with "Insufficient
// nvidia.com/gpu" before provisioning ever starts.
const nvidiaGPUResource = "nvidia.com/gpu"

// virtualCapacity is the nominal capacity advertised by a virtual node. The
// numbers are deliberately synthetic and effectively infinite: a virtual node
// has no real machine behind it, so capacity's only job is to clear the
// scheduler's fit check (requests ≤ allocatable) and let the Pod bind. The
// provider enforces the real shape and rejects (→ failover) if the workload
// can't actually be satisfied. GPUs are advertised for the same reason as
// cpu/memory — a GPU Pod carries nvidia.com/gpu in its limits, and without an
// allocatable count here the scheduler would never bind it.
//
// TODO: this advertises only nvidia.com/gpu, the sole key the current (Modal)
// provider reads. A provider serving a different accelerator resource key (e.g.
// amd.com/gpu, or a typed MIG key) would have its GPU Pods rejected by the
// scheduler before provisioning. When such a provider lands, drive capacity from
// the provider instead of this shared list — e.g. have Capabilities declare the
// accelerator resource keys it serves and build the node capacity from that.
func virtualCapacity() corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1k"),
		corev1.ResourceMemory: resource.MustParse("10Ti"),
		corev1.ResourcePods:   resource.MustParse("1k"),
		nvidiaGPUResource:     resource.MustParse("1k"),
	}
}
