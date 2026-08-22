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
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/version"
)

// informerResync is the full-resync period for the virtual node's informers.
const informerResync = time.Minute

// podSyncWorkers is how many workers the pod controller runs per queue. VK serializes
// work per pod key, so distinct pods provision in parallel while one key never runs
// twice — without this, a single slow provision blocks pods that would succeed
// instantly. Modest, to bound concurrent bursts against a provider's rate limits.
//
// Worthless on its own: the workers pull from queues whose ADMISSION is rate limited, so
// the ceiling is podQueueRate below, not this. See podQueueRateLimiter.
const podSyncWorkers = 8

// podQueueRate and podQueueBurst size the token bucket that admits work into each of the
// pod controller's queues.
//
// This is the single most important number in this file at fleet scale. VK's default,
// which applies whenever the config leaves a limiter nil, is
// workqueue.DefaultControllerRateLimiter(): a bucket of 10 items/s, burst 100, shared by
// EVERY key in the queue. Every Pod event goes in through the rate-limited Enqueue, so
// that default caps the whole node's pod pipeline at 10 events/s no matter how many
// workers run or how much CPU the manager has — and a Pod costs two or three events on
// its way through (the delete, the terminal status it produces, the final removal), so a
// batch teardown measured 3.4-6 Pods/s and did not move when the CPU limit doubled.
//
// 200/s sustained with a 400 burst is deliberately still a ceiling rather than none: the
// queues exist to protect the API server from a fleet-sized burst, and each admitted item
// is a status write. It buys 20x headroom over the default while leaving something in
// place if a bug ever turns into an emit storm.
//
// The floor it has to clear is the poll loop, which re-emits every tracked pod on every
// tick to keep status propagation level-triggered (see reconcileOnce): N pods on the
// default 15s cadence offer N/15 items per second all by themselves — 66/s at a thousand
// pods, which is why the default bucket was permanently saturated at that size.
const (
	podQueueRate  = 200
	podQueueBurst = 400
)

// podQueueRateLimiter returns the limiter for ONE of the pod controller's queues.
//
// A fresh instance per call, never a shared one: the bucket is per limiter, so handing the
// same value to two queues would make them compete for one budget — the exact coupling
// (status pushes starving deletes) that makes a stall hard to read.
//
// The per-key exponential half is kept exactly as VK's default has it. It only fires after
// a FAILED sync, and it is what stops one pod whose write keeps erroring from spinning; it
// was never the throughput limit, so there is nothing to gain by touching it.
// The TYPED interface, though VK's config field is the untyped workqueue.RateLimiter:
// client-go 0.33 deprecates the latter, and since it is defined as TypedRateLimiter[any] the
// value returned here still assigns to that field.
func podQueueRateLimiter() workqueue.TypedRateLimiter[any] {
	return workqueue.NewTypedMaxOfRateLimiter[any](
		workqueue.NewTypedItemExponentialFailureRateLimiter[any](5*time.Millisecond, 1000*time.Second),
		&workqueue.TypedBucketRateLimiter[any]{
			Limiter: rate.NewLimiter(rate.Limit(podQueueRate), podQueueBurst),
		},
	)
}

// NodeName returns the virtual node name for a provider: "nebula-<provider>".
// One static node per provider (see docs/architecture.md §3); the scheduler
// routes an ungated Pod to it via the ProviderLabel nodeSelector.
func NodeName(providerName string) string {
	return "nebula-" + providerName
}

// RBAC for the virtual kubelet: the pod controller reports Pod status and reads the
// config/secret/service objects a Pod references; the node controller maintains the
// Node, its lease, and events. NodePools and NodeClaims are read because every provisioning
// input that is not the workload's own shape is resolved from cluster state at provision
// time rather than from the Pod (see Handler.poolFor and Handler.claimFor).
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=nodepools;nodeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps;secrets;services,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

// Runner is a manager.Runnable owning one provider's virtual node. It starts the VK
// node/pod controllers and blocks until the manager's context is cancelled, so it
// follows the manager's lifecycle and leader election like any other runnable.
//
// It wires virtual-kubelet's lower-level `node` package directly rather than the
// `nodeutil` wrapper, which pulls in a kubelet HTTP/auth stack whose apiserver
// dependency does not compile against the k8s 0.33 line we pin. The one kubelet route
// we serve (logs) is hosted by KubeletServer, which needs none of it.
type Runner struct {
	prov      provider.Provider
	client    kubernetes.Interface
	blocklist Blocklister
	cluster   ClusterReader
	nodeName  string

	// kubelet is the shared endpoint serving `kubectl logs` for every node. Nil is
	// supported: the node then advertises no address, so the API server reports "node does
	// not have an address" instead of hanging on a port nothing serves.
	kubelet *KubeletServer
}

// NewRunner builds the virtual-node runner for one provider. blocklist (Provision
// failures) and kubelet (the log endpoint) are both shared, and both may be nil. cluster is
// how the handler reads policy and placement from the NodePool and NodeClaim instead of from
// the Pod, so a nil one leaves this node unable to provision at all (see Handler.poolFor).
func NewRunner(
	prov provider.Provider, client kubernetes.Interface, blocklist Blocklister,
	kubelet *KubeletServer, cluster ClusterReader,
) *Runner {
	return &Runner{
		prov:      prov,
		client:    client,
		blocklist: blocklist,
		cluster:   cluster,
		nodeName:  NodeName(prov.Name()),
		kubelet:   kubelet,
	}
}

var _ manager.Runnable = (*Runner)(nil)

// Start builds and runs the virtual node until ctx is cancelled. It is invoked
// by the controller-runtime manager.
func (r *Runner) Start(ctx context.Context) error {
	log := logf.FromContext(ctx).WithValues("virtualNode", r.nodeName, "provider", r.prov.Name())

	handler := NewHandler(r.prov, r.client, r.blocklist, r.cluster)
	nodeSpec := nodeSpec(r.nodeName, r.prov.Name())

	// Register on the endpoint AND advertise it, so the API server can proxy `kubectl
	// logs` here. Both together: advertising an endpoint nobody serves makes logs hang
	// instead of failing fast.
	if r.kubelet != nil {
		r.kubelet.Register(r.nodeName, handler)
		nodeSpec.Status.Addresses = r.kubelet.nodeAddress()
		nodeSpec.Status.DaemonEndpoints = r.kubelet.daemonEndpoints()
	}

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
		// All three set EXPLICITLY, because a nil one silently gets a 10 items/s bucket
		// (see podQueueRateLimiter). Each gets its own limiter so the three cannot starve
		// each other: work arrives here from Kubernetes, status arrives from the provider,
		// and the deletes are what a teardown waits on.
		SyncPodsFromKubernetesRateLimiter:    podQueueRateLimiter(),
		SyncPodStatusFromProviderRateLimiter: podQueueRateLimiter(),
		DeletePodsFromKubernetesRateLimiter:  podQueueRateLimiter(),
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
	go pc.Run(ctx, podSyncWorkers) //nolint:errcheck

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
				// hostname is what the scheduler's default topology spread keys off.
				// ProviderLabel routes a Pod here (placement's nodeSelector, paired with
				// the NoSchedule taint of the same key below). ManagedByLabel marks the
				// object as Nebula's for tooling and policies.
				corev1.LabelHostname:          nodeName,
				nebulav1alpha1.ProviderLabel:  providerName,
				nebulav1alpha1.ManagedByLabel: nebulav1alpha1.ManagedByValue,
				// The ROLES column of `kubectl get nodes` comes from these label KEYS
				// alone (the value is ignored, hence ""). "worker" so the node reads as a
				// normal schedulable worker, "nebula" so ours stand out from real ones.
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
				// The VERSION column of `kubectl get nodes`, stamped at build time via
				// -ldflags (see pkg/version); an unstamped build reports "nebula-dev".
				KubeletVersion:  version.Get(),
				OperatingSystem: "linux",
				// Architecture feeds the kubernetes.io/arch label that workloads pin on.
				// A default, not a truth: no instance exists yet at node-creation time,
				// and amd64 is right for every wired provider.
				// TODO: source it from the provider when an arm64 backend (GH200) lands,
				// or a Pod pinning arch=arm64 will fail to schedule here.
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

// nvidiaGPUResource is the extended-resource key GPU Pods may request under. A Nebula Pod
// declares its accelerator on the annotation (which the provider reads), but an author may
// also set nvidia.com/gpu limits for portability — and then the node must advertise the
// resource or the scheduler rejects the Pod before provisioning starts.
const nvidiaGPUResource = "nvidia.com/gpu"

// virtualCapacity is what a virtual node advertises. The numbers are synthetic and
// effectively infinite: there is no real machine here, so capacity's only job is to clear
// the scheduler's fit check and let the Pod bind. The provider enforces the real shape and
// rejects (→ failover) what it cannot satisfy.
//
// TODO: only nvidia.com/gpu is advertised. A provider serving a different accelerator key
// (amd.com/gpu, a typed MIG key) would have its GPU Pods rejected before provisioning;
// when one lands, have Capabilities declare its resource keys and build capacity from
// that.
func virtualCapacity() corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1k"),
		corev1.ResourceMemory: resource.MustParse("10Ti"),
		corev1.ResourcePods:   resource.MustParse("1k"),
		nvidiaGPUResource:     resource.MustParse("1k"),
	}
}
