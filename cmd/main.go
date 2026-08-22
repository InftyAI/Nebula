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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	corev1 "k8s.io/api/core/v1"

	"k8s.io/client-go/kubernetes"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/internal/controller"
	webhookv1 "github.com/InftyAI/Nebula/internal/webhook/v1"
	nebulacert "github.com/InftyAI/Nebula/pkg/cert"
	"github.com/InftyAI/Nebula/pkg/failover"
	"github.com/InftyAI/Nebula/pkg/provider"
	awsprovider "github.com/InftyAI/Nebula/pkg/provider/aws"
	"github.com/InftyAI/Nebula/pkg/provider/fake"
	"github.com/InftyAI/Nebula/pkg/provider/modal"
	"github.com/InftyAI/Nebula/pkg/vnode"
	// +kubebuilder:scaffold:imports
)

// defaultNamespace is where the manager is installed by config/default. It is only
// a fallback for managerNamespace when POD_NAMESPACE is unset.
const defaultNamespace = "nebula-system"

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(nebulav1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var kubeletAddr, kubeletClientCA string
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.StringVar(&kubeletAddr, "kubelet-bind-address", vnode.DefaultKubeletAddr,
		"The address the virtual nodes' kubelet API (container logs, i.e. `kubectl logs`) binds to. "+
			"Set to \"\" to disable it, which makes logs unsupported.")
	flag.StringVar(&kubeletClientCA, "kubelet-client-ca", "",
		"PEM bundle of CAs whose client certificates may call the kubelet API. Empty (the default) "+
			"serves TLS without client verification, because which CA signs the API server's kubelet "+
			"client certificate is not portable across distributions; restrict the port with a "+
			"NetworkPolicy, or set this to your API server's kubelet client CA.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Create watchers for metrics and webhooks certificates
	var metricsCertWatcher, webhookCertWatcher *certwatcher.CertWatcher

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		var err error
		webhookCertWatcher, err = certwatcher.New(
			filepath.Join(webhookCertPath, webhookCertName),
			filepath.Join(webhookCertPath, webhookCertKey),
		)
		if err != nil {
			setupLog.Error(err, "Failed to initialize webhook certificate watcher")
			os.Exit(1)
		}

		webhookTLSOpts = append(webhookTLSOpts, func(config *tls.Config) {
			config.GetCertificate = webhookCertWatcher.GetCertificate
		})
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: webhookTLSOpts,
	})

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		var err error
		metricsCertWatcher, err = certwatcher.New(
			filepath.Join(metricsCertPath, metricsCertName),
			filepath.Join(metricsCertPath, metricsCertKey),
		)
		if err != nil {
			setupLog.Error(err, "to initialize metrics certificate watcher", "error", err)
			os.Exit(1)
		}

		metricsServerOptions.TLSOpts = append(metricsServerOptions.TLSOpts, func(config *tls.Config) {
			config.GetCertificate = metricsCertWatcher.GetCertificate
		})
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "nebula.inftyai.com",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Provision the webhook serving cert in-process, replacing both cert-manager and
	// the out-of-band hack/gen-webhook-cert.sh. The rotator mints the keypair into a
	// Secret, writes it where the webhook server reads it, patches the caBundle into
	// the MutatingWebhookConfiguration, and keeps renewing it before expiry — the one
	// thing neither prior approach did (the script's cert simply expired years later).
	//
	// certsReady closes once the cert is on disk AND the caBundle is patched. The
	// webhook must not register before that: with failurePolicy=Fail, serving on a
	// missing keypair means every Pod CREATE in the cluster fails admission.
	certsReady := make(chan struct{})
	if enableWebhooks() {
		if err := nebulacert.CertsManager(mgr, managerNamespace(), certsReady); err != nil {
			setupLog.Error(err, "unable to set up cert rotation")
			os.Exit(1)
		}
	} else {
		// Nothing will close the channel, so close it here or the goroutine below would
		// block forever and no controller would ever start.
		close(certsReady)
	}

	// Register provider backends into the process-wide registry that both
	// reconcilers resolve through (their Providers field defaults to
	// provider.Get). Done before SetupWithManager so a pool/claim reconciled at
	// startup already sees its provider. The manager's client backs the AWS region
	// source (regions are read from NodePools at call time, not env), so it is
	// threaded in; the client is only queried at runtime, after the cache has synced.
	registerProviders(context.Background(), mgr.GetClient())

	// One shared failover blocklist, written by the virtual kubelet handlers on a
	// Provision failure and read by the placement controller to skip a candidate
	// that just failed (zone → region → tier). It is in-memory, process-wide state
	// (empty on restart, self-refilling), so a single instance is threaded into
	// both sides rather than persisted.
	blocklist := failover.New()

	// The kubelet endpoint for `kubectl logs` — one listener shared by every provider's
	// node, hence built here rather than in setupVirtualNodes. Nil is supported: the
	// nodes then advertise no address, and logs report NotFound.
	kubeletSrv := setupKubeletServer(mgr, kubeletAddr, kubeletClientCA)

	// Controller and webhook registration is deferred until the cert exists, so it
	// runs in a goroutine: the cert cannot be minted until the manager is STARTED
	// (the rotator is a Runnable and needs a synced cache), so blocking here would
	// deadlock. controller-runtime supports Add after Start — a Runnable registered
	// then is started immediately — which is what makes this safe.
	//
	// The controllers wait too, not just the webhook. They create Pods, and every Pod
	// CREATE goes through the defaulting webhook that injects the provider-selection
	// gate; a Pod created while that webhook is untrusted would either be rejected
	// (failurePolicy=Fail) or, worse, admitted ungated and scheduled by vanilla
	// Kubernetes — silently bypassing placement and never reaching a provider.
	go func() {
		setupLog.Info("waiting for the webhook certificate to be ready")
		<-certsReady
		setupLog.Info("webhook certificate ready")

		if err := setupControllers(mgr, blocklist, kubeletSrv); err != nil {
			setupLog.Error(err, "unable to set up controllers")
			os.Exit(1)
		}
	}()

	if metricsCertWatcher != nil {
		setupLog.Info("Adding metrics certificate watcher to manager")
		if err := mgr.Add(metricsCertWatcher); err != nil {
			setupLog.Error(err, "unable to add metrics certificate watcher to manager")
			os.Exit(1)
		}
	}

	if webhookCertWatcher != nil {
		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(webhookCertWatcher); err != nil {
			setupLog.Error(err, "unable to add webhook certificate watcher to manager")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// enableWebhooks reports whether the Pod defaulting webhook (and therefore the cert
// rotator that makes it trustable) should run. It is off only when explicitly
// disabled, which is how the local `make run` and tests avoid needing a cert and a
// reachable Service.
func enableWebhooks() bool {
	// nolint:goconst
	return os.Getenv("ENABLE_WEBHOOKS") != "false"
}

// managerNamespace is the namespace the manager runs in, which scopes both the
// webhook cert Secret and the cert's DNS name. It is read from POD_NAMESPACE
// (projected via fieldRef in config/manager/manager.yaml) rather than hardcoded, so
// an install into a non-default namespace still gets a cert the API server accepts.
// The fallback only matters for an out-of-cluster run, where the webhook is
// typically disabled anyway.
func managerNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	setupLog.Info("POD_NAMESPACE is unset; falling back to the default install namespace",
		"namespace", defaultNamespace)
	return defaultNamespace
}

// setupControllers registers every controller, the virtual nodes and the webhook.
// It runs only after the webhook serving cert is ready (see main), which is why it
// is a function rather than inline: everything here depends on Pod admission
// working, so none of it may be registered before the API server trusts the webhook.
func setupControllers(mgr ctrl.Manager, blocklist *failover.Blocklist, kubeletSrv *vnode.KubeletServer) error {
	if err := (&controller.NodePoolReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create NodePool controller: %w", err)
	}
	if err := (&controller.NodeClaimReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create NodeClaim controller: %w", err)
	}
	if err := (&controller.PodPlacementReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Blocklist: blocklist,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create PodPlacement controller: %w", err)
	}

	// The workload controllers sit on top of the provisioning core above: each
	// synthesizes objects onto the same placement path rather than talking to a
	// provider itself. Sandbox produces the Pod that backs one remote box;
	// SandboxSet produces Sandboxes.
	if err := (&controller.SandboxReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create Sandbox controller: %w", err)
	}
	if err := (&controller.SandboxSetReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create SandboxSet controller: %w", err)
	}

	// Start one virtual node per registered provider. The virtual kubelet owns
	// provisioning: its pod controller calls provider.Provision on CreatePod and
	// provider.Terminate on DeletePod, so an ungated Pod bound to a provider's
	// virtual node materializes an external instance. Each Runner is a
	// manager.Runnable, so it shares the manager's lifecycle and leader election.
	if err := setupVirtualNodes(mgr, blocklist, kubeletSrv); err != nil {
		return fmt.Errorf("unable to set up virtual nodes: %w", err)
	}
	if enableWebhooks() {
		if err := webhookv1.SetupPodWebhookWithManager(mgr); err != nil {
			return fmt.Errorf("unable to create Pod webhook: %w", err)
		}
	}
	// +kubebuilder:scaffold:builder
	return nil
}

// setupKubeletServer builds the shared endpoint that serves `kubectl logs` for Pods
// on the virtual nodes, and adds it to the manager.
//
// Returns nil — logs unsupported, nodes advertising no address — rather than failing
// the process, when addr is empty (turned off) or POD_IP is missing. That address is
// what the API server dials and nothing substitutes for it: a Service would balance to
// a non-leader replica, which holds no tracked Pods. Either way only logs degrade, so
// it is logged loudly and the manager carries on.
func setupKubeletServer(mgr ctrl.Manager, addr, clientCA string) *vnode.KubeletServer {
	if addr == "" {
		setupLog.Info("kubelet API disabled by configuration; `kubectl logs` will not work for Nebula pods")
		return nil
	}
	podIP := vnode.PodIPFromEnv()
	if podIP == "" {
		setupLog.Info("POD_IP is not set; serving no kubelet API, so `kubectl logs` will not work " +
			"for Nebula pods (project it with a fieldRef — see config/manager)")
		return nil
	}
	srv, err := vnode.NewKubeletServer(podIP, addr, clientCA)
	if err != nil {
		// A real misconfiguration, but only of the log path: fail the feature, not the
		// manager.
		setupLog.Error(err, "unable to set up the kubelet API; `kubectl logs` will not work for Nebula pods")
		return nil
	}
	if err := mgr.Add(srv); err != nil {
		setupLog.Error(err, "unable to add the kubelet API to the manager")
		return nil
	}
	setupLog.Info("kubelet API enabled", "addr", addr, "advertisedIP", podIP, "clientCertRequired", clientCA != "")
	return srv
}

// setupVirtualNodes adds a vnode.Runner to the manager for every registered
// provider. The Runner needs a typed clientset (the virtual kubelet's node/pod
// controllers use client-go directly, not the controller-runtime client), built
// from the same rest.Config the manager uses. kubeletSrv is the shared kubelet API
// each node advertises for logs; nil disables it.
func setupVirtualNodes(mgr ctrl.Manager, blocklist vnode.Blocklister, kubeletSrv *vnode.KubeletServer) error {
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return err
	}
	for _, name := range provider.Names() {
		prov, ok := provider.Get(name)
		if !ok {
			continue
		}
		// The manager's client, so the pool and claim reads on the provisioning path hit the
		// shared cache the controllers already keep warm.
		cluster := vnode.NewCachedClusterReader(mgr.GetClient())
		if err := mgr.Add(vnode.NewRunner(prov, clientset, blocklist, kubeletSrv, cluster)); err != nil {
			return err
		}
		setupLog.Info("registered virtual node", "provider", name, "node", vnode.NodeName(name))
	}
	return nil
}

// registerProviders wires the compiled-in provider adapters into the registry.
// A provider whose credentials are absent (e.g. Modal creds not mounted in a
// dev cluster) is logged and skipped rather than fatal: the control plane must
// still run for the providers that ARE configured, and a pool referencing an
// unregistered provider surfaces as a clear NodePool condition rather than a
// crash loop.
func registerProviders(ctx context.Context, c client.Client) {
	if p, err := modal.NewSDKClient(ctx, os.Getenv("MODAL_APP_NAME")); err != nil {
		setupLog.Info("skipping Modal provider registration", "reason", err.Error())
	} else {
		provider.Register(p)
		setupLog.Info("registered provider", "provider", p.Name())
	}

	// AWS. There is NO region env/flag: the regions this provider may use are declared
	// per-pool in the NodePool (ProviderSpec.Regions) and read at call time via the
	// region source below, so a pool added at runtime widens the fan-out without a
	// restart. One AWS provider spans every such region (per-region clients are built
	// lazily). The adapter is otherwise self-configuring: it resolves each region's
	// GPU AMI and default-VPC subnets itself, so no launch template or pre-created
	// infra is needed. Credentials are secrets and are NEVER read here: the SDK client
	// uses the default credential chain (IRSA / instance-role / AWS_ACCESS_KEY_ID
	// delivered via a Secret), and one account-global credential authorizes every
	// region. Registration only fails (and is a non-fatal skip) if the price catalog
	// cannot load — region config can no longer make it fail.
	if p, err := awsprovider.NewSDKClient(ctx, awsRegionSource(c)); err != nil {
		setupLog.Info("skipping AWS provider registration", "reason", err.Error())
	} else {
		provider.Register(p)
		setupLog.Info("registered provider", "provider", p.Name())
	}

	// The fake provider is an in-memory backend used only by the e2e suite to
	// exercise the full control-plane loop without cloud credentials. It ships in
	// the binary but registers ONLY when explicitly enabled, so it can never place
	// real workloads in production.
	if os.Getenv("NEBULA_ENABLE_FAKE_PROVIDER") == "true" {
		p := fake.New()
		provider.Register(p)
		setupLog.Info("registered provider", "provider", p.Name())
	}
}

// awsRegionSource returns the AWS adapter's RegionSource: the union of
// ProviderSpec.Regions across every NodePool referencing the "aws" provider. No
// env/flag needed — regions are the operator's per-pool declaration — and editing a
// pool widens the swept set on the next List tick without a restart.
//
// Evaluated per List/Offerings tick, served from the manager's informer cache (no API
// call), so the O(pools) scan is cheap; sweepRegions dedupes. On a list error (cache
// not synced yet, a transient failure) it returns nil and sweepRegions falls back to
// the regions already provisioned into. Uses a background context, since it runs long
// after registration returns.
func awsRegionSource(c client.Client) awsprovider.RegionSource {
	return func() []string {
		var pools nebulav1alpha1.NodePoolList
		if err := c.List(context.Background(), &pools); err != nil {
			setupLog.V(1).Info("aws region source: list NodePools failed; sweeping provisioned regions only",
				"reason", err.Error())
			return nil
		}
		var regions []string
		for i := range pools.Items {
			for _, ps := range pools.Items[i].Spec.Providers {
				if ps.Name == provider.ProviderAWS {
					// Expand PER POOL, before unioning. ProviderSpec.Regions is a
					// constraint, not a list: an omitted one means "every region", and
					// unioning the raw lists first would collapse that to "nothing" —
					// the swept set would miss regions placement provisions into, and
					// List's absence is reported as Terminated on live instances.
					// This is the same expansion regionsFor applies on the placement
					// side; both must agree, so both call this one function.
					regions = append(regions, awsprovider.ExpandRegions(ps.Regions)...)
				}
			}
		}
		return regions
	}
}
