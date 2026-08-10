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
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

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

	// sanddcontroller, not controller: the name is already taken by Nebula's own
	// internal/controller below.
	sanddcontroller "github.com/InftyAI/SandD/go/controller"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/internal/controller"
	webhookv1 "github.com/InftyAI/Nebula/internal/webhook/v1"
	nebulacert "github.com/InftyAI/Nebula/pkg/cert"
	"github.com/InftyAI/Nebula/pkg/failover"
	"github.com/InftyAI/Nebula/pkg/provider"
	awsprovider "github.com/InftyAI/Nebula/pkg/provider/aws"
	"github.com/InftyAI/Nebula/pkg/provider/fake"
	"github.com/InftyAI/Nebula/pkg/provider/modal"
	"github.com/InftyAI/Nebula/pkg/sandd"
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

	// Both halves of SandD auth, resolved HERE, before the manager starts: a bad or
	// absent key must be a startup failure rather than a per-Provision error discovered
	// later (every instance provisioned without a token is one whose daemon can never
	// dial in, and it bills until someone notices), and setupControllers runs in a
	// goroutine where the only way to report a failure is os.Exit — a misconfiguration
	// there would surface as a mid-flight crash instead of a startup error.
	sanddCfg, err := setupSandD()
	if err != nil {
		setupLog.Error(err, "unable to set up SandD daemon authentication")
		os.Exit(1)
	}

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

		if err := setupControllers(mgr, blocklist, sanddCfg); err != nil {
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

// SandD signer configuration, all from the environment so the key can be rotated
// and the TTL tuned without a rebuild:
//
//	SANDD_SIGNING_KEY_PATH  PKCS#8 Ed25519 private key (mounted from a Secret).
//	                        UNSET = SandD token minting is OFF.
//	SANDD_EXTERNAL_HOST     the externally reachable address of this manager's dial-in
//	                        port, e.g. "sandd.example.com" or a full
//	                        "wss://sandd.example.com/ws". REQUIRED when the key path
//	                        is set; see setupSandD for why it cannot be defaulted.
//	SANDD_SIGNING_KID       key id stamped in the JWT header, so verifiers can hold
//	                        several public keys during a rotation (default "default").
//	SANDD_TOKEN_ISSUER      the `iss` claim (default "nebula").
//	SANDD_TOKEN_TTL         token lifetime, any time.ParseDuration form (default 24h).
const (
	defaultSandDSigningKID = "default"
	defaultSandDIssuer     = "nebula"
	defaultSandDTokenTTL   = 24 * time.Hour

	// sanddWebSocketPath is the path the controller serves its dial-in WebSocket on
	// (axum route "/ws" in the SandD repo's server/). Appended when the operator gives
	// a bare host, so the common configuration is just a hostname.
	sanddWebSocketPath = "/ws"
)

// setupSandD builds what the virtual kubelet needs to hand each provisioned instance
// a working dial-in: the endpoint its daemon connects to, and the minter that signs
// the token it presents there.
//
// Both travel in one *vnode.SandD because they are ONE feature and must switch as
// one — a token with no reachable address cannot be used, and an address with no
// token is refused by the controller. The single switch is SANDD_SIGNING_KEY_PATH:
// unset returns (nil, nil) and SandD is off, so a cluster with no key Secret
// provisions as before, its instances simply getting no token.
//
// Every OTHER misconfiguration is an error, not a fallback: a key path that is set
// but unreadable, a TTL that does not parse, a key that cannot be marshalled, or a
// MISSING EXTERNAL HOST. Each means someone intended SandD to be on and got it
// wrong, and silently degrading would provision a fleet of instances whose daemons
// can never dial in — billing with no control surface. That is the failure this
// fails fast to avoid.
//
// SANDD_EXTERNAL_HOST is required for exactly that reason. It cannot be derived: this
// manager's in-cluster Service name is not resolvable or routable from a provider's
// network, so any value this process could invent would be an address no daemon can
// reach. Only the operator knows the ingress/LB name that fronts it.
//
// Returning the concrete *vnode.SandD rather than a bare interface also sidesteps a
// trap worth naming: an interface holding a NIL POINTER is not == nil (an interface
// value is a (type, value) pair, so assigning a typed nil fills the type half).
// pkg/vnode decides the feature is off via `h.sandd == nil`, and producing the nil
// HERE — on the one path where the key is absent — means no caller can reintroduce
// that trap.
func setupSandD() (*vnode.SandD, error) {
	keyPath := os.Getenv("SANDD_SIGNING_KEY_PATH")
	if keyPath == "" {
		setupLog.Info("SANDD_SIGNING_KEY_PATH is unset; SandD daemon authentication is disabled")
		return nil, nil
	}

	// Required, not defaulted. See the doc comment: a guessed address is worse than
	// no feature, because it provisions billable instances that can never connect.
	host := os.Getenv("SANDD_EXTERNAL_HOST")
	if host == "" {
		return nil, fmt.Errorf(
			"SANDD_EXTERNAL_HOST is required when SANDD_SIGNING_KEY_PATH is set: it is the " +
				"externally reachable address of this manager's SandD dial-in port (e.g. " +
				"sandd.example.com), which daemons on provider instances dial. It cannot be " +
				"derived from the cluster because the manager's Service name is not " +
				"resolvable outside it")
	}

	kid := os.Getenv("SANDD_SIGNING_KID")
	if kid == "" {
		kid = defaultSandDSigningKID
	}
	issuer := os.Getenv("SANDD_TOKEN_ISSUER")
	if issuer == "" {
		issuer = defaultSandDIssuer
	}
	ttl := defaultSandDTokenTTL
	if raw := os.Getenv("SANDD_TOKEN_TTL"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("parsing SANDD_TOKEN_TTL %q: %w", raw, err)
		}
		ttl = parsed
	}

	signer, err := sandd.NewSigner(keyPath, kid, issuer, ttl)
	if err != nil {
		return nil, err
	}

	endpoint, err := sanddEndpoint(host)
	if err != nil {
		return nil, err
	}

	// The verification material is DERIVED from the signing key, not configured. Both
	// halves now live in one process, so the public key, kid and issuer cannot disagree
	// with what is actually signing — which removes the failure that used to take the
	// whole fleet down at once (a public key published through a second channel that had
	// drifted from the private one, so every daemon failed to authenticate together).
	publicKey, err := signer.PublicKeyPEM()
	if err != nil {
		return nil, err
	}

	// Start the controller daemons dial into, IN THIS PROCESS. There is no Deployment,
	// Service or public-key ConfigMap: a daemon's connection is a live socket owned by
	// whichever process accepted it, so hosting it here is what lets the virtual kubelet
	// reach back into a workload directly instead of asking a second process to relay.
	//
	// It binds ALL interfaces on SanddControllerPort — daemons reach it from outside the
	// cluster through the operator's edge, which terminates TLS and forwards here.
	srv, err := sanddcontroller.Start(sanddcontroller.Config{
		Bind:         fmt.Sprintf("0.0.0.0:%d", nebulav1alpha1.SanddControllerPort),
		PublicKeyPEM: publicKey,
		ControllerID: nebulav1alpha1.SanddControllerAudience,
		Issuer:       issuer,
		KID:          kid,
	})
	if err != nil {
		return nil, fmt.Errorf("starting the embedded SandD controller: %w", err)
	}

	// Deliberately never closed: it must outlive every exec session and live as long as
	// the process. Releasing the port at shutdown is the OS's job, and closing it early
	// would tear down live shells for no gain.
	//
	// Log the metadata and the endpoint (public), but NEVER the key or any token it
	// mints. The endpoint is worth logging: "which address did we tell daemons to
	// dial" is the first question when none of them connect.
	setupLog.Info("SandD enabled with an embedded controller",
		"kid", kid, "issuer", issuer, "ttl", ttl.String(), "endpoint", endpoint,
		"listenPort", nebulav1alpha1.SanddControllerPort,
		"audience", nebulav1alpha1.SanddControllerAudience)
	return &vnode.SandD{
		Endpoint: endpoint,
		Minter:   signer,
		Execer:   vnode.EmbeddedExecer{Server: srv},
	}, nil
}

// sanddEndpoint turns the operator's SANDD_EXTERNAL_HOST into the WebSocket URL a
// daemon dials.
//
// The host is accepted in the forms an operator actually types — "sandd.example.com",
// "sandd.example.com:8765", or a full "wss://sandd.example.com/ws" — and normalized
// to one URL, because a bare host is the obvious thing to write and silently
// mishandling a scheme someone added is a miserable way to lose an afternoon.
//
// wss:// is the default and http(s) is rejected outright: the daemon dials a
// WebSocket, TLS terminates at the edge, and ws:// (plaintext, opt-in) exists only
// for a local/dev edge that does not terminate TLS. A plaintext default would ship
// bearer tokens in the clear.
//
// No port is appended for wss://: the edge listens on 443 and forwards to the
// fleet's SanddControllerPort in-cluster, so the port the controller binds is not
// the port a daemon names.
func sanddEndpoint(host string) (string, error) {
	h := strings.TrimSpace(host)

	// Already a URL: validate the scheme and keep the operator's path.
	if i := strings.Index(h, "://"); i >= 0 {
		scheme := h[:i]
		switch scheme {
		case "wss", "ws":
		default:
			return "", fmt.Errorf(
				"SANDD_EXTERNAL_HOST %q has scheme %q: daemons dial a WebSocket, so use wss:// "+
					"(or ws:// for a local edge that does not terminate TLS)", host, scheme)
		}
		u, err := url.Parse(h)
		if err != nil {
			return "", fmt.Errorf("parsing SANDD_EXTERNAL_HOST %q: %w", host, err)
		}
		if u.Host == "" {
			return "", fmt.Errorf("SANDD_EXTERNAL_HOST %q names no host", host)
		}
		// An explicit path is honoured as-is; only a bare/rooted URL gets /ws, so an
		// edge that mounts the controller under a prefix can say so.
		if u.Path == "" || u.Path == "/" {
			u.Path = sanddWebSocketPath
		}
		return u.String(), nil
	}

	// A bare host (optionally with a port). Reject anything with a path or stray
	// slashes rather than guessing what was meant.
	if strings.ContainsAny(h, "/ ") {
		return "", fmt.Errorf(
			"SANDD_EXTERNAL_HOST %q is neither a bare host nor a ws(s):// URL", host)
	}
	if h == "" {
		return "", fmt.Errorf("SANDD_EXTERNAL_HOST is empty")
	}
	return "wss://" + h + sanddWebSocketPath, nil
}

// setupControllers registers every controller, the virtual nodes and the webhook.
// It runs only after the webhook serving cert is ready (see main), which is why it
// is a function rather than inline: everything here depends on Pod admission
// working, so none of it may be registered before the API server trusts the webhook.
func setupControllers(
	mgr ctrl.Manager,
	blocklist *failover.Blocklist,
	sanddCfg *vnode.SandD,
) error {
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
		// No SandD field: the controller runs in this process, so placement creates
		// nothing for it and needs none of its configuration. The dial-in credentials
		// are assembled per instance by the virtual kubelet below.
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
	if err := setupVirtualNodes(mgr, blocklist, sanddCfg); err != nil {
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

// setupVirtualNodes adds a vnode.Runner to the manager for every registered
// provider. The Runner needs a typed clientset (the virtual kubelet's node/pod
// controllers use client-go directly, not the controller-runtime client), built
// from the same rest.Config the manager uses.
func setupVirtualNodes(mgr ctrl.Manager, blocklist vnode.Blocklister, sanddCfg *vnode.SandD) error {
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return err
	}
	for _, name := range provider.Names() {
		prov, ok := provider.Get(name)
		if !ok {
			continue
		}
		if err := mgr.Add(vnode.NewRunner(prov, clientset, blocklist, sanddCfg)); err != nil {
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
// ProviderSpec.Regions across every NodePool that references the "aws" provider. It
// needs no env/flag — regions are the operator's per-pool declaration — and a pool
// added/edited at runtime widens the swept set on the next List tick, no restart
// required.
//
// It is evaluated on each List/Offerings tick. The underlying List is served from
// the manager's informer cache (no API call), so scanning the pools per tick is
// cheap even though it is O(pools); sweepRegions dedupes the result. On a list error
// (cache not yet synced at startup, a transient failure) it returns nil and
// sweepRegions falls back to the regions already provisioned into. It uses a
// background context, not the registration ctx, since it runs long after
// registration returns.
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
					regions = append(regions, ps.Regions...)
				}
			}
		}
		return regions
	}
}
