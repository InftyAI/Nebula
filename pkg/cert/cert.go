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

// Package cert provisions the webhook serving certificate in-process, so Nebula has
// no cert-manager dependency and no out-of-band setup step.
//
// Two things must agree exactly: the TLS keypair the manager serves, and that cert's
// CA in the MutatingWebhookConfiguration's caBundle. Both prior options were worse —
// cert-manager is a second operator to install first (and made the e2e suite need
// network access), while hack/gen-webhook-cert.sh mints a cert at deploy time that
// NEVER rotates, so its expiry is a time bomb that fires years later.
//
// The rotator does both jobs in-process: mint the keypair into a Secret, patch the
// caBundle, keep renewing before expiry. Patching the CA from the cert just written
// is what keeps the served cert and the trusted CA from drifting.
//
// The Secret is the ONLY thing it writes; it never touches the filesystem. The
// keypair reaches certDir because the manager projects that Secret there as a volume,
// and the rotator only polls the path to know when the kubelet has. See certDir —
// getting this backwards (an emptyDir) silently stops the manager from starting.
package cert

import (
	"fmt"

	rotator "github.com/open-policy-agent/cert-controller/pkg/rotator"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	// serviceName is the webhook Service the cert is issued for. The rotator needs
	// it to build the DNS name the API server dials, so it must match
	// config/webhook/service.yaml AFTER kustomize applies the nebula- namePrefix.
	serviceName = "nebula-webhook-service"

	// secretName is the Secret the keypair is stored in. Like the two names above it
	// carries the nebula- prefix, because that is what config/default's namePrefix
	// actually renders ("webhook-server-cert" here would look for a Secret no overlay
	// creates).
	//
	// The Secret MUST already exist when the manager starts, which is why
	// config/webhook ships it empty: the rotator Gets this Secret and then Updates it,
	// but never Creates it, so an absent Secret is a fatal startup error that
	// crash-loops the manager rather than something it recovers from. Empty is all it
	// needs — nil Data is exactly the condition that triggers minting.
	secretName = "nebula-webhook-server-cert"

	// certDir is where the keypair lands on disk and where controller-runtime's
	// webhook server reads it from. It is the controller-runtime default, and the path
	// the manager projects the Secret above at.
	//
	// The rotator does NOT write here, despite the name: CertDir is a path it only
	// os.Stat()s to decide readiness (ensureCertsMounted). The KUBELET puts the files
	// here by projecting the Secret, so the volume must be that Secret, not an emptyDir.
	// With an emptyDir the files never appear, IsReady never closes, and because
	// controller and webhook registration wait on it (see CertsManager), nothing starts
	// while the manager still reports Running.
	certDir = "/tmp/k8s-webhook-server/serving-certs"

	// mutatingWebhookConfName is the MutatingWebhookConfiguration whose caBundle
	// gets patched — the nebula- prefixed name from config/webhook/manifests.yaml.
	//
	// There is deliberately no ValidatingWebhookConfiguration here: Nebula has only
	// the Pod defaulter (see internal/webhook/v1), and Sandbox validation is done
	// with CEL in the CRD rather than a webhook. Naming a config that does not exist
	// would make the rotator fail to patch it on every reconcile.
	mutatingWebhookConfName = "nebula-mutating-webhook-configuration"

	caName = "nebula-ca"
	caOrg  = "nebula"
)

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="admissionregistration.k8s.io",resources=mutatingwebhookconfigurations,verbs=get;list;watch;update

// CertsManager registers the cert rotator with the manager. It closes setupFinish
// once the cert is on disk and the caBundle is patched.
//
// Nothing that depends on the webhook may start before that channel closes: the
// webhook server would otherwise serve on a missing keypair and the API server
// would reject the call, which — with failurePolicy=Fail — means every Pod CREATE
// in the cluster fails admission. The caller runs controller setup in a goroutine
// blocked on this channel (see cmd/main.go).
//
// namespace must be the namespace the manager actually runs in, since it scopes
// both the Secret and the cert's DNS name; it is read from POD_NAMESPACE rather
// than hardcoded so a non-default install namespace still gets a valid cert.
func CertsManager(mgr ctrl.Manager, namespace string, setupFinish chan struct{}) error {
	// The DNS name the API server dials, and therefore the name the cert must be
	// valid for: <service>.<namespace>.svc.
	dnsName := fmt.Sprintf("%s.%s.svc", serviceName, namespace)

	return rotator.AddRotator(mgr, &rotator.CertRotator{
		SecretKey: types.NamespacedName{
			Namespace: namespace,
			Name:      secretName,
		},
		CertDir:        certDir,
		CAName:         caName,
		CAOrganization: caOrg,
		DNSName:        dnsName,
		IsReady:        setupFinish,
		Webhooks: []rotator.WebhookInfo{{
			Type: rotator.Mutating,
			Name: mutatingWebhookConfName,
		}},
		// RequireLeaderElection is deliberately left false. The rotator must run in
		// EVERY replica, not just the leader: CertDir is each pod's own local disk, and a
		// replica that never wrote the keypair there cannot serve the webhook — and
		// webhook serving is not leader-elected, so the API server will call a
		// non-leader. The Secret is the shared source of truth, so replicas after the
		// first find a valid cert there and simply write it to their own disk rather than
		// minting a competing one.
	})
}
