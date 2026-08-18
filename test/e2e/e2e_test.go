//go:build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/InftyAI/Nebula/test/utils"
)

// namespace where the project is deployed in
const namespace = "nebula-system"

// serviceAccountName created for the project
const serviceAccountName = "nebula-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "nebula-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "nebula-metrics-binding"

// Fake-provider placement-flow fixtures. fakeProviderName must match
// fake.ProviderName; fakeVirtualNode is vnode.NodeName(fakeProviderName)
// ("nebula-<provider>"). Kept as literals here so the e2e package stays free of
// production imports.
const (
	fakeProviderName = "fake"
	fakeVirtualNode  = "nebula-fake"
	fakePoolName     = "e2e-fake-pool"
	fakeWorkloadPod  = "e2e-fake-workload"
	// fakeWorkloadNS is a dedicated namespace for the placement-flow workload. It
	// must NOT be the manager namespace: the mutating webhook's namespaceSelector
	// excludes nebula-system (see config/webhook/selector_patch.yaml), so a Pod
	// there would never get the scheduling gate and placement would never run.
	fakeWorkloadNS = "nebula-e2e-workload"
)

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		// No cert step here: the manager mints its own webhook serving cert at
		// startup (pkg/cert) into an emptyDir, so there is nothing to pre-create —
		// the same ordering hack/deploy.sh uses in prod. The cert only exists once
		// the manager is RUNNING, so the assertions below must be Eventually.

		// Deploy via the e2e overlay, which bakes NEBULA_ENABLE_FAKE_PROVIDER=true
		// into the manager env so the in-memory fake provider registers at first
		// boot. This drives the placement→NodeClaim→virtual-node flow without cloud
		// credentials (no real provider registers in a Kind cluster). Baking it in
		// (vs. a post-deploy `kubectl set env`) means the manager boots ONCE — no
		// second rollout, so no leader-election re-acquire window where the metrics
		// endpoint serves before the first reconcile. The fake ships in the binary
		// but only registers on this env var, so production `make deploy` never has it.
		By("deploying the controller-manager (e2e overlay: fake provider enabled)")
		cmd = exec.Command("make", "deploy-e2e", fmt.Sprintf("IMG=%s", projectImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		// No caBundle injection step: the manager patches the
		// MutatingWebhookConfiguration itself once its cert rotator runs (pkg/cert),
		// from the same cert it just wrote. The "CA injection" spec below asserts that
		// happened, so this is covered by an Eventually rather than a deploy step.
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	//
	// ORDER MATTERS: every NodeClaim holds the terminate finalizer, and the only thing
	// that removes it is the NodeClaim controller. Undeploying the manager first strands
	// them — `make uninstall` then blocks forever on a CRD that can never finish deleting
	// (the CRD waits for its CRs; the CRs wait for a controller that is gone), and the
	// external instance behind each claim is leaked, still billing. So claims are drained
	// while the manager is still running.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("cleaning up the fake-provider workload, pool, and namespace")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", fakeWorkloadPod,
			"-n", fakeWorkloadNS, "--ignore-not-found=true"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "nodepool", fakePoolName, "--ignore-not-found=true"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", fakeWorkloadNS, "--ignore-not-found=true"))

		By("cleaning up the sync-benchmark batch, pool, and namespace")
		// The benchmark deletes its own batch, so this only covers the spec failing
		// part-way — a leftover Pod here would keep a claim alive and block the drain
		// below. Pods go with the namespace; the pool is cluster-scoped.
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", perfWorkloadNS, "--ignore-not-found=true"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "nodepool", perfPoolName, "--ignore-not-found=true"))

		By("waiting for NodeClaims to drain while the manager can still terminate instances")
		drained := waitForNodeClaimsGone(2 * time.Minute)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		// Only safe once no claim holds the finalizer. Skipped otherwise, because
		// deleting the CRD now would wedge it in Terminating for good.
		if drained {
			By("uninstalling CRDs")
			cmd = exec.Command("make", "uninstall")
			_, _ = utils.Run(cmd)
		} else {
			fmt.Println("WARNING: NodeClaims did not drain; skipping CRD uninstall to avoid " +
				"a CRD stuck Terminating. Check the provider for leaked instances.")
		}

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				// Get the name of the controller-manager pod
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				// Validate the pod's status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=nebula-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("waiting for the metrics endpoint to be ready")
			verifyMetricsEndpointReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "endpoints", metricsServiceName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("8443"), "Metrics endpoint is not ready")
			}
			Eventually(verifyMetricsEndpointReady).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("controller-runtime.metrics\tServing metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted).Should(Succeed())

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": ["curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics"],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			metricsOutput := getMetricsOutput()
			Expect(metricsOutput).To(ContainSubstring(
				"controller_runtime_reconcile_total",
			))
		})

		It("should have the self-signed webhook serving cert Secret", func() {
			// The manager's own cert rotator (pkg/cert) creates this Secret after it
			// starts — nothing pre-creates it, so this asserts the rotator ran.
			By("validating that the webhook serving cert Secret exists")
			verifyCertSecret := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "secrets", "nebula-webhook-server-cert", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyCertSecret).Should(Succeed())
		})

		It("should have CA injection for mutating webhooks", func() {
			By("checking CA injection for mutating webhooks")
			verifyCAInjection := func(g Gomega) {
				cmd := exec.Command("kubectl", "get",
					"mutatingwebhookconfigurations.admissionregistration.k8s.io",
					"nebula-mutating-webhook-configuration",
					"-o", "go-template={{ range .webhooks }}{{ .clientConfig.caBundle }}{{ end }}")
				mwhOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(len(mwhOutput)).To(BeNumerically(">", 10))
			}
			Eventually(verifyCAInjection).Should(Succeed())
		})

		It("should place an opted-in Pod onto the fake provider's virtual node", func() {
			// This exercises the whole control-plane loop against the in-memory fake
			// provider: webhook gate → placement picks the pool's provider → NodeClaim
			// ledger → the fake's virtual node provisions → the Pod binds and runs.

			By("waiting for the fake provider's virtual node to register")
			verifyNode := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "node", fakeVirtualNode)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "fake virtual node not registered")
			}
			Eventually(verifyNode).Should(Succeed())

			By("creating the workload namespace (webhook-eligible, restricted policy)")
			_, _ = utils.Run(exec.Command("kubectl", "create", "ns", fakeWorkloadNS))
			cmd := exec.Command("kubectl", "label", "--overwrite", "ns", fakeWorkloadNS,
				"pod-security.kubernetes.io/enforce=restricted")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to label the workload namespace")

			By("creating a NodePool that allows the fake provider")
			manifest := fmt.Sprintf(`apiVersion: nebula.inftyai.com/v1alpha1
kind: NodePool
metadata:
  name: %s
spec:
  providers:
  - name: %s
  capacityTypes:
  - OnDemand
  strategy: Ordered
---
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
  labels:
    nebula.inftyai.com/enabled: "true"
    nebula.inftyai.com/nodepool: %s
spec:
  securityContext:
    runAsNonRoot: true
    seccompProfile:
      type: RuntimeDefault
  containers:
  - name: main
    image: registry.k8s.io/pause:3.10
    securityContext:
      allowPrivilegeEscalation: false
      capabilities:
        drop:
        - ALL
`, fakePoolName, fakeProviderName, fakeWorkloadPod, fakeWorkloadNS, fakePoolName)
			manifestFile := filepath.Join("/tmp", "nebula-fake-workload.yaml")
			Expect(os.WriteFile(manifestFile, []byte(manifest), 0o644)).To(Succeed())
			cmd = exec.Command("kubectl", "apply", "-f", manifestFile)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to apply the NodePool + Pod")

			By("verifying the placement controller creates a NodeClaim for the Pod")
			verifyClaim := func(g Gomega) {
				// util.ClaimName(namespace, name) == "<namespace>-<name>".
				claim := fmt.Sprintf("%s-%s", fakeWorkloadNS, fakeWorkloadPod)
				cmd := exec.Command("kubectl", "get", "nodeclaim", claim,
					"-o", "jsonpath={.spec.provider}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "NodeClaim not created")
				g.Expect(out).To(Equal(fakeProviderName), "NodeClaim names the wrong provider")
			}
			Eventually(verifyClaim).Should(Succeed())

			By("verifying the Pod is ungated and bound to the fake virtual node")
			verifyBound := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", fakeWorkloadPod, "-n", fakeWorkloadNS,
					"-o", "jsonpath={.spec.nodeName}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal(fakeVirtualNode), "Pod not bound to the fake virtual node")
			}
			Eventually(verifyBound).Should(Succeed())

			By("cleaning up the fake workload")
			_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", manifestFile, "--ignore-not-found=true"))
		})

		It("should sync a batch of workloads within the time budget", Label("perf"), func() {
			// A benchmark, not a latency SLO: it scales one Deployment to N replicas and
			// reports how long the whole sync path takes per workload, asserting only a
			// loose ceiling so it catches a stalled path without flaking on a busy node.
			//
			// The perf label keeps it OUT of `make test-e2e` (which filters '!perf') and
			// is what `make test-perf` selects, since the batch is slow enough that it
			// does not belong in every e2e run. See perf_test.go.
			benchmarkWorkloadSync()
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		// TODO: Customize the e2e test suite with scenarios specific to your project.
		// Consider applying sample/CR(s) and check their status and/or verifying
		// the reconciliation by using the metrics, i.e.:
		// metricsOutput := getMetricsOutput()
		// Expect(metricsOutput).To(ContainSubstring(
		//    fmt.Sprintf(`controller_runtime_reconcile_total{controller="%s",result="success"} 1`,
		//    strings.ToLower(<Kind>),
		// ))
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	// Temporary file to store the token request
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		// Execute kubectl command to create the token
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		// Parse the JSON output to extract the token
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() string {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	metricsOutput, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
	Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
	return metricsOutput
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}

// waitForControllerReady blocks until the manager can serve the whole path a new Pod
// takes: the Deployment reports Available, the mutating webhook carries the CA bundle the
// manager mints for itself, and the fake provider's virtual node reports Ready.
//
// It exists because `make test-perf` filters the suite to the perf label, so the specs that
// assert the manager came up ("should run successfully", "should have CA injection") never
// run — and a batch applied against a half-started manager is not a failed run, it is a
// WRONG one. Both gaps inflate a stage that then reads as Nebula being slow:
//
//   - the webhook is failurePolicy=Fail on Pod CREATE, so until pkg/cert has patched its
//     caBundle from inside the manager, every replica create is REJECTED and the ReplicaSet
//     retries with backoff. That lands in "Pods creation".
//   - the Node object exists a moment before the node controller marks it Ready, and while
//     it is NotReady the node lifecycle controller taints it NoSchedule, so an ungated Pod
//     cannot bind. That lands in "Pods bound".
//
// Waiting on the CONDITION rather than on the pod phase for the first check: Running is
// true of a manager whose probes have not passed and which has not won leader election, and
// leader election is what gates every reconcile the benchmark measures.
func waitForControllerReady() {
	By("waiting for the controller-manager Deployment to report Available")
	// By label, not by name, so this does not have to know the kustomize name prefix. An
	// Eventually over `get` rather than `kubectl wait`, because `wait` on a selector that
	// matches nothing yet fails outright instead of retrying.
	Eventually(func(g Gomega) {
		out, err := utils.Run(exec.Command("kubectl", "get", "deployment",
			"-l", "control-plane=controller-manager", "-n", namespace,
			"-o", "go-template={{range .items}}{{range .status.conditions}}"+
				"{{if eq .type \"Available\"}}{{.status}}{{end}}{{end}}{{end}}"))
		g.Expect(err).NotTo(HaveOccurred(), "Failed to read the controller-manager Deployment")
		g.Expect(out).To(Equal("True"), "the controller-manager is not Available yet")
	}).Should(Succeed())

	By("waiting for the mutating webhook's CA bundle to be injected")
	Eventually(func(g Gomega) {
		out, err := utils.Run(exec.Command("kubectl", "get",
			"mutatingwebhookconfigurations.admissionregistration.k8s.io",
			"nebula-mutating-webhook-configuration",
			"-o", "go-template={{range .webhooks}}{{.clientConfig.caBundle}}{{end}}"))
		g.Expect(err).NotTo(HaveOccurred())
		// Same threshold as the CA-injection spec: enough to tell a real bundle from an
		// empty field, without pinning the cert's size.
		g.Expect(len(out)).To(BeNumerically(">", 10), "the webhook caBundle is not injected yet")
	}).Should(Succeed())

	By("waiting for the fake provider's virtual node to report Ready")
	Eventually(func(g Gomega) {
		out, err := utils.Run(exec.Command("kubectl", "get", "node", fakeVirtualNode,
			"-o", "go-template={{range .status.conditions}}"+
				"{{if eq .type \"Ready\"}}{{.status}}{{end}}{{end}}"))
		g.Expect(err).NotTo(HaveOccurred(), "fake virtual node not registered")
		g.Expect(out).To(Equal("True"), "fake virtual node registered but not Ready")
	}).Should(Succeed())
}

// waitForNodeClaimsGone polls until no NodeClaim is left, reporting whether they
// drained within timeout. A leftover claim means its terminate finalizer never ran, so
// the caller must not delete the CRD (it would wedge) and an instance may be leaking.
//
// Deliberately a plain poll rather than Eventually: a failed assertion here would abort
// the rest of AfterAll, skipping the undeploy it is trying to protect.
func waitForNodeClaimsGone(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		out, err := utils.Run(exec.Command("kubectl", "get", "nodeclaims",
			"-o", "go-template={{range .items}}{{.metadata.name}}{{\"\\n\"}}{{end}}"))
		// A missing CRD counts as drained: there is nothing left to hold a finalizer.
		if err != nil || len(utils.GetNonEmptyLines(out)) == 0 {
			return true
		}
		if time.Now().After(deadline) {
			fmt.Printf("NodeClaims still present after %s:\n%s", timeout, out)
			return false
		}
		time.Sleep(2 * time.Second)
	}
}
