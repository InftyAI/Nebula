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

// This suite deploys and undeploys a real installation, so it is behind a build tag:
// a plain `go test ./...` must not be able to pick it up and tear down a live cluster.
// Run it with `make test-e2e`.
package e2e

import (
	"fmt"
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/InftyAI/Nebula/test/utils"
)

// projectImage is the name of the image which will be build and loaded
// with the code source changes to be tested.
const projectImage = "example.com/nebula:v0.0.1"

// NOTE: there is deliberately no cert-manager setup here. The manager provisions its
// own webhook serving cert in-process (pkg/cert), so this suite needs neither a
// cert-manager install nor the network access to fetch its manifests — which is what
// used to make BeforeSuite fail on a machine without them.

// TestE2E runs the end-to-end (e2e) test suite for the project. These tests execute in an isolated,
// temporary environment to validate project changes with the purpose of being used in CI jobs.
// The default setup requires Kind and builds/loads the Manager Docker image locally.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting nebula integration test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	// FIRST, before anything can reach a cluster: pin every command this suite runs to
	// the throwaway kind cluster's own kubeconfig. Otherwise they follow the ambient
	// current-context, and this suite deploys, undeploys, and deletes namespaces — on
	// whatever the developer happened to be pointed at.
	By("pinning the suite to its own kind cluster")
	Expect(utils.UseKindKubeconfig()).To(Succeed(), "Failed to pin the suite to a kind cluster")

	By("building the manager(Operator) image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", projectImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager(Operator) image")

	// TODO(user): If you want to change the e2e test vendor from Kind, ensure the image is
	// built and available before running the tests. Also, remove the following block.
	By("loading the manager(Operator) image on Kind")
	err = utils.LoadImageToKindClusterWithName(projectImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager(Operator) image into Kind")
})

// AfterSuite unpins last, so no command can fall back to the ambient context while
// any cleanup is still running.
var _ = AfterSuite(func() {
	utils.ReleaseKindKubeconfig()
})
