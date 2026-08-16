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

package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2" // nolint:revive,staticcheck
)

const (
	prometheusOperatorVersion = "v0.77.1"
	prometheusOperatorURL     = "https://github.com/prometheus-operator/prometheus-operator/" +
		"releases/download/%s/bundle.yaml"
)

func warnError(err error) {
	_, _ = fmt.Fprintf(GinkgoWriter, "warning: %v\n", err)
}

// Run executes the provided command within this context
func Run(cmd *exec.Cmd) (string, error) {
	// Isolate the working directory to the command via cmd.Dir rather than
	// os.Chdir: os.Chdir mutates the process-wide cwd, which is not goroutine-safe
	// and would corrupt parallel commands (and anything else relying on cwd).
	dir, err := GetProjectDir()
	if err != nil {
		return "", fmt.Errorf("get project dir: %w", err)
	}
	cmd.Dir = dir

	cmd.Env = append(os.Environ(), "GO111MODULE=on")
	// Pin the target cluster when the suite has one (see UseKindKubeconfig). Appended
	// last so it wins over any inherited KUBECONFIG.
	if kubeconfigPath != "" {
		cmd.Env = append(cmd.Env, "KUBECONFIG="+kubeconfigPath)
	}
	command := strings.Join(cmd.Args, " ")
	_, _ = fmt.Fprintf(GinkgoWriter, "running: %q\n", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%q failed with error %q: %w", command, string(output), err)
	}

	return string(output), nil
}

// InstallPrometheusOperator installs the prometheus Operator to be used to export the enabled metrics.
func InstallPrometheusOperator() error {
	url := fmt.Sprintf(prometheusOperatorURL, prometheusOperatorVersion)
	cmd := exec.Command("kubectl", "create", "-f", url)
	_, err := Run(cmd)
	return err
}

// UninstallPrometheusOperator uninstalls the prometheus
func UninstallPrometheusOperator() {
	url := fmt.Sprintf(prometheusOperatorURL, prometheusOperatorVersion)
	cmd := exec.Command("kubectl", "delete", "-f", url)
	if _, err := Run(cmd); err != nil {
		warnError(err)
	}
}

// IsPrometheusCRDsInstalled checks if any Prometheus CRDs are installed
// by verifying the existence of key CRDs related to Prometheus.
func IsPrometheusCRDsInstalled() bool {
	// List of common Prometheus CRDs
	prometheusCRDs := []string{
		"prometheuses.monitoring.coreos.com",
		"prometheusrules.monitoring.coreos.com",
		"prometheusagents.monitoring.coreos.com",
	}

	cmd := exec.Command("kubectl", "get", "crds", "-o", "custom-columns=NAME:.metadata.name")
	output, err := Run(cmd)
	if err != nil {
		return false
	}
	crdList := GetNonEmptyLines(output)
	for _, crd := range prometheusCRDs {
		for _, line := range crdList {
			if strings.Contains(line, crd) {
				return true
			}
		}
	}

	return false
}

// No cert-manager helpers here, deliberately: the manager provisions its own
// webhook serving cert in-process (pkg/cert), so the e2e suite has nothing to
// install or wait for before deploying.

// defaultKindCluster is the throwaway cluster the e2e suite targets when
// KIND_CLUSTER is unset. It matches the Makefile's KIND_CLUSTER default, and is
// deliberately NOT "kind": that is the conventional name of a developer's own
// cluster, and this suite deploys, undeploys and deletes namespaces.
const defaultKindCluster = "nebula-test-e2e"

// KindClusterName is the kind cluster the suite runs against.
func KindClusterName() string {
	if v := os.Getenv("KIND_CLUSTER"); v != "" {
		return v
	}
	return defaultKindCluster
}

// kubeconfigPath pins every command Run launches to one cluster. Empty until
// UseKindKubeconfig succeeds.
var kubeconfigPath string

// UseKindKubeconfig points the whole suite at KindClusterName()'s own kubeconfig,
// so nothing it runs can reach the ambient current-context.
//
// This is a safety mechanism, not a convenience. Every kubectl call here inherits
// the current context, and the suite runs `make undeploy` and
// `kubectl delete ns nebula-system` — against a live cluster that destroys a real
// installation. KIND_CLUSTER alone does not prevent it: it only chose which cluster
// `kind load` pushed the image to, so the image and the API calls could land on
// DIFFERENT clusters. Writing a private kubeconfig fixes it centrally, and covers the
// `make install`/`deploy-e2e`/`undeploy` subprocesses too, since they inherit the env.
//
// A missing cluster is a hard error: failing to start beats deploying somewhere else.
func UseKindKubeconfig() error {
	cluster := KindClusterName()
	out, err := exec.Command("kind", "get", "kubeconfig", "--name", cluster).Output()
	if err != nil {
		return fmt.Errorf("get kubeconfig for kind cluster %q (create it with `make setup-test-e2e`): %w", cluster, err)
	}

	f, err := os.CreateTemp("", "nebula-e2e-kubeconfig-")
	if err != nil {
		return fmt.Errorf("create temp kubeconfig: %w", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(out); err != nil {
		return fmt.Errorf("write temp kubeconfig: %w", err)
	}

	kubeconfigPath = f.Name()
	_, _ = fmt.Fprintf(GinkgoWriter, "e2e pinned to kind cluster %q via %s\n", cluster, kubeconfigPath)
	return nil
}

// ReleaseKindKubeconfig removes the temp kubeconfig. Unpinning is deliberate: a later
// Run must not silently fall back to the ambient context, so callers use this only at
// the very end of the suite.
func ReleaseKindKubeconfig() {
	if kubeconfigPath == "" {
		return
	}
	if err := os.Remove(kubeconfigPath); err != nil {
		warnError(err)
	}
	kubeconfigPath = ""
}

// LoadImageToKindClusterWithName loads a local docker image to the kind cluster
func LoadImageToKindClusterWithName(name string) error {
	kindOptions := []string{"load", "docker-image", name, "--name", KindClusterName()}
	cmd := exec.Command("kind", kindOptions...)
	_, err := Run(cmd)
	return err
}

// GetNonEmptyLines converts given command output string into individual objects
// according to line breakers, and ignores the empty elements in it.
func GetNonEmptyLines(output string) []string {
	var res []string
	elements := strings.Split(output, "\n")
	for _, element := range elements {
		if element != "" {
			res = append(res, element)
		}
	}

	return res
}

// GetProjectDir will return the directory where the project is
func GetProjectDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return wd, fmt.Errorf("failed to get current working directory: %w", err)
	}
	wd = strings.ReplaceAll(wd, "/test/e2e", "")
	return wd, nil
}

// UncommentCode searches for target in the file and remove the comment prefix
// of the target content. The target content may span multiple lines.
func UncommentCode(filename, target, prefix string) error {
	// false positive
	// nolint:gosec
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file %q: %w", filename, err)
	}
	strContent := string(content)

	idx := strings.Index(strContent, target)
	if idx < 0 {
		return fmt.Errorf("unable to find the code %q to uncomment", target)
	}

	out := new(bytes.Buffer)
	_, err = out.Write(content[:idx])
	if err != nil {
		return fmt.Errorf("failed to write to output: %w", err)
	}

	scanner := bufio.NewScanner(bytes.NewBufferString(target))
	if !scanner.Scan() {
		return nil
	}
	for {
		if _, err = out.WriteString(strings.TrimPrefix(scanner.Text(), prefix)); err != nil {
			return fmt.Errorf("failed to write to output: %w", err)
		}
		// Avoid writing a newline in case the previous line was the last in target.
		if !scanner.Scan() {
			break
		}
		if _, err = out.WriteString("\n"); err != nil {
			return fmt.Errorf("failed to write to output: %w", err)
		}
	}

	if _, err = out.Write(content[idx+len(target):]); err != nil {
		return fmt.Errorf("failed to write to output: %w", err)
	}

	// false positive
	// nolint:gosec
	if err = os.WriteFile(filename, out.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write file %q: %w", filename, err)
	}

	return nil
}
