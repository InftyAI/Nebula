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

package lambda

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/util"
)

func bootstrap(pod *corev1.Pod, req provider.ProvisionRequest, port int) (string, error) {
	if err := validateWorkload(pod, req, port); err != nil {
		return "", err
	}
	c := pod.Spec.Containers[0]

	var script strings.Builder
	script.WriteString("#!/bin/bash\nset -euo pipefail\n")
	fmt.Fprintf(&script, "docker pull -- %s\n", shellQuote(c.Image))
	script.WriteString("docker run --detach --name nebula-workload --restart always --pull never --gpus all")
	fmt.Fprintf(&script, " --publish %d:%d", port, port)
	_, limits := util.PodResources(pod)
	if limits.CPU > 0 {
		fmt.Fprintf(&script, " --cpus %s", strconv.FormatFloat(limits.CPU, 'f', -1, 64))
	}
	if limits.MemoryMiB > 0 {
		fmt.Fprintf(&script, " --memory %dm", limits.MemoryMiB)
	}
	if c.WorkingDir != "" {
		fmt.Fprintf(&script, " --workdir %s", shellQuote(c.WorkingDir))
	}
	for _, k := range slices.Sorted(maps.Keys(req.Env.Config)) {
		fmt.Fprintf(&script, " --env %s", shellQuote(k+"="+req.Env.Config[k]))
	}
	if len(c.Command) > 0 {
		fmt.Fprintf(&script, " --entrypoint %s", shellQuote(c.Command[0]))
	}
	fmt.Fprintf(&script, " -- %s", shellQuote(c.Image))
	if len(c.Command) > 1 {
		for _, arg := range c.Command[1:] {
			fmt.Fprintf(&script, " %s", shellQuote(arg))
		}
	}
	for _, arg := range c.Args {
		fmt.Fprintf(&script, " %s", shellQuote(arg))
	}
	script.WriteByte('\n')
	if script.Len() > 1_000_000 {
		return "", fmt.Errorf("lambda: cloud-init exceeds 1 MB: %w", provider.ErrRequestRejected)
	}
	return script.String(), nil
}

func validateWorkload(pod *corev1.Pod, req provider.ProvisionRequest, port int) error {
	if len(req.Env.Secrets) != 0 || req.RegistryAuth != nil {
		return fmt.Errorf("lambda: cloud-init requires a public image and non-secret environment: %w",
			provider.ErrRequestRejected)
	}
	if pod == nil || len(pod.Spec.Containers) != 1 ||
		len(pod.Spec.InitContainers)+len(pod.Spec.EphemeralContainers)+len(pod.Spec.Volumes) != 0 ||
		pod.Spec.HostNetwork || pod.Spec.HostPID || pod.Spec.HostIPC ||
		pod.Spec.SecurityContext != nil && !reflect.DeepEqual(*pod.Spec.SecurityContext, corev1.PodSecurityContext{}) {
		return fmt.Errorf("lambda: expected one container without volumes or host namespaces: %w",
			provider.ErrRequestRejected)
	}
	if pod.Spec.RestartPolicy != "" && pod.Spec.RestartPolicy != corev1.RestartPolicyAlways {
		return fmt.Errorf("lambda: only restartPolicy Always is supported: %w", provider.ErrRequestRejected)
	}
	c := pod.Spec.Containers[0]
	if c.Image == "" || c.SecurityContext != nil || c.LivenessProbe != nil || c.StartupProbe != nil ||
		c.Lifecycle != nil || c.Stdin || c.TTY {
		return fmt.Errorf("lambda: unsupported image, security context or process settings: %w",
			provider.ErrRequestRejected)
	}
	for _, resources := range []corev1.ResourceList{c.Resources.Requests, c.Resources.Limits} {
		for name := range resources {
			switch name {
			case corev1.ResourceCPU, corev1.ResourceMemory, "nvidia.com/gpu":
			default:
				return fmt.Errorf("lambda: unsupported resource %s: %w", name, provider.ErrRequestRejected)
			}
		}
	}
	if len(c.Ports) != 1 || c.Ports[0].ContainerPort != int32(port) || c.Ports[0].HostPort != 0 ||
		c.Ports[0].HostIP != "" || c.Ports[0].Protocol != "" && c.Ports[0].Protocol != corev1.ProtocolTCP {
		return fmt.Errorf("lambda: declare container TCP port %d: %w", port, provider.ErrRequestRejected)
	}
	if c.ReadinessProbe != nil {
		return fmt.Errorf("lambda: the provider probes /healthz on port %d: %w", port, provider.ErrRequestRejected)
	}

	return nil
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
