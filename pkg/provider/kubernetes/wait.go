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

package kubernetes

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/InftyAI/Nebula/pkg/util"
)

// waitInterval is how often a pending Provision re-reads its pod.
const waitInterval = 2 * time.Second

// waitForProvision watches a new pod for provisionTimeout: long enough to catch a warm node
// binding it or the node autoscaler refusing it, without holding a worker through node boot.
// Neither within the window is no answer yet (false, nil); the poll loop takes over from there
// (see toInstance). Image pull and startup after binding are the readiness deadline's.
func waitForProvision(ctx context.Context, cc clusterClients, as nodeAutoscaler, namespace, name string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, as.provisionTimeout())
	defer cancel()
	t := time.NewTicker(waitInterval)
	defer t.Stop()
	for {
		bound, err := checkBinding(ctx, cc, as, namespace, name)
		if bound || err != nil {
			return bound, err
		}
		select {
		case <-ctx.Done():
			return false, nil
		case <-t.C:
		}
	}
}

// checkBinding is one pass of waitForProvision. A failed read is not a verdict, so it is
// swallowed and the next tick retries.
func checkBinding(ctx context.Context, cc clusterClients, as nodeAutoscaler, namespace, name string) (bool, error) {
	pod, err := cc.kube.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, fmt.Errorf("kubernetes: pod %s/%s disappeared before it was bound", namespace, name)
	}
	if err != nil {
		return false, nil
	}
	// With reserved capacity, the pod will have a NodeName assigned once it is scheduled.
	if pod.Spec.NodeName != "" {
		return true, nil
	}
	if util.Finished(pod.Status.Phase) {
		return false, fmt.Errorf("kubernetes: pod %s/%s ended %s before it was bound: %s",
			namespace, name, pod.Status.Phase, pod.Status.Message)
	}
	return false, as.refused(ctx, cc, pod)
}
