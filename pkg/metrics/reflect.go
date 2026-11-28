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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Results of reflecting one source to one target cluster: the copy was written or deleted as
// needed (succeeded), an object of that name there isn't a copy (refused), or the API call failed.
const (
	ReflectSucceeded = "succeeded"
	ReflectRefused   = "refused"
	ReflectFailed    = "failed"
)

// Reflections counts reflections of home-cluster Secrets and ConfigMaps to the Kubernetes
// provider's other clusters. Sustained failures leave copies stale, so a container restarted
// there may read an expired registry token or presigned URL.
var Reflections = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "nebula_reflections_total",
	Help: "Reflections of home-cluster Secrets and ConfigMaps to a target cluster, by cluster and result.",
}, []string{"cluster", "result"})

func init() {
	ctrlmetrics.Registry.MustRegister(Reflections)
}
