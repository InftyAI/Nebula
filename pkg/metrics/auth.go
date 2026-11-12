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

	"github.com/InftyAI/Nebula/pkg/provider"
)

var (
	// ProviderAuthFailures counts poll-loop List calls that failed on credentials or
	// permissions. The poll loop runs on a cadence through the provider's auth path, so this is
	// where a dying token shows first; Provision's own are in ProvisionFailures{reason="auth"}.
	ProviderAuthFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nebula_provider_auth_failures_total",
		Help: "Provider List calls from the poll loop that failed on auth (ErrAuth), by provider.",
	}, []string{labelProvider})

	// ProviderAuthFailing is 1 from a provider's first auth failure to its next success. Any
	// other error leaves it as it was: a timeout says nothing about credentials.
	ProviderAuthFailing = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nebula_provider_auth_failing",
		Help: "1 while a provider's poll-loop List keeps failing on auth, 0 once it succeeds, by provider.",
	}, []string{labelProvider})
)

func init() {
	ctrlmetrics.Registry.MustRegister(ProviderAuthFailures, ProviderAuthFailing)
}

// RecordProviderAuth records the outcome of one poll-loop List call for prov.
func RecordProviderAuth(prov string, err error) {
	switch {
	case err == nil:
		ProviderAuthFailing.WithLabelValues(prov).Set(0)
	case provider.IsAuthFailure(err):
		ProviderAuthFailures.WithLabelValues(prov).Inc()
		ProviderAuthFailing.WithLabelValues(prov).Set(1)
	}
}
