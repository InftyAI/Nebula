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

// Package sandd holds the manager-side client for the SandD key broker (see
// cmd/keybroker). It is the transport that backs provider.DaemonKeyMinter: the
// provider package stays HTTP-free and unit-testable, and this package owns the
// one network call the manager makes to mint a per-daemon mesh key at provision
// time. Nothing here touches headscale directly — the broker alone holds that
// authority; the manager only asks it for a key.
package sandd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// BrokerClient mints headscale pre-auth keys by calling the in-cluster key broker
// (nebula-keybroker Service). It implements provider.DaemonKeyMinter for the daemon
// case; the controller mints its own key directly from its pod, not through this.
type BrokerClient struct {
	// baseURL is the broker root, e.g. "http://nebula-keybroker.nebula-system:8090".
	baseURL string
	http    *http.Client
}

// NewBrokerClient builds a client for the broker at baseURL. A zero/empty baseURL
// yields a nil client so callers can treat "no broker configured" as "no dynamic
// minting" (fall back to the static key) without a separate flag.
func NewBrokerClient(baseURL string) *BrokerClient {
	if strings.TrimSpace(baseURL) == "" {
		return nil
	}
	return &BrokerClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		// A short timeout: the broker is an in-cluster sidecar shelling out to a
		// local CLI, so a healthy mint is sub-second. This bounds the Provision path
		// if the broker is wedged — the caller then fails provisioning cleanly rather
		// than hanging until the Provision deadline.
		http: &http.Client{Timeout: 10 * time.Second},
	}
}

// keyResponse mirrors the broker's JSON response (cmd/keybroker.keyResponse). Only
// Key is load-bearing here; the rest is metadata the broker returns for logging.
type keyResponse struct {
	Key string `json:"key"`
}

// MintDaemonKey requests a fresh reusable, ephemeral daemon key from the broker.
// Reusable so a reaped daemon can re-register after a mesh blip (a single-use key
// would be spent and the daemon could never rejoin); ephemeral so its node is still
// reaped on disconnect. It satisfies provider.DaemonKeyMinter. The returned key is a
// secret; callers must not log it.
func (c *BrokerClient) MintDaemonKey(ctx context.Context) (string, error) {
	return c.mint(ctx, "daemon")
}

// mint POSTs /keys?kind=<kind> and returns the minted key. It is shared by the
// daemon path (and usable for a controller path) so the single request/parse/error
// shape lives in one place.
func (c *BrokerClient) mint(ctx context.Context, kind string) (string, error) {
	u := fmt.Sprintf("%s/keys?kind=%s", c.baseURL, url.QueryEscape(kind))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return "", fmt.Errorf("building key-broker request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("calling key broker: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// Do not echo the body: on success it is key material, and even on error the
		// status code is the actionable signal.
		return "", fmt.Errorf("key broker returned status %d", resp.StatusCode)
	}

	var kr keyResponse
	if err := json.NewDecoder(resp.Body).Decode(&kr); err != nil {
		return "", fmt.Errorf("decoding key-broker response: %w", err)
	}
	if kr.Key == "" {
		return "", fmt.Errorf("key broker returned an empty key")
	}
	return kr.Key, nil
}
