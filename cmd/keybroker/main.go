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

// Command keybroker is a tiny HTTP service that mints headscale pre-auth keys on
// demand, so nothing else in the system needs headscale admin authority.
//
// WHY IT EXISTS: SandD daemons and the controller need a mesh pre-auth key to
// join, but neither can safely hold headscale's admin credentials — the daemon
// runs inside an untrusted tenant container, and the controller is user-facing
// sample code. Minting keys by hand also doesn't scale: every experiment spins up
// a fresh controller and every workload is a new daemon. This broker centralizes
// the privilege: it is the ONLY component that talks to headscale's admin surface,
// and it does so over headscale's LOCAL unix socket as a sidecar in the headscale
// pod — so there is no admin API key to store and no gRPC port to expose. Callers
// just POST /keys and get back a single, freshly-minted key string.
//
// SECURITY POSTURE:
//   - It shells out to the `headscale` CLI over the shared unix socket
//     (--config points at headscale's own config, which carries unix_socket). No
//     API key, no network call to headscale.
//   - It is reachable only in-cluster (a ClusterIP Service). It mints keys but
//     cannot read or revoke them and exposes nothing else, so a compromised caller
//     can at most cause it to mint extra ephemeral keys (which auto-reap).
//   - The minted key is a secret: it is returned in the response body but NEVER
//     logged. Only the key's metadata (kind, reusable, ephemeral) is logged.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// keyKind is the caller-declared purpose of the key, which selects its policy. The
// two consumers want opposite lifecycles (see policyFor), so the caller names its
// role rather than passing raw flags — the broker owns the policy, not the caller.
type keyKind string

const (
	// kindDaemon is a GPU-workload daemon key: REUSABLE + ephemeral + bounded TTL.
	// Each workload still gets its OWN freshly-minted key (best tenant isolation) that
	// headscale auto-reaps once the pod disconnects (no orphan node pile-up). It is
	// REUSABLE — not single-use — because on a flaky mesh a daemon's node gets reaped
	// (headscale ephemeral_node_inactivity_timeout) during a transient blip, and after
	// a reap the ONLY way back in is to re-register with the pre-auth key. A single-use
	// key is already spent by then, so the daemon wedges forever, unable to rejoin. A
	// reusable key lets every `tailscale up` retry re-register the node, which is what
	// makes reconnection actually robust. The blast radius of a leaked reusable key is
	// bounded by the ephemeral reap (nodes vanish on disconnect) and the TTL below.
	kindDaemon keyKind = "daemon"
	// kindController is a SandD controller key: REUSABLE + ephemeral + long TTL.
	// Reusable so it can re-register across restarts. Ephemeral — the SAME as a
	// daemon — because the controller holds NO persistent state (no PVC): headscale
	// reaps its node the instant the pod disconnects, which FREES the stable
	// sandd-controller MagicDNS name for the next pod to reclaim (the name is pinned
	// by the pod hostname, not by a persisted node key). This is what lets a restart
	// reclaim the same name with no -suffix. It also sidesteps the port-443 dial
	// wedge that a persisted /var/lib/tailscale caused. Long TTL (720h) just bounds a
	// key that outlives a brief reap gap; ephemeral reaping is the real cleanup path.
	kindController keyKind = "controller"
)

// keyPolicy is the resolved (headscale-flag) shape of a key for a given kind.
type keyPolicy struct {
	reusable   bool
	ephemeral  bool
	expiration string // headscale --expiration duration, e.g. "1h", "720h"
}

// policyFor maps a kind onto its headscale flags. Unknown kinds are rejected by the
// caller before this is reached, so there is no default policy to fall through to.
func policyFor(kind keyKind) (keyPolicy, bool) {
	switch kind {
	case kindDaemon:
		// Reusable so a reaped daemon can re-register: on a flaky mesh the node is
		// reaped during a blip, and re-auth via the pre-auth key is the only way back
		// in — a single-use key would already be spent, wedging the daemon forever.
		// Ephemeral so the node is still reaped on disconnect (no orphan pile-up). TTL
		// bounds a leaked key; each workload gets its own freshly-minted one anyway.
		return keyPolicy{reusable: true, ephemeral: true, expiration: "24h"}, true
	case kindController:
		// Reusable so it can re-register across restarts; ephemeral so the old node
		// is reaped on disconnect, freeing the stable MagicDNS name for the fresh pod
		// to reclaim (the controller has no PVC, so nothing to preserve). Long TTL
		// (720h) just bounds a key that outlives a brief reap gap.
		return keyPolicy{reusable: true, ephemeral: true, expiration: "1h"}, true
	default:
		return keyPolicy{}, false
	}
}

// keyResponse is the JSON body returned to a caller. It carries the freshly-minted
// key plus the policy metadata, so a caller can log/inspect what it got WITHOUT the
// broker having to expose headscale's richer key object.
type keyResponse struct {
	Key        string `json:"key"`
	Kind       string `json:"kind"`
	Reusable   bool   `json:"reusable"`
	Ephemeral  bool   `json:"ephemeral"`
	Expiration string `json:"expiration"`
}

// headscalePreAuthKey is the subset of `headscale preauthkeys create -o json` we
// parse. headscale prints the created key object as JSON; we only need its `key`.
type headscalePreAuthKey struct {
	Key string `json:"key"`
}

// headscaleUser is the subset of `headscale users list -o json` we parse. We only
// need the name to decide whether the configured user already exists.
type headscaleUser struct {
	Name string `json:"name"`
}

// config is the broker's runtime configuration, all from env with sane defaults so
// the manifest can stay minimal.
type config struct {
	// listenAddr is the HTTP bind address (SANDD_KEYBROKER_LISTEN, default :8090).
	listenAddr string
	// user is the headscale user keys are minted under (SANDD_KEYBROKER_USER,
	// default "nebula"). The broker ensures it exists at startup (ensureHeadscaleUser),
	// so no manual bootstrap is needed.
	user string
	// headscaleConfig is the path to headscale's config file, which carries the
	// unix_socket the CLI dials (SANDD_KEYBROKER_HS_CONFIG, default the standard
	// /etc/headscale/config.yaml the sidecar shares with headscale).
	headscaleConfig string
	// headscaleBin is the headscale CLI path (SANDD_KEYBROKER_HS_BIN, default
	// "headscale" on PATH — the sidecar image is FROM headscale).
	headscaleBin string
}

func loadConfig() config {
	return config{
		listenAddr:      envOr("SANDD_KEYBROKER_LISTEN", ":8090"),
		user:            envOr("SANDD_KEYBROKER_USER", "nebula"),
		headscaleConfig: envOr("SANDD_KEYBROKER_HS_CONFIG", "/etc/headscale/config.yaml"),
		headscaleBin:    envOr("SANDD_KEYBROKER_HS_BIN", "headscale"),
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// commandRunner mints a key by invoking the headscale CLI. It is a seam so tests
// can inject a fake without a real headscale (the real one is runHeadscale).
type commandRunner func(cfg config, policy keyPolicy) (string, error)

// userEnsurer creates cfg.user in headscale if it does not already exist. It is a
// seam so tests can inject a fake without a real headscale (the real one is
// ensureHeadscaleUser).
type userEnsurer func(cfg config) error

// ensureUserRetries and ensureUserRetryDelay bound the startup wait for headscale's
// unix socket. ~10 attempts over 20s comfortably covers headscale opening its socket
// while the sidecars boot together, without hanging a genuinely broken deploy forever.
// Vars, not consts, so tests can shrink the delay to run without real sleeps.
var (
	ensureUserRetries    = 10
	ensureUserRetryDelay = 2 * time.Second
)

// ensureHeadscaleUserWithRetry retries ensure until it succeeds or the attempt budget
// is exhausted, absorbing the sidecar startup race where headscale's socket is not up
// yet. The ensure func is a parameter so tests drive it without a real headscale or
// real sleeps. Returns the last error if every attempt fails.
func ensureHeadscaleUserWithRetry(cfg config, ensure userEnsurer) error {
	var err error
	for attempt := 1; attempt <= ensureUserRetries; attempt++ {
		if err = ensure(cfg); err == nil {
			return nil
		}
		if attempt < ensureUserRetries {
			log.Printf("ensure headscale user attempt %d/%d failed (retrying): %v", attempt, ensureUserRetries, err)
			time.Sleep(ensureUserRetryDelay)
		}
	}
	return err
}

// ensureHeadscaleUser makes the configured user exist, idempotently: it lists users
// over the local unix socket, returns early if cfg.user is already present, and
// otherwise creates it. This replaces the manual `headscale users create` bootstrap
// step — the broker already has socket access, so it can self-provision the one user
// it mints under. Safe to run on every startup.
func ensureHeadscaleUser(cfg config) error {
	listCmd := exec.Command(cfg.headscaleBin, "--config", cfg.headscaleConfig, "users", "list", "--output", "json")
	var stdout, stderr strings.Builder
	listCmd.Stdout = &stdout
	listCmd.Stderr = &stderr
	if err := listCmd.Run(); err != nil {
		return fmt.Errorf("headscale users list failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	var users []headscaleUser
	if err := json.Unmarshal([]byte(stdout.String()), &users); err != nil {
		return fmt.Errorf("parsing headscale users list: %w", err)
	}
	for _, u := range users {
		if u.Name == cfg.user {
			log.Printf("headscale user %q already exists", cfg.user)
			return nil
		}
	}

	createCmd := exec.Command(cfg.headscaleBin, "--config", cfg.headscaleConfig, "users", "create", cfg.user)
	var createErr strings.Builder
	createCmd.Stderr = &createErr
	if err := createCmd.Run(); err != nil {
		return fmt.Errorf("headscale users create %q failed: %w: %s", cfg.user, err, strings.TrimSpace(createErr.String()))
	}
	log.Printf("created headscale user %q", cfg.user)
	return nil
}

// runHeadscale shells out to `headscale preauthkeys create -o json` over the local
// unix socket (via --config, which carries unix_socket) and returns the key string.
// It NEVER logs the key or the raw output (which contains the key).
func runHeadscale(cfg config, policy keyPolicy) (string, error) {
	args := []string{
		"--config", cfg.headscaleConfig,
		"preauthkeys", "create",
		"--user", cfg.user,
		"--expiration", policy.expiration,
		"--output", "json",
	}
	if policy.reusable {
		args = append(args, "--reusable")
	}
	if policy.ephemeral {
		args = append(args, "--ephemeral")
	}

	cmd := exec.Command(cfg.headscaleBin, args...)
	// Capture stdout (the JSON key object) separately from stderr so a headscale
	// error message can be surfaced without risking the key leaking into a log via
	// combined output.
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("headscale preauthkeys create failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	var key headscalePreAuthKey
	if err := json.Unmarshal([]byte(stdout.String()), &key); err != nil {
		// Do NOT include stdout in the error: it is the key material.
		return "", fmt.Errorf("parsing headscale key output: %w", err)
	}
	if key.Key == "" {
		return "", fmt.Errorf("headscale returned an empty key")
	}
	return key.Key, nil
}

// keysHandler handles POST /keys?kind=<daemon|controller>. It resolves the policy,
// mints one key, and returns it as JSON. GET/other methods and unknown kinds are
// rejected. The runner seam lets tests avoid a real headscale.
func keysHandler(cfg config, run commandRunner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "only POST is supported", http.StatusMethodNotAllowed)
			return
		}
		kind := keyKind(r.URL.Query().Get("kind"))
		policy, ok := policyFor(kind)
		if !ok {
			http.Error(w, fmt.Sprintf("unknown or missing kind %q (want daemon|controller)", kind), http.StatusBadRequest)
			return
		}

		key, err := run(cfg, policy)
		if err != nil {
			// err carries headscale's stderr, never the key. Safe to log + return.
			log.Printf("mint failed kind=%s: %v", kind, err)
			http.Error(w, "failed to mint key", http.StatusBadGateway)
			return
		}

		// Log METADATA only — never the key itself.
		log.Printf("minted key kind=%s reusable=%s ephemeral=%s exp=%s",
			kind, strconv.FormatBool(policy.reusable), strconv.FormatBool(policy.ephemeral), policy.expiration)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(keyResponse{
			Key:        key,
			Kind:       string(kind),
			Reusable:   policy.reusable,
			Ephemeral:  policy.ephemeral,
			Expiration: policy.expiration,
		})
	}
}

func main() {
	cfg := loadConfig()

	// Self-provision the user we mint under, so the mesh works without a manual
	// `headscale users create`. The broker is a SIDECAR that starts alongside
	// headscale, so its unix socket may not be up yet on the first attempt — retry
	// with backoff rather than crash-looping on that startup race. Fatal only after
	// the window elapses: a broker that can't guarantee its user exists would 502 on
	// every mint, so fail loudly instead of serving broken.
	if err := ensureHeadscaleUserWithRetry(cfg, ensureHeadscaleUser); err != nil {
		log.Fatalf("ensuring headscale user %q: %v", cfg.user, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/keys", keysHandler(cfg, runHeadscale))
	// Liveness/readiness: the broker is up as soon as it can serve HTTP. We do NOT
	// probe headscale here (that would need a real mint); mint errors surface as a
	// 502 on /keys, which the caller retries.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("keybroker listening on %s (user=%s, headscale config=%s)",
		cfg.listenAddr, cfg.user, cfg.headscaleConfig)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("keybroker server exited: %v", err)
	}
}
