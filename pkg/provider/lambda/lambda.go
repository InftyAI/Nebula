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

// Package lambda runs one GPU workload container per Lambda On-Demand Cloud VM.
package lambda

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nebula "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/provider/catalog"
	"github.com/InftyAI/Nebula/pkg/util"
)

const (
	statusTerminating = "terminating"
	statusTerminated  = "terminated"
)

type settings struct {
	workspace, region, sshKey, firewall, cluster string
	port                                         int
}

type Provider struct {
	catalog.Base
	api    *cloudClient
	reader client.Reader
	writer client.Writer
	config settings
	health func(context.Context, string) bool

	skuMu      sync.Mutex
	skus       map[string]instanceTypeEntry
	skuExpires time.Time
}

var _ provider.Provider = (*Provider)(nil)

func NewFromEnv(reader client.Reader, writer client.Writer) (*Provider, error) {
	config := settings{port: 8080}
	key := ""
	for _, field := range []struct {
		name string
		dest *string
	}{
		{"LAMBDA_API_KEY", &key},
		{"LAMBDA_WORKSPACE_ID", &config.workspace},
		{"LAMBDA_REGION", &config.region},
		{"LAMBDA_SSH_KEY_NAME", &config.sshKey},
		{"LAMBDA_FIREWALL_RULESET_ID", &config.firewall},
		{"LAMBDA_CLUSTER_ID", &config.cluster},
	} {
		*field.dest = strings.TrimSpace(os.Getenv(field.name))
		if *field.dest == "" {
			return nil, fmt.Errorf("lambda: %s is required", field.name)
		}
	}
	if len(config.cluster) > 128 {
		return nil, errors.New("lambda: LAMBDA_CLUSTER_ID exceeds the 128-character tag limit")
	}
	if geography(config.region) == "" {
		return nil, errors.New("lambda: LAMBDA_REGION must be a concrete Lambda region")
	}
	if raw := os.Getenv("LAMBDA_SERVICE_PORT"); raw != "" {
		port, err := strconv.Atoi(raw)
		if err != nil || port < 1024 || port > 65535 {
			return nil, errors.New("lambda: LAMBDA_SERVICE_PORT must be between 1024 and 65535")
		}
		config.port = port
	}
	cat, err := catalog.Load()
	if err != nil {
		return nil, err
	}
	return newProvider(newCloudClient(key), reader, writer, cat, config), nil
}

func newProvider(
	api *cloudClient, reader client.Reader, writer client.Writer, cat catalog.Lookup, config settings,
) *Provider {
	probeClient := &http.Client{
		Transport: &http.Transport{DialContext: (&net.Dialer{Timeout: 2 * time.Second}).DialContext, DisableKeepAlives: true},
		Timeout:   2 * time.Second, CheckRedirect: refuseRedirect,
	}
	return &Provider{
		Base: catalog.Base{ProviderName: provider.ProviderLambda, Catalog: cat},
		api:  api, reader: reader, writer: writer, config: config,
		health: func(ctx context.Context, endpoint string) bool {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+endpoint+"/healthz", nil)
			if err != nil {
				return false
			}
			res, err := probeClient.Do(req)
			if err != nil {
				return false
			}
			_ = res.Body.Close()
			return res.StatusCode >= 200 && res.StatusCode < 300
		},
	}
}

func (*Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{NativeTags: true, ProvisionTimeout: 60 * time.Second}
}

func geography(region string) string {
	prefix, suffix, found := strings.Cut(region, "-")
	if !found || !strings.Contains(suffix, "-") {
		return ""
	}
	switch prefix {
	case "asia":
		return "ap"
	case "europe":
		return "eu"
	case "us", "me":
		return prefix
	default:
		return ""
	}
}

func (p *Provider) ResolveRegions(declared, narrowTo []string) []string {
	region := p.config.region
	geo := geography(region)
	if len(narrowTo) != 0 && !slices.Contains(narrowTo, geo) {
		return nil
	}
	if len(declared) == 0 || slices.Contains(declared, region) || slices.Contains(declared, geo) {
		return []string{region}
	}
	return nil
}

func (p *Provider) Provision(
	ctx context.Context, pod *corev1.Pod, req provider.ProvisionRequest,
) (provider.ProvisionResult, error) {
	if req.Region != p.config.region ||
		(req.CapacityType != "" && req.CapacityType != nebula.CapacityOnDemand) ||
		(req.Egress != nil && req.Egress.ModeOrOpen() != nebula.EgressOpen) {
		return provider.ProvisionResult{}, fmt.Errorf(
			"lambda: expected the configured region, OnDemand and Open egress: %w", provider.ErrRequestRejected,
		)
	}
	script, err := bootstrap(pod, req, p.config.port)
	if err != nil {
		return provider.ProvisionResult{}, err
	}
	nc, err := p.provisionClaim(ctx, pod, req.ClaimName)
	if err != nil {
		return provider.ProvisionResult{}, err
	}

	recorded, err := readAttempt(nc)
	if err != nil {
		return provider.ProvisionResult{}, err
	}
	found, err := p.find(ctx, nc, recorded)
	if err != nil {
		if recorded != nil {
			err = errors.Join(errLaunchUnresolved, err)
		}
		return provider.ProvisionResult{}, err
	}
	if found != nil && found.State != provider.InstanceTerminated {
		return provider.ProvisionResult{InstanceID: found.ID, Reserved: true}, nil
	}
	if recorded != nil {
		return provider.ProvisionResult{}, errors.New("lambda: claimed instance has already terminated")
	}
	accel, count, err := util.AcceleratorRequest(pod)
	if err != nil {
		return provider.ProvisionResult{}, fmt.Errorf("lambda: %w: %w", provider.ErrRequestRejected, err)
	}
	ids, ok := p.MapAccelerator(accel, count)
	if !ok {
		return provider.ProvisionResult{}, fmt.Errorf("lambda: %w", provider.ErrUnsupportedAccelerator)
	}
	sku := ids[0]
	if err := p.validateSKU(ctx, pod, sku, count); err != nil {
		return provider.ProvisionResult{}, err
	}

	if err := p.api.reserveLaunch(ctx); err != nil {
		return provider.ProvisionResult{}, err
	}
	a := &attempt{Started: time.Now().UTC()}
	concurrent, err := p.reserveAttempt(ctx, pod, nc, a)
	if err != nil {
		return provider.ProvisionResult{}, fmt.Errorf("lambda: reserve NodeClaim launch: %w", err)
	}
	if concurrent != nil {
		other, err := p.find(ctx, nc, concurrent)
		if err != nil {
			return provider.ProvisionResult{}, errors.Join(errLaunchUnresolved, err)
		}
		if other != nil && other.State != provider.InstanceTerminated {
			return provider.ProvisionResult{InstanceID: other.ID, Reserved: true}, nil
		}
		return provider.ProvisionResult{}, errors.New("lambda: claimed instance has already terminated")
	}

	id, err := p.api.launch(ctx, launchRequest{
		Region: req.Region, InstanceType: sku, Name: instanceName(nc), UserData: script,
		Workspace: p.config.workspace, SSHKeyNames: []string{p.config.sshKey},
		Image: map[string]string{"family": "lambda-stack-24-04"}, Tags: claimTags(p.config.cluster, nc),
		FirewallRulesets: []map[string]string{{"id": p.config.firewall}},
	})
	if err != nil {
		if !definitelyRejected(err) {
			return provider.ProvisionResult{}, errors.Join(errLaunchUnresolved, err)
		}
		classified := classify(err)
		if recordErr := p.finishAttempt(ctx, nc, a.Started, nil); recordErr != nil {
			return provider.ProvisionResult{}, errors.Join(
				classified, fmt.Errorf("lambda: clear rejected launch: %w", recordErr),
			)
		}
		return provider.ProvisionResult{}, classified
	}
	a.InstanceID = id
	if err := p.finishAttempt(ctx, nc, a.Started, a); err != nil {
		return provider.ProvisionResult{}, fmt.Errorf("lambda: record accepted launch: %w: %w", errLaunchUnresolved, err)
	}
	return provider.ProvisionResult{InstanceID: id, Reserved: true}, nil
}

// reserveAttempt retries harmless claim updates without letting two callers send a launch.
func (p *Provider) reserveAttempt(
	ctx context.Context, pod *corev1.Pod, nc *nebula.NodeClaim, a *attempt,
) (*attempt, error) {
	var concurrent *attempt
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := p.provisionClaim(ctx, pod, nc.Name)
		if err != nil {
			return err
		}
		if current.UID != nc.UID {
			return errors.New("lambda: NodeClaim changed before launch")
		}
		nc = current
		concurrent, err = readAttempt(nc)
		if err != nil || concurrent != nil {
			return err
		}
		return p.updateAttempt(ctx, nc, a)
	})
	return concurrent, err
}

func (p *Provider) provisionClaim(ctx context.Context, pod *corev1.Pod, name string) (*nebula.NodeClaim, error) {
	nc, err := p.claim(ctx, name)
	if err != nil {
		return nil, err
	}
	if nc == nil || !nc.DeletionTimestamp.IsZero() || nc.Spec.Provider != p.Name() || nc.Spec.Region != p.config.region ||
		nc.Spec.PodRef.UID != string(pod.UID) || nc.Spec.PodRef.Name != pod.Name ||
		nc.Spec.PodRef.Namespace != pod.Namespace {
		return nil, fmt.Errorf("lambda: no current NodeClaim for workload: %w", provider.ErrRequestRejected)
	}
	return nc, nil
}

func (p *Provider) validateSKU(ctx context.Context, pod *corev1.Pod, sku string, count int32) error {
	p.skuMu.Lock()
	defer p.skuMu.Unlock()
	if time.Now().After(p.skuExpires) {
		skus, err := p.api.instanceTypes(ctx, p.config.workspace)
		if err != nil {
			return classify(err)
		}
		p.skus, p.skuExpires = skus, time.Now().Add(30*time.Second)
	}
	entry, ok := p.skus[sku]
	if !ok || entry.Type.Architecture != "x86_64" || entry.Type.Specs.GPUs != count {
		return fmt.Errorf("lambda: SKU %q does not match requested x86-64 GPU shape: %w",
			sku, provider.ErrUnsupportedAccelerator)
	}
	request, limit := util.PodResources(pod)
	if max(request.CPU, limit.CPU) > float64(entry.Type.Specs.VCPUs) ||
		max(request.MemoryMiB, limit.MemoryMiB) > entry.Type.Specs.MemoryGiB*1024 {
		return fmt.Errorf("lambda: CPU or memory exceeds the capacity of %q: %w", sku, provider.ErrSpec)
	}
	if !slices.ContainsFunc(entry.Regions, func(r region) bool { return r.Name == p.config.region }) {
		return fmt.Errorf("lambda: SKU %q unavailable in %s: %w", sku, p.config.region, provider.ErrNoCapacity)
	}
	return nil
}

func (p *Provider) Get(ctx context.Context, id, region string) (*provider.Instance, error) {
	vm, err := p.api.get(ctx, id)
	if err != nil || vm == nil {
		return nil, classify(err)
	}
	name, _ := scopedClaim(*vm, p.config.cluster)
	if name == "" {
		return nil, nil
	}
	nc, err := p.claim(ctx, name)
	if err != nil {
		return nil, err
	}
	if !p.owns(*vm, nc) || region != "" && region != vm.Region.Name {
		return nil, nil
	}
	recorded, err := readAttempt(nc)
	if err != nil {
		return nil, err
	}
	if recorded == nil || recorded.InstanceID != id {
		// The finalizer can resolve an unrecorded launch through FindByClaim.
		return nil, nil
	}
	if !nc.DeletionTimestamp.IsZero() {
		// The finalizer uses Get to authorize terminating the recorded ID. Check
		// for duplicates now, even if the VM disappears before Terminate runs.
		return p.find(ctx, nc, recorded)
	}
	out := p.observe(ctx, *vm, name, false)
	return &out, nil
}

func (p *Provider) Terminate(ctx context.Context, id, region string) error {
	if id == "" {
		return nil
	}
	vm, err := p.api.get(ctx, id)
	if err != nil || vm == nil {
		return classify(err)
	}
	name, _ := scopedClaim(*vm, p.config.cluster)
	if name == "" {
		return errors.New("lambda: refusing to terminate an instance without Nebula ownership")
	}
	nc, err := p.claim(ctx, name)
	if err != nil {
		return err
	}
	if !p.owns(*vm, nc) || region != "" && region != vm.Region.Name {
		return errors.New("lambda: refusing to terminate an instance outside its NodeClaim")
	}
	recorded, err := readAttempt(nc)
	if err != nil {
		return err
	}
	found, err := p.find(ctx, nc, recorded)
	if err != nil {
		return err
	}
	if found == nil || found.ID != id {
		return errors.New("lambda: refusing to terminate an instance outside the launch record")
	}

	if vm.Status == statusTerminated {
		return nil
	}
	if vm.Status != statusTerminating {
		err := p.api.terminate(ctx, id)
		var apiErr *apiError
		if err != nil && (!errors.As(err, &apiErr) || apiErr.status != http.StatusNotFound) {
			return classify(err)
		}
	}
	return errors.New("lambda: waiting for instance termination to be confirmed")
}

func (p *Provider) FindByClaim(ctx context.Context, name, region string) (*provider.Instance, error) {
	nc, err := p.claim(ctx, name)
	if err != nil || nc == nil {
		return nil, err
	}
	if nc.Spec.Provider != p.Name() || nc.Spec.Region != p.config.region || region != "" && region != nc.Spec.Region {
		return nil, errors.New("lambda: NodeClaim is outside the configured provider region")
	}
	recorded, err := readAttempt(nc)
	if err != nil {
		return nil, err
	}
	return p.find(ctx, nc, recorded)
}

func (p *Provider) find(ctx context.Context, nc *nebula.NodeClaim, recorded *attempt) (*provider.Instance, error) {
	vms, err := p.api.list(ctx, p.config.workspace)
	if err != nil {
		return nil, classify(err)
	}
	var vm *instance
	for _, candidate := range vms {
		if !p.owns(candidate, nc) || candidate.Status == statusTerminated &&
			(recorded == nil || recorded.InstanceID != "" && recorded.InstanceID != candidate.ID) {
			continue
		}
		if vm != nil {
			return nil, fmt.Errorf("lambda: multiple instances for NodeClaim %s", nc.Name)
		}
		vm = &candidate
	}

	if recorded != nil && recorded.InstanceID != "" {
		if vm == nil {
			vm, err = p.api.get(ctx, recorded.InstanceID)
			if err != nil {
				return nil, classify(err)
			}
		}
		if vm != nil && (!p.owns(*vm, nc) || vm.ID != recorded.InstanceID) {
			return nil, fmt.Errorf("lambda: instance does not match the launch record for NodeClaim %s", nc.Name)
		}
	}
	if vm == nil {
		if recorded != nil && !recorded.Observed {
			return nil, errLaunchUnresolved
		}
		return nil, nil
	}
	if err := p.recordObservation(ctx, nc, recorded, vm.ID); err != nil {
		return nil, err
	}
	out := p.observe(ctx, *vm, nc.Name, false)
	return &out, nil
}

func (p *Provider) List(ctx context.Context) ([]provider.Instance, error) {
	// Read the claims first so a concurrent launch cannot be mistaken for a missing VM.
	var claims nebula.NodeClaimList
	if err := p.reader.List(ctx, &claims, client.MatchingLabels{nebula.ProviderLabel: p.Name()}); err != nil {
		return nil, err
	}
	byName := make(map[string]*nebula.NodeClaim, len(claims.Items))
	for i := range claims.Items {
		byName[claims.Items[i].Name] = &claims.Items[i]
	}
	vms, err := p.api.list(ctx, p.config.workspace)
	if err != nil {
		return nil, classify(err)
	}
	matches := make(map[string]instance)
	for _, vm := range vms {
		name, _ := scopedClaim(vm, p.config.cluster)
		if !p.owns(vm, byName[name]) || vm.Status == statusTerminated {
			continue
		}
		if _, exists := matches[name]; exists {
			return nil, fmt.Errorf("lambda: multiple live instances for NodeClaim %s", name)
		}
		matches[name] = vm
	}
	for name, nc := range byName {
		recorded, err := readAttempt(nc)
		if err != nil {
			return nil, err
		}
		if recorded == nil {
			continue
		}
		if vm, present := matches[name]; present {
			if err := p.recordObservation(ctx, nc, recorded, vm.ID); err != nil {
				return nil, err
			}
			continue
		}
		if recorded.InstanceID == "" {
			continue
		}
		if !nc.DeletionTimestamp.IsZero() {
			continue
		}
		vm, err := p.api.get(ctx, recorded.InstanceID)
		if err != nil {
			return nil, classify(err)
		}
		if vm != nil {
			if !p.owns(*vm, nc) {
				return nil, errors.New("lambda: recorded instance does not belong to NodeClaim")
			}
			if err := p.recordObservation(ctx, nc, recorded, vm.ID); err != nil {
				return nil, err
			}
			matches[name] = *vm
		} else if !recorded.Observed {
			matches[name] = instance{ID: recorded.InstanceID, Region: region{Name: nc.Spec.Region}, Status: "booting"}
		}
	}
	out := make([]provider.Instance, len(matches))
	var checks errgroup.Group
	checks.SetLimit(8)
	i := 0
	for name, vm := range matches {
		index := i
		checks.Go(func() error {
			inst := p.observe(ctx, vm, name, true)
			if inst.State == provider.InstanceNotReady {
				wasRunning, err := p.workloadWasRunning(ctx, byName[name])
				if err != nil {
					return err
				}
				if !wasRunning {
					inst.State = provider.InstancePending
				}
			}
			out[index] = inst
			return nil
		})
		i++
	}
	if err := checks.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (p *Provider) recordObservation(ctx context.Context, nc *nebula.NodeClaim, record *attempt, id string) error {
	if record == nil {
		return nil
	}
	if record.InstanceID != "" && record.InstanceID != id {
		return fmt.Errorf("lambda: instance does not match the launch record for NodeClaim %s", nc.Name)
	}
	if record.Observed {
		return nil
	}
	observed := &attempt{Started: record.Started, InstanceID: id, Observed: true}
	if err := p.finishAttempt(ctx, nc, record.Started, observed); err != nil {
		return fmt.Errorf("lambda: record observed launch: %w", err)
	}
	return nil
}

// The Pod carries its previous phase across manager restarts and probe failures.
func (p *Provider) workloadWasRunning(ctx context.Context, nc *nebula.NodeClaim) (bool, error) {
	ref := nc.Spec.PodRef
	var pod corev1.Pod
	err := p.reader.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &pod)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return string(pod.UID) == ref.UID && pod.Status.Phase == corev1.PodRunning, nil
}

func (p *Provider) observe(ctx context.Context, vm instance, name string, probe bool) provider.Instance {
	result := provider.Instance{ID: vm.ID, ClaimName: name, Region: vm.Region.Name,
		CapacityType: nebula.CapacityOnDemand, State: provider.InstancePending}
	switch vm.Status {
	case "active":
		result.State = provider.InstanceNotReady
		// The public address comes from the owned VM record, while the port and path are operator-controlled.
		ip, err := netip.ParseAddr(vm.IP)
		if probe && err == nil && ip.Is4() && ip.IsGlobalUnicast() && !ip.IsPrivate() {
			endpoint := net.JoinHostPort(ip.String(), strconv.Itoa(p.config.port))
			if p.health(ctx, endpoint) {
				result.State, result.Endpoint = provider.InstanceRunning, endpoint
			}
		}
	case "unhealthy", statusTerminating:
		result.State = provider.InstanceNotReady
	case statusTerminated, "preempted":
		result.State = provider.InstanceTerminated
	}
	return result
}

func (*Provider) ClassifyProvisionError(err error, accelerator, region string) provider.BlockScope {
	block := provider.ClassifyError(err, nebula.CapacityOnDemand, accelerator)
	if block.Deny == provider.DenyCandidate || block.Deny == provider.DenyWorkload {
		block.Region = region
	}
	return block
}

func classify(err error) error {
	var apiErr *apiError
	if !errors.As(err, &apiErr) || apiErr.status >= 500 {
		return err
	}
	var category error
	switch apiErr.code {
	case "global/invalid-api-key", "global/account-inactive", "global/forbidden":
		category = provider.ErrAuth
	case codeInsufficientCapacity:
		category = provider.ErrNoCapacity
	case "global/quota-exceeded":
		category = provider.ErrQuota
	case "global/invalid-parameters", "global/invalid-address", codeObjectMissing:
		category = provider.ErrRequestRejected
	default:
		return err
	}
	return fmt.Errorf("%w: %w", category, err)
}
