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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	nebula "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

const (
	attemptAnnotation = "nebula.inftyai.com/lambda-launch"
)

var errLaunchUnresolved = fmt.Errorf("lambda: waiting for the instance to appear: %w", provider.ErrProvisionUnresolved)

type attempt struct {
	Started    time.Time `json:"started"`
	InstanceID string    `json:"instanceID,omitempty"`
	Observed   bool      `json:"observed,omitempty"`
	Abandoned  bool      `json:"abandoned,omitempty"`
}

func readAttempt(nc *nebula.NodeClaim) (*attempt, error) {
	value := nc.Annotations[attemptAnnotation]
	if value == "" {
		return nil, nil
	}
	var a attempt
	if err := json.Unmarshal([]byte(value), &a); err != nil || a.Started.IsZero() {
		return nil, fmt.Errorf("lambda: invalid launch record for NodeClaim %s", nc.Name)
	}
	return &a, nil
}

func (p *Provider) claim(ctx context.Context, name string) (*nebula.NodeClaim, error) {
	var nc nebula.NodeClaim
	if err := p.reader.Get(ctx, client.ObjectKey{Name: name}, &nc); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return &nc, nil
}

func (p *Provider) updateAttempt(ctx context.Context, nc *nebula.NodeClaim, a attempt) error {
	value, err := json.Marshal(a)
	if err != nil {
		return err
	}
	next := nc.DeepCopy()
	if next.Annotations == nil {
		next.Annotations = map[string]string{}
	}
	next.Annotations[attemptAnnotation] = string(value)
	if !a.Abandoned {
		// A Pod delete may race the NodeClaim controller's first reconcile.
		controllerutil.AddFinalizer(next, nebula.TerminateInstanceFinalizer)
	}
	return p.writer.Patch(ctx, next, client.MergeFromWithOptions(nc, client.MergeFromWithOptimisticLock{}))
}

// abandonAttempt records that this invocation sent no launch or got a definite
// refusal. Retain that evidence for the next reconciliation if Kubernetes is down.
func (p *Provider) abandonAttempt(ctx context.Context, nc *nebula.NodeClaim, started time.Time) error {
	if err := p.finishAttempt(ctx, nc, attempt{Started: started, Abandoned: true}); err != nil {
		p.abandonedMu.Lock()
		if p.abandoned == nil {
			p.abandoned = make(map[types.UID]map[time.Time]struct{})
		}
		if p.abandoned[nc.UID] == nil {
			p.abandoned[nc.UID] = make(map[time.Time]struct{})
		}
		p.abandoned[nc.UID][started] = struct{}{}
		p.abandonedMu.Unlock()
		return err
	}
	return nil
}

func (p *Provider) currentAttempt(ctx context.Context, nc *nebula.NodeClaim) (*attempt, error) {
	p.abandonedMu.Lock()
	pending := make([]time.Time, 0, len(p.abandoned[nc.UID]))
	for started := range p.abandoned[nc.UID] {
		pending = append(pending, started)
	}
	p.abandonedMu.Unlock()
	if len(pending) == 0 {
		return readAttempt(nc)
	}
	for _, started := range pending {
		if err := p.finishAttempt(ctx, nc, attempt{Started: started, Abandoned: true}); err != nil {
			return nil, fmt.Errorf("lambda: retry recording abandoned launch for NodeClaim %s: %w", nc.Name, err)
		}
		p.abandonedMu.Lock()
		delete(p.abandoned[nc.UID], started)
		if len(p.abandoned[nc.UID]) == 0 {
			delete(p.abandoned, nc.UID)
		}
		p.abandonedMu.Unlock()
	}
	current, err := p.claim(ctx, nc.Name)
	if current == nil || err != nil {
		return nil, err
	}
	if current.UID != nc.UID {
		return nil, errors.New("lambda: NodeClaim was replaced while recording abandoned launch")
	}
	return readAttempt(current)
}

func (p *Provider) finishAttempt(ctx context.Context, nc *nebula.NodeClaim, a attempt) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	backoff := wait.Backoff{Duration: 50 * time.Millisecond, Factor: 2, Steps: 5}
	return retry.OnError(backoff, retryableAttemptWrite, func() error {
		current, err := p.claim(ctx, nc.Name)
		if err != nil {
			return err
		}
		if current == nil || current.UID != nc.UID {
			if a.Abandoned {
				return nil
			}
			return errors.New("lambda: NodeClaim changed while recording launch result")
		}
		recorded, err := readAttempt(current)
		if err != nil {
			return err
		}
		if a.Abandoned {
			if recorded != nil && (!recorded.Started.Equal(a.Started) || recorded.InstanceID != "" || recorded.Abandoned) {
				return nil
			}
			// Write even if the record is absent. Changing the resourceVersion
			// prevents an unacknowledged reservation from committing after this read.
			return p.updateAttempt(ctx, current, a)
		}
		if recorded == nil || !recorded.Started.Equal(a.Started) || recorded.Abandoned && !a.Observed {
			return errors.New("lambda: NodeClaim launch record changed")
		}
		if recorded.InstanceID != "" {
			if recorded.InstanceID != a.InstanceID {
				return errors.New("lambda: NodeClaim already records another launch result")
			}
			if recorded.Observed || !a.Observed {
				return nil
			}
		}
		return p.updateAttempt(ctx, current, a)
	})
}

func retryableAttemptWrite(err error) bool {
	if apierrors.IsConflict(err) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) ||
		apierrors.IsServiceUnavailable(err) || apierrors.IsTooManyRequests(err) || apierrors.IsInternalError(err) {
		return true
	}
	var timeout net.Error
	return errors.As(err, &timeout) && timeout.Timeout()
}

func instanceName(nc *nebula.NodeClaim) string {
	sum := sha256.Sum256([]byte(nc.Name + "/" + string(nc.UID)))
	return "nebula-" + hex.EncodeToString(sum[:])[:48]
}

func claimTags(cluster string, nc *nebula.NodeClaim) []tag {
	first := nc.Name[:min(len(nc.Name), 128)]
	tags := []tag{
		{Key: "nebula-cluster", Value: cluster},
		{Key: "nebula-claim-uid", Value: string(nc.UID)},
		{Key: "nebula-claim-0", Value: first},
	}
	if len(first) < len(nc.Name) {
		tags = append(tags, tag{Key: "nebula-claim-1", Value: nc.Name[len(first):]})
	}
	return tags
}

func scopedClaim(vm instance, cluster string) (name, uid string) {
	var first, second, owner string
	seen := map[string]bool{}
	for _, t := range vm.Tags {
		if !strings.HasPrefix(t.Key, "nebula-") {
			continue
		}
		if seen[t.Key] {
			return "", ""
		}
		seen[t.Key] = true
		switch t.Key {
		case "nebula-cluster":
			owner = t.Value
		case "nebula-claim-uid":
			uid = t.Value
		case "nebula-claim-0":
			first = t.Value
		case "nebula-claim-1":
			second = t.Value
		}
	}
	if owner != cluster || uid == "" || first == "" || len(first)+len(second) > 253 {
		return "", ""
	}
	return first + second, uid
}

func (p *Provider) owns(vm instance, nc *nebula.NodeClaim) bool {
	if nc == nil || nc.Spec.Provider != provider.ProviderLambda || nc.Spec.Region != p.config.region ||
		vm.Workspace != p.config.workspace || vm.Region.Name != p.config.region {
		return false
	}
	name, uid := scopedClaim(vm, p.config.cluster)
	return name == nc.Name && uid == string(nc.UID)
}
