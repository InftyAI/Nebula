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
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

func (p *Provider) updateAttempt(ctx context.Context, nc *nebula.NodeClaim, a *attempt) error {
	next := nc.DeepCopy()
	if next.Annotations == nil {
		next.Annotations = map[string]string{}
	}
	if a == nil {
		delete(next.Annotations, attemptAnnotation)
	} else {
		value, err := json.Marshal(a)
		if err != nil {
			return err
		}
		next.Annotations[attemptAnnotation] = string(value)
		// A Pod delete may race the NodeClaim controller's first reconcile.
		controllerutil.AddFinalizer(next, nebula.TerminateInstanceFinalizer)
	}
	err := p.writer.Patch(ctx, next, client.MergeFromWithOptions(nc, client.MergeFromWithOptimisticLock{}))
	return err
}

func (p *Provider) finishAttempt(ctx context.Context, nc *nebula.NodeClaim, started time.Time, a *attempt) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for {
		current, err := p.claim(ctx, nc.Name)
		if err != nil {
			return err
		}
		if current == nil || current.UID != nc.UID {
			return errors.New("lambda: NodeClaim changed while recording launch result")
		}
		recorded, err := readAttempt(current)
		if err != nil {
			return err
		}
		if recorded == nil || !recorded.Started.Equal(started) {
			return errors.New("lambda: NodeClaim launch record changed")
		}
		if recorded.InstanceID != "" {
			if a == nil || recorded.InstanceID != a.InstanceID {
				return errors.New("lambda: NodeClaim already records another launch result")
			}
			if recorded.Observed || !a.Observed {
				return nil
			}
		}
		err = p.updateAttempt(ctx, current, a)
		if !apierrors.IsConflict(err) {
			return err
		}
	}
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
