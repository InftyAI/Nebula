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
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	kubefake "k8s.io/client-go/kubernetes/fake"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	"github.com/InftyAI/Nebula/pkg/provider"
)

var trainerRule = ReflectionRule{
	Resources:      []string{resourceSecrets, resourceConfigMaps},
	ObjectSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "trainer"}},
}

func TestReflectionCompile(t *testing.T) {
	sel := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "trainer"}}
	cases := map[string]struct {
		rule    ReflectionRule
		wantErr string
	}{
		"valid":            {rule: trainerRule},
		"allowlist stated": {rule: ReflectionRule{Resources: []string{"secrets"}, ObjectSelector: sel, Mode: "AllowList"}},
		"no resources":     {rule: ReflectionRule{ObjectSelector: sel}, wantErr: "no resources"},
		"other resource":   {rule: ReflectionRule{Resources: []string{"pods"}, ObjectSelector: sel}, wantErr: `"pods"`},
		"denylist": {
			rule:    ReflectionRule{Resources: []string{"secrets"}, ObjectSelector: sel, Mode: "DenyList"},
			wantErr: "not supported",
		},
		"no object selector": {rule: ReflectionRule{Resources: []string{"secrets"}}, wantErr: "objectSelector is required"},
		"empty object selector": {
			rule:    ReflectionRule{Resources: []string{"secrets"}, ObjectSelector: &metav1.LabelSelector{}},
			wantErr: "must not be empty",
		},
		"nebula label": {
			rule: ReflectionRule{Resources: []string{"secrets"},
				ObjectSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"nebula.inftyai.com/reflect": "true"}}},
			wantErr: "Nebula's own",
		},
		"nebula label in an expression": {
			rule: ReflectionRule{Resources: []string{"secrets"},
				ObjectSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "nebula.inftyai.com/reflected", Operator: metav1.LabelSelectorOpExists}}}},
			wantErr: "Nebula's own",
		},
		"bad namespace selector": {
			rule: ReflectionRule{Resources: []string{"secrets"}, ObjectSelector: sel,
				NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "team", Operator: "Near"}}}},
			wantErr: "namespaceSelector",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ReflectionConfig{Rules: []ReflectionRule{tc.rule}}.compile()
			if tc.wantErr == "" && err != nil {
				t.Fatalf("compile: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("compile err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadConfigReflection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "clusters.yaml")
	raw := `clusters:
- geography: us
  host: true
  name: home
  provider: eks
  region: us-east-1
- geography: us
  name: target
  provider: eks
  region: us-west-2
reflection:
  rules:
  - resources: [secrets]
    namespaceSelector:
      matchLabels: {kubernetes.io/metadata.name: org-1}
    objectSelector:
      matchLabels: {app: trainer}
`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if r := cfg.Reflection.Rules; len(r) != 1 || r[0].ObjectSelector.MatchLabels["app"] != "trainer" ||
		r[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "org-1" {
		t.Fatalf("rules = %+v", r)
	}
}

// reflectFixture is a home cluster's caches and two targets, plus a host that is never one.
type reflectFixture struct {
	r                              *reflector
	secrets, configMaps, nsIndexer cache.Indexer
	targets                        map[string]*kubefake.Clientset
	events                         *record.FakeRecorder
}

func newReflectFixture(t *testing.T, rules ...ReflectionRule) *reflectFixture {
	t.Helper()
	compiled, err := ReflectionConfig{Rules: rules}.compile()
	if err != nil {
		t.Fatal(err)
	}
	clusters := []ClusterConfig{
		{Name: "eks-use1", Provider: ClusterProviderEKS, Region: "us-east-1", Geography: "us"},
		{Name: "eks-eun1", Provider: ClusterProviderEKS, Region: "eu-north-1", Geography: "eu"},
		{Name: "host", Provider: ClusterProviderEKS, Region: "us-west-2", Geography: "us", Host: true},
	}
	targets := map[string]*kubefake.Clientset{}
	clients := map[string]clusterClients{}
	for _, cl := range clusters {
		targets[cl.Name] = kubefake.NewClientset()
		clients[cl.Name] = clusterClients{kube: targets[cl.Name]}
	}
	p := New(context.Background(), clusters, clients, testCatalog, nil)
	index := func() cache.Indexer {
		return cache.NewIndexer(cache.MetaNamespaceKeyFunc,
			cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	}
	f := &reflectFixture{
		secrets: index(), configMaps: index(), nsIndexer: index(),
		targets: targets, events: record.NewFakeRecorder(10),
	}
	f.r = &reflector{
		rules: compiled, targets: p.reflectTargets(),
		secrets:    corev1listers.NewSecretLister(f.secrets),
		configMaps: corev1listers.NewConfigMapLister(f.configMaps),
		namespaces: corev1listers.NewNamespaceLister(f.nsIndexer),
		events:     f.events, queue: newReflectQueue(),
		reflectTo: reflectTo, timeout: reflectTimeout,
	}
	t.Cleanup(f.r.queue.ShutDown)
	f.add(t, f.nsIndexer, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "org-1", Labels: map[string]string{"kubernetes.io/metadata.name": "org-1", "team": "research"}}})
	return f
}

func (f *reflectFixture) add(t *testing.T, idx cache.Indexer, obj any) {
	t.Helper()
	if err := idx.Add(obj); err != nil {
		t.Fatal(err)
	}
}

func (f *reflectFixture) handle(t *testing.T, resource, name string) {
	t.Helper()
	if err := f.r.handle(context.Background(), objectKey{resource, "org-1", name}); err != nil {
		t.Fatalf("handle: %v", err)
	}
}

func (f *reflectFixture) targetSecret(cluster, name string) (*corev1.Secret, error) {
	return f.targets[cluster].CoreV1().Secrets("org-1").Get(context.Background(), name, metav1.GetOptions{})
}

func trainerSecret(data string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "hf", Namespace: "org-1", UID: "src-uid",
			Labels: map[string]string{"app": "trainer"}},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"token": []byte(data)},
	}
}

// A selected Secret is copied into its namespace, created if missing, in every target but the
// host, and follows its source.
func TestReflectCopiesToEveryTarget(t *testing.T) {
	f := newReflectFixture(t, trainerRule)
	f.add(t, f.secrets, trainerSecret("one"))
	f.handle(t, resourceSecrets, "hf")

	for _, cl := range []string{"eks-use1", "eks-eun1"} {
		got, err := f.targetSecret(cl, "hf")
		if err != nil || string(got.Data["token"]) != "one" || !isReflected(got) || got.Labels["app"] != "trainer" {
			t.Fatalf("%s: copy = %+v, %v; want the data, the source's labels and Nebula's", cl, got, err)
		}
		if _, err := f.targets[cl].CoreV1().Namespaces().Get(context.Background(), "org-1", metav1.GetOptions{}); err != nil {
			t.Errorf("%s: namespace not created: %v", cl, err)
		}
	}
	if _, err := f.targetSecret("host", "hf"); !apierrors.IsNotFound(err) {
		t.Errorf("host got a copy: %v", err)
	}

	f.add(t, f.secrets, trainerSecret("two"))
	f.handle(t, resourceSecrets, "hf")
	if got, _ := f.targetSecret("eks-use1", "hf"); string(got.Data["token"]) != "two" {
		t.Errorf("copy = %q, want it updated", got.Data["token"])
	}
}

// A copy's labels follow its source's, except a key under Nebula's prefix, which Nebula sets alone.
func TestReflectCopiesTheSourceLabels(t *testing.T) {
	f := newReflectFixture(t, trainerRule)
	s := trainerSecret("one")
	s.Labels["example.com/experiment-id"] = "exp-1"
	s.Labels["nebula.inftyai.com/reflected"] = "false"
	f.add(t, f.secrets, s)
	f.handle(t, resourceSecrets, "hf")
	got, err := f.targetSecret("eks-use1", "hf")
	if err != nil || !isReflected(got) || got.Labels["example.com/experiment-id"] != "exp-1" {
		t.Fatalf("copy labels = %v, %v", got.Labels, err)
	}

	s = trainerSecret("one")
	s.Labels["example.com/experiment-id"] = "exp-2"
	f.add(t, f.secrets, s)
	f.handle(t, resourceSecrets, "hf")
	got, _ = f.targetSecret("eks-use1", "hf")
	want := map[string]string{"app": "trainer", "example.com/experiment-id": "exp-2", reflectedLabel: labelTrue}
	if !maps.Equal(got.Labels, want) {
		t.Errorf("copy labels = %v, want %v", got.Labels, want)
	}
}

func TestReflectConfigMap(t *testing.T) {
	f := newReflectFixture(t, trainerRule)
	f.add(t, f.configMaps, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "conf", Namespace: "org-1", Labels: map[string]string{"app": "trainer"}},
		Data:       map[string]string{"k": "v"}, BinaryData: map[string][]byte{"b": {1}},
	})
	f.handle(t, resourceConfigMaps, "conf")
	got, err := f.targets["eks-eun1"].CoreV1().ConfigMaps("org-1").Get(context.Background(), "conf", metav1.GetOptions{})
	if err != nil || got.Data["k"] != "v" || len(got.BinaryData["b"]) != 1 || !isReflected(got) {
		t.Fatalf("copy = %+v, %v", got, err)
	}
}

// What no rule selects isn't copied: a rule for another resource, labels or namespace.
func TestReflectOnlyWhatARuleSelects(t *testing.T) {
	cases := map[string]ReflectionRule{
		"other resource": {Resources: []string{resourceConfigMaps}, ObjectSelector: trainerRule.ObjectSelector},
		"other labels": {Resources: []string{resourceSecrets},
			ObjectSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "server"}}},
		"other namespace": {Resources: []string{resourceSecrets}, ObjectSelector: trainerRule.ObjectSelector,
			NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "infra"}}},
	}
	for name, rule := range cases {
		t.Run(name, func(t *testing.T) {
			f := newReflectFixture(t, rule)
			f.add(t, f.secrets, trainerSecret("one"))
			f.handle(t, resourceSecrets, "hf")
			if _, err := f.targetSecret("eks-use1", "hf"); !apierrors.IsNotFound(err) {
				t.Fatalf("copied although unselected: %v", err)
			}
		})
	}
}

// A copy goes once its source is deleted or no longer selected.
func TestReflectDeletesTheCopy(t *testing.T) {
	cases := map[string]func(f *reflectFixture, t *testing.T){
		"deleted": func(f *reflectFixture, t *testing.T) {
			if err := f.secrets.Delete(trainerSecret("one")); err != nil {
				t.Fatal(err)
			}
		},
		"unselected": func(f *reflectFixture, t *testing.T) {
			s := trainerSecret("one")
			s.Labels = nil
			f.add(t, f.secrets, s)
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			f := newReflectFixture(t, trainerRule)
			f.add(t, f.secrets, trainerSecret("one"))
			f.handle(t, resourceSecrets, "hf")
			change(f, t)
			f.handle(t, resourceSecrets, "hf")
			for _, cl := range []string{"eks-use1", "eks-eun1"} {
				if _, err := f.targetSecret(cl, "hf"); !apierrors.IsNotFound(err) {
					t.Errorf("%s: copy still there: %v", cl, err)
				}
			}
		})
	}
}

// An object of the source's name that isn't a copy is never written or deleted, only reported.
func TestReflectLeavesWhatIsNotACopy(t *testing.T) {
	f := newReflectFixture(t, trainerRule)
	theirs := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "hf", Namespace: "org-1"},
		Data:       map[string][]byte{"theirs": []byte("keep")},
	}
	if err := f.targets["eks-use1"].Tracker().Add(theirs); err != nil {
		t.Fatal(err)
	}
	f.add(t, f.secrets, trainerSecret("one"))
	f.handle(t, resourceSecrets, "hf")
	if got, _ := f.targetSecret("eks-use1", "hf"); string(got.Data["theirs"]) != "keep" || len(got.Data) != 1 {
		t.Fatalf("their Secret was changed: %+v", got.Data)
	}
	if got, err := f.targetSecret("eks-eun1", "hf"); err != nil || !isReflected(got) {
		t.Errorf("the other target wasn't reflected to: %v", err)
	}
	select {
	case e := <-f.events.Events:
		if !strings.Contains(e, reasonReflectionRefused) || !strings.Contains(e, "eks-use1") {
			t.Errorf("event = %q", e)
		}
	default:
		t.Error("no event for the refusal")
	}

	if err := f.secrets.Delete(trainerSecret("one")); err != nil {
		t.Fatal(err)
	}
	f.handle(t, resourceSecrets, "hf")
	if _, err := f.targetSecret("eks-use1", "hf"); err != nil {
		t.Fatalf("their Secret was deleted: %v", err)
	}
}

// A Secret's type can't be updated, so a copy whose source changed type is replaced.
func TestReflectReplacesACopyOfAnotherType(t *testing.T) {
	f := newReflectFixture(t, trainerRule)
	f.add(t, f.secrets, trainerSecret("one"))
	f.handle(t, resourceSecrets, "hf")
	s := trainerSecret("one")
	s.Type = corev1.SecretTypeDockerConfigJson
	f.add(t, f.secrets, s)
	f.handle(t, resourceSecrets, "hf")
	if got, err := f.targetSecret("eks-use1", "hf"); err != nil || got.Type != corev1.SecretTypeDockerConfigJson {
		t.Fatalf("copy = %+v, %v; want the new type", got, err)
	}
}

// The home cluster listed as a target without host: its source is found there, and left as is.
func TestReflectSkipsTheSourceItself(t *testing.T) {
	f := newReflectFixture(t, trainerRule)
	src := trainerSecret("one")
	if err := f.targets["eks-use1"].Tracker().Add(src); err != nil {
		t.Fatal(err)
	}
	f.add(t, f.secrets, src)
	f.handle(t, resourceSecrets, "hf")
	got, _ := f.targetSecret("eks-use1", "hf")
	if isReflected(got) || got.UID != types.UID("src-uid") {
		t.Fatalf("the source was overwritten: %+v", got)
	}
	if len(f.events.Events) != 0 {
		t.Errorf("the source itself was reported as a refusal: %q", <-f.events.Events)
	}
}

// A source's labels changing away from a rule queues it, so its copies are deleted.
func TestReflectQueuesAnUnselectedUpdate(t *testing.T) {
	f := newReflectFixture(t, trainerRule)
	old := trainerSecret("one")
	old.ResourceVersion = "1"
	cur := old.DeepCopy()
	cur.ResourceVersion, cur.Labels = "2", nil
	f.r.objectEvents(resourceSecrets).OnUpdate(old, cur)
	if f.r.queue.Len() != 1 {
		t.Fatalf("queued %d, want the source", f.r.queue.Len())
	}
	other := cur.DeepCopy()
	other.ResourceVersion = "3"
	// Done, or a re-add would only mark the key dirty and leave Len at 0.
	k, _ := f.r.queue.Get()
	f.r.queue.Done(k)
	f.r.objectEvents(resourceSecrets).OnUpdate(cur, other)
	if f.r.queue.Len() != 0 {
		t.Fatal("queued a source never selected")
	}
}

// The sweep queues every selectable source and every copy a target holds, orphans included.
func TestReflectSweepQueuesSourcesAndCopies(t *testing.T) {
	f := newReflectFixture(t, trainerRule)
	f.add(t, f.secrets, trainerSecret("one"))
	f.add(t, f.secrets, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "org-1"}})
	orphan := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "gone", Namespace: "org-1", Labels: reflectedLabels()}}
	if err := f.targets["eks-eun1"].Tracker().Add(orphan); err != nil {
		t.Fatal(err)
	}
	f.r.sweep(context.Background())
	got := map[objectKey]bool{}
	for f.r.queue.Len() > 0 {
		k, _ := f.r.queue.Get()
		got[k] = true
		f.r.queue.Done(k)
	}
	want := map[objectKey]bool{{resourceSecrets, "org-1", "hf"}: true, {resourceConfigMaps, "org-1", "gone"}: true}
	if len(got) != len(want) || !got[objectKey{resourceSecrets, "org-1", "hf"}] ||
		!got[objectKey{resourceConfigMaps, "org-1", "gone"}] {
		t.Fatalf("queued %v, want %v", got, want)
	}

	f.handle(t, resourceConfigMaps, "gone")
	if _, err := f.targets["eks-eun1"].CoreV1().ConfigMaps("org-1").Get(context.Background(), "gone",
		metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("orphan still there: %v", err)
	}
}

// Reflect end to end, on informers: a source written in the home cluster reaches a target, and its
// deletion removes the copy.
func TestReflectFromInformers(t *testing.T) {
	f := newReflectFixture(t, trainerRule)
	home := kubefake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "org-1"}},
		trainerSecret("one"),
	)
	factory := informers.NewSharedInformerFactory(home, 0)
	p := &Provider{reflection: f.r.rules, clusters: map[string]cluster{}, order: nil}
	for _, cl := range f.r.targets {
		p.clusters[cl.cfg.Region] = cl
		p.order = append(p.order, cl.cfg.Region)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := p.Reflect(ctx, provider.ReflectSources{
		Client: home, Secrets: factory.Core().V1().Secrets(), ConfigMaps: factory.Core().V1().ConfigMaps(),
		Namespaces: factory.Core().V1().Namespaces(),
	})
	if err != nil {
		t.Fatal(err)
	}
	factory.Start(ctx.Done())

	waitFor := func(what string, done func() bool) {
		t.Helper()
		if err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 5*time.Second, true,
			func(context.Context) (bool, error) { return done(), nil }); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	waitFor("copy written", func() bool {
		got, err := f.targetSecret("eks-eun1", "hf")
		return err == nil && string(got.Data["token"]) == "one"
	})
	if err := home.CoreV1().Secrets("org-1").Delete(ctx, "hf", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor("copy deleted", func() bool {
		_, err := f.targetSecret("eks-eun1", "hf")
		return apierrors.IsNotFound(err)
	})
}

// Each target gets its own deadline: one that stalls past it leaves the next its full budget.
func TestReflectTimesOutEachTargetAlone(t *testing.T) {
	f := newReflectFixture(t, trainerRule)
	f.add(t, f.secrets, trainerSecret("one"))
	f.r.timeout = 20 * time.Millisecond
	var live []bool
	f.r.reflectTo = func(ctx context.Context, _ kubeClient, _ objectKey, _ runtime.Object) (bool, error) {
		if len(live) == 0 {
			<-ctx.Done() // the first target stalls until its deadline
		}
		live = append(live, ctx.Err() == nil)
		return false, ctx.Err()
	}
	err := f.r.handle(context.Background(), objectKey{resourceSecrets, "org-1", "hf"})
	if len(live) != 2 || live[0] || !live[1] {
		t.Fatalf("contexts live = %v, want the stalled target's expired and the next one's live", live)
	}
	if err == nil || !strings.Contains(err.Error(), "eks-use1") || strings.Contains(err.Error(), "eks-eun1") {
		t.Errorf("err = %v, want only the stalled target's failure", err)
	}
}
