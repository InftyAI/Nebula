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
	"encoding/base64"
	"fmt"
	"net/http"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// eksTokenPrefix and clusterIDHeader are the EKS token format `aws eks get-token` emits:
	// a presigned STS GetCallerIdentity URL, bound to one cluster by the signed header.
	eksTokenPrefix  = "k8s-aws-v1."
	clusterIDHeader = "x-k8s-aws-id"
	// tokenTTL is how long a minted token is reused. EKS accepts one for 15 minutes; the
	// margin covers clock skew and a request in flight when it is swapped.
	tokenTTL = 10 * time.Minute

	// clusterQPS and clusterBurst are each cluster client's own budget, separate from the
	// home cluster's (see restConfigQPS in cmd/main.go).
	clusterQPS   = 50
	clusterBurst = 100
)

// clusterClients is what the provider needs from one cluster: pods, Secrets, namespaces and
// events, plus discovery to check the node autoscaler is installed.
type clusterClients struct {
	kube k8s.Interface
}

// newEKSClients builds the clients for one EKS cluster from Nebula's ambient AWS identity
// (the SDK's default chain: an access key today, Pod Identity later). Nothing is stored:
// the endpoint and CA come from DescribeCluster, the bearer token from STS.
func newEKSClients(ctx context.Context, cl ClusterConfig) (clusterClients, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cl.Region))
	if err != nil {
		return clusterClients{}, fmt.Errorf("kubernetes: cluster %q: load AWS config: %w", cl.Name, err)
	}
	if cl.RoleARN != "" {
		awsCfg.Credentials = awssdk.NewCredentialsCache(
			stscreds.NewAssumeRoleProvider(sts.NewFromConfig(awsCfg), cl.RoleARN))
	}

	out, err := eks.NewFromConfig(awsCfg).DescribeCluster(ctx,
		&eks.DescribeClusterInput{Name: awssdk.String(cl.Name)})
	if err != nil {
		return clusterClients{}, fmt.Errorf("kubernetes: describe EKS cluster %q in %s: %w", cl.Name, cl.Region, err)
	}
	c := out.Cluster
	if c == nil || c.Endpoint == nil || c.CertificateAuthority == nil || c.CertificateAuthority.Data == nil {
		return clusterClients{}, fmt.Errorf("kubernetes: EKS cluster %q has no endpoint or CA yet", cl.Name)
	}
	ca, err := base64.StdEncoding.DecodeString(*c.CertificateAuthority.Data)
	if err != nil {
		return clusterClients{}, fmt.Errorf("kubernetes: cluster %q: decode CA: %w", cl.Name, err)
	}

	tokens := &eksTokenSource{presign: sts.NewPresignClient(sts.NewFromConfig(awsCfg)), cluster: cl.Name}
	restCfg := &rest.Config{
		Host:            *c.Endpoint,
		TLSClientConfig: rest.TLSClientConfig{CAData: ca},
		QPS:             clusterQPS,
		Burst:           clusterBurst,
		WrapTransport: func(rt http.RoundTripper) http.RoundTripper {
			return &bearerTransport{base: rt, tokens: tokens}
		},
	}
	kube, err := k8s.NewForConfig(restCfg)
	if err != nil {
		return clusterClients{}, fmt.Errorf("kubernetes: cluster %q: build client: %w", cl.Name, err)
	}
	return clusterClients{kube: kube}, nil
}

// eksTokenSource mints and caches the bearer token for one cluster.
type eksTokenSource struct {
	presign *sts.PresignClient
	cluster string

	mu      sync.Mutex
	token   string
	expires time.Time
}

func (s *eksTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && time.Now().Before(s.expires) {
		return s.token, nil
	}
	req, err := s.presign.PresignGetCallerIdentity(ctx, &sts.GetCallerIdentityInput{},
		func(o *sts.PresignOptions) {
			o.ClientOptions = append(o.ClientOptions, func(so *sts.Options) {
				so.APIOptions = append(so.APIOptions, smithyhttp.AddHeaderValue(clusterIDHeader, s.cluster))
			})
		})
	if err != nil {
		return "", fmt.Errorf("kubernetes: presign EKS token for %q: %w", s.cluster, err)
	}
	s.token = eksTokenPrefix + base64.RawURLEncoding.EncodeToString([]byte(req.URL))
	s.expires = time.Now().Add(tokenTTL)
	return s.token, nil
}

// bearerTransport adds the current EKS token to every request.
type bearerTransport struct {
	base   http.RoundTripper
	tokens interface {
		Token(context.Context) (string, error)
	}
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.tokens.Token(req.Context())
	if err != nil {
		return nil, err
	}
	req = req.Clone(req.Context()) // a RoundTripper must not mutate the caller's request
	req.Header.Set("Authorization", "Bearer "+token)
	return t.base.RoundTrip(req)
}

// kubeClient is the typed client; an alias so tests can pass client-go's fake.
type kubeClient = k8s.Interface
