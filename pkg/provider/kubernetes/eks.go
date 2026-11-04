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
	"strings"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"k8s.io/apimachinery/pkg/runtime"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// eksTokenPrefix and clusterIDHeader are the EKS token format `aws eks get-token` emits:
	// a presigned STS GetCallerIdentity URL, bound to one cluster by the signed header.
	eksTokenPrefix  = "k8s-aws-v1."
	clusterIDHeader = "x-k8s-aws-id"
	// expiresHeader carries presignExpiry, the value `aws eks get-token` sends. EKS bounds a
	// token's life itself (see tokenTTL), not by this.
	expiresHeader = "X-Amz-Expires"
	presignExpiry = "60"
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
	// ec2 answers which instance families the cluster's region offers (see offeredFamilies);
	// nil keeps every family the catalog has.
	ec2 describeOfferings
}

// describeOfferings is the one EC2 call used, so a test can stand in for the client.
type describeOfferings interface {
	DescribeInstanceTypeOfferings(context.Context, *ec2.DescribeInstanceTypeOfferingsInput,
		...func(*ec2.Options)) (*ec2.DescribeInstanceTypeOfferingsOutput, error)
}

// offeredFamilies returns which of families the client's region offers any instance type of.
func offeredFamilies(ctx context.Context, api describeOfferings, families []string) (map[string]bool, error) {
	patterns := make([]string, len(families))
	for i, f := range families {
		patterns[i] = f + ".*" // the family's own types only, never another family's prefix
	}
	found := map[string]bool{}
	pages := ec2.NewDescribeInstanceTypeOfferingsPaginator(api, &ec2.DescribeInstanceTypeOfferingsInput{
		LocationType: ec2types.LocationTypeRegion,
		Filters:      []ec2types.Filter{{Name: awssdk.String("instance-type"), Values: patterns}},
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("kubernetes: describe instance type offerings: %w", err)
		}
		for _, off := range page.InstanceTypeOfferings {
			family, _, _ := strings.Cut(string(off.InstanceType), ".")
			found[family] = true
		}
	}
	return found, nil
}

// newEKSClients builds the clients for one EKS cluster from Nebula's ambient AWS identity (the
// SDK's default chain, the controller's IRSA role in a deployment), which must be in the
// cluster's account: EKS has no resource policy to grant DescribeCluster across accounts.
// Nothing is stored: the endpoint and CA come from DescribeCluster, the bearer token from STS.
func newEKSClients(ctx context.Context, cl ClusterConfig) (clusterClients, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cl.Region))
	if err != nil {
		return clusterClients{}, fmt.Errorf("kubernetes: cluster %q: load AWS config: %w", cl.Name, err)
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
		// Protobuf for the built-in types read every poll (pods, events): several times smaller
		// and cheaper to decode than JSON. Custom resources only speak JSON, hence the fallback.
		ContentConfig: rest.ContentConfig{
			ContentType:        runtime.ContentTypeProtobuf,
			AcceptContentTypes: runtime.ContentTypeProtobuf + "," + runtime.ContentTypeJSON,
		},
		WrapTransport: func(rt http.RoundTripper) http.RoundTripper {
			return &bearerTransport{base: rt, tokens: tokens}
		},
	}
	kube, err := k8s.NewForConfig(restCfg)
	if err != nil {
		return clusterClients{}, fmt.Errorf("kubernetes: cluster %q: build client: %w", cl.Name, err)
	}
	return clusterClients{kube: kube, ec2: ec2.NewFromConfig(awsCfg)}, nil
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
	// The STS presign client sets no expiry, and EKS rejects a token without one as
	// Unauthorized. As a header it is hoisted into the query like every X-Amz-* one.
	req, err := s.presign.PresignGetCallerIdentity(ctx, &sts.GetCallerIdentityInput{},
		func(o *sts.PresignOptions) {
			o.ClientOptions = append(o.ClientOptions, func(so *sts.Options) {
				so.APIOptions = append(so.APIOptions,
					smithyhttp.SetHeaderValue(clusterIDHeader, s.cluster),
					smithyhttp.SetHeaderValue(expiresHeader, presignExpiry))
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
