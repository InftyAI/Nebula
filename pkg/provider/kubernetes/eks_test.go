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
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// The token is the shape `aws eks get-token` emits. Each piece is one EKS checks: a missing
// X-Amz-Expires, or a cluster ID that is not signed, is rejected as Unauthorized with no
// further detail.
func TestEKSTokenSource_Format(t *testing.T) {
	cfg := awssdk.Config{
		Region:      "eu-north-1",
		Credentials: credentials.NewStaticCredentialsProvider("AKIDEXAMPLE", "secret", "session"),
	}
	s := &eksTokenSource{presign: sts.NewPresignClient(sts.NewFromConfig(cfg)), cluster: "my-cluster"}

	tok, err := s.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	encoded, ok := strings.CutPrefix(tok, eksTokenPrefix)
	if !ok {
		t.Fatalf("token %q lacks the %q prefix", tok, eksTokenPrefix)
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("token is not unpadded base64url: %v", err)
	}
	u, err := url.Parse(string(raw))
	if err != nil {
		t.Fatalf("token is not a URL: %v", err)
	}
	q := u.Query()

	if u.Host != "sts.eu-north-1.amazonaws.com" || q.Get("Action") != "GetCallerIdentity" {
		t.Errorf("presigned %s?Action=%s, want sts.eu-north-1.amazonaws.com?Action=GetCallerIdentity",
			u.Host, q.Get("Action"))
	}
	if got := q.Get("X-Amz-Expires"); got != presignExpiry {
		t.Errorf("X-Amz-Expires = %q, want %q", got, presignExpiry)
	}
	if signed := strings.Split(q.Get("X-Amz-SignedHeaders"), ";"); !slices.Contains(signed, clusterIDHeader) {
		t.Errorf("signed headers %v lack %s", signed, clusterIDHeader)
	}

	if again, _ := s.Token(context.Background()); again != tok {
		t.Error("a second call within tokenTTL minted a new token")
	}
}

// expiringCreds hands out credentials that expire at a fixed distance from now, counting the
// calls so a test can see when the cache refreshes.
type expiringCreds struct {
	ttl   time.Duration
	calls int
}

func (p *expiringCreds) Retrieve(context.Context) (awssdk.Credentials, error) {
	p.calls++
	return awssdk.Credentials{
		AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "secret", SessionToken: "session",
		CanExpire: true, Expires: time.Now().Add(p.ttl),
	}, nil
}

// A token is reused for tokenTTL and dies with the credentials that signed it, so the cache
// must hand out only credentials that outlive tokenTTL.
func TestCredentialsCacheOptions_OutliveTheToken(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ttl   time.Duration
		calls int
	}{
		{"credentials ending within a token's life are refreshed", tokenTTL + time.Minute, 2},
		{"credentials with time to spare are reused", time.Hour, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &expiringCreds{ttl: tc.ttl}
			cache := awssdk.NewCredentialsCache(p, credentialsCacheOptions)
			for range 2 {
				if _, err := cache.Retrieve(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if p.calls != tc.calls {
				t.Errorf("provider called %d times, want %d", p.calls, tc.calls)
			}
		})
	}
}

// mintingTokens counts mints: a token is reused until invalidated.
type mintingTokens struct {
	token string
	mints int
}

func (m *mintingTokens) Token(context.Context) (string, error) {
	if m.token == "" {
		m.mints++
		m.token = fmt.Sprintf("token-%d", m.mints)
	}
	return m.token, nil
}

func (m *mintingTokens) Invalidate(token string) {
	if m.token == token {
		m.token = ""
	}
}

type statusTransport []int

func (s *statusTransport) RoundTrip(*http.Request) (*http.Response, error) {
	code := (*s)[0]
	*s = (*s)[1:]
	return &http.Response{StatusCode: code, Body: http.NoBody}, nil
}

// A token that went bad before tokenTTL costs one failed request: the 401 drops it and the
// next request mints afresh. Any other answer keeps it.
func TestBearerTransport_DropsTheTokenOn401(t *testing.T) {
	tokens := &mintingTokens{}
	rt := &bearerTransport{base: &statusTransport{200, 403, 401, 200}, tokens: tokens}
	for range 4 {
		req, _ := http.NewRequest(http.MethodGet, "https://example.invalid", nil)
		resp, err := rt.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	if tokens.mints != 2 {
		t.Errorf("minted %d tokens, want 2: one at the start, one after the 401 only", tokens.mints)
	}
}

// A request that fails late must not drop the token another request minted since.
func TestEKSTokenSource_InvalidateKeepsANewerToken(t *testing.T) {
	s := &eksTokenSource{token: "newer", expires: time.Now().Add(tokenTTL)}
	s.Invalidate("older")
	if s.token != "newer" {
		t.Errorf("token = %q, want the newer one kept", s.token)
	}
	s.Invalidate("newer")
	if s.token != "" {
		t.Errorf("token = %q, want it dropped", s.token)
	}
}
