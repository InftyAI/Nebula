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
	"net/url"
	"slices"
	"strings"
	"testing"

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
