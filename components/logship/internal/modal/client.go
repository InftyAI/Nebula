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

package modal

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/modal-labs/modal-client/go/proto/modal_proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// sdkVersion is the modal-client version this mimics in the headers below. Kept in sync with
// go.mod by TestSDKVersionMatchesModulePin rather than by remembering.
const sdkVersion = "0.9.0"

const defaultServerURL = "https://api.modal.com:443"

// keepalive matches the SDK's. A log stream is idle for its whole 55-second window whenever the
// sandbox is quiet, so PermitWithoutStream is not optional decoration here.
var keepaliveParams = keepalive.ClientParameters{
	Time:                30 * time.Second,
	Timeout:             10 * time.Second,
	PermitWithoutStream: true,
}

// LogsClient is the one RPC this package needs off pb.ModalClientClient.
//
// Narrowed so a test can supply a fake without implementing several hundred methods, and so the
// blast radius of talking to the raw proto is one line.
type LogsClient interface {
	SandboxGetLogs(context.Context, *pb.SandboxGetLogsRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[pb.TaskLogsBatch], error)
}

// Credentials are the Modal tokens, and the server to send them to.
type Credentials struct {
	TokenID     string
	TokenSecret string
	ServerURL   string
}

// CredentialsFromEnv reads the tokens the SDK would read, minus the profile file.
//
// Env-only on purpose: the SDK's loader also merges ~/.modal.toml, and it is unexported, so there
// is nothing to reuse. In-cluster the tokens arrive as a Secret, which is env either way — what
// diverges is a local run against a toml profile, which has to export the two variables.
func CredentialsFromEnv() (Credentials, error) {
	c := Credentials{
		TokenID:     os.Getenv("MODAL_TOKEN_ID"),
		TokenSecret: os.Getenv("MODAL_TOKEN_SECRET"),
		ServerURL:   os.Getenv("MODAL_SERVER_URL"),
	}
	if c.TokenID == "" || c.TokenSecret == "" {
		return Credentials{}, fmt.Errorf("MODAL_TOKEN_ID and MODAL_TOKEN_SECRET must be set")
	}
	if c.ServerURL == "" {
		c.ServerURL = defaultServerURL
	}
	return c, nil
}

// Dial opens a control-plane connection for reading logs. The caller closes the conn.
//
// Its own connection because the SDK's is unexported (Client.cpClient, with no accessor), and
// cheap to replace for this one RPC: the SDK installs four unary interceptors but exactly ONE
// stream interceptor, and all that one does is attach the static headers below. The rotating-JWT
// machinery is unary-only, so a SandboxGetLogs stream needs none of it.
func Dial(creds Credentials) (*grpc.ClientConn, LogsClient, error) {
	target, transport, err := dialTarget(creds.ServerURL)
	if err != nil {
		return nil, nil, err
	}
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(transport),
		grpc.WithKeepaliveParams(keepaliveParams),
		grpc.WithUnaryInterceptor(headerInjectorUnary(creds)),
		grpc.WithStreamInterceptor(headerInjectorStream(creds)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("dialing modal at %s: %w", creds.ServerURL, err)
	}
	return conn, pb.NewModalClientClient(conn), nil
}

func dialTarget(serverURL string) (string, credentials.TransportCredentials, error) {
	if after, ok := strings.CutPrefix(serverURL, "https://"); ok {
		return after, credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}), nil
	}
	// Plaintext is how the SDK reaches a local test server, and the only reason it is allowed.
	if after, ok := strings.CutPrefix(serverURL, "http://"); ok {
		return after, insecure.NewCredentials(), nil
	}
	return "", nil, fmt.Errorf("invalid MODAL_SERVER_URL %q: want an http:// or https:// prefix", serverURL)
}

// authHeaders are the five the SDK sends, reproduced rather than invented: the client type and
// version are what a server-side minimum-version check would read, so identifying ourselves here
// instead risks a rejection we would have no way to diagnose. They pin to the SDK we vendor the
// proto from, and move when it does.
func authHeaders(creds Credentials) []string {
	return []string{
		"x-modal-client-type", strconv.Itoa(int(pb.ClientType_CLIENT_TYPE_LIBMODAL_GO)),
		"x-modal-client-version", "1.0.0",
		"x-modal-libmodal-version", "modal-go/" + sdkVersion,
		"x-modal-token-id", creds.TokenID,
		"x-modal-token-secret", creds.TokenSecret,
	}
}

func headerInjectorUnary(creds Credentials) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(metadata.AppendToOutgoingContext(ctx, authHeaders(creds)...), method, req, reply, cc, opts...)
	}
}

func headerInjectorStream(creds Credentials) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(metadata.AppendToOutgoingContext(ctx, authHeaders(creds)...), desc, cc, method, opts...)
	}
}
