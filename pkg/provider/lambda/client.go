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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const (
	apiURL            = "https://cloud.lambda.com/api/v1"
	maxResponseSize   = 16 << 20
	codeObjectMissing = "global/object-does-not-exist"
)

type cloudClient struct {
	baseURL  string
	token    string
	http     *http.Client
	requests *rate.Limiter
	launches *rate.Limiter
}

func newCloudClient(token string) *cloudClient {
	return &cloudClient{
		baseURL: apiURL, token: token,
		http:     &http.Client{Timeout: 20 * time.Second, CheckRedirect: refuseRedirect},
		requests: rate.NewLimiter(rate.Every(time.Second), 1),
		launches: rate.NewLimiter(rate.Every(12*time.Second), 1),
	}
}

func refuseRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

type tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type launchRequest struct {
	Region           string              `json:"region_name"`
	InstanceType     string              `json:"instance_type_name"`
	SSHKeyNames      []string            `json:"ssh_key_names"`
	Name             string              `json:"name"`
	Image            map[string]string   `json:"image"`
	UserData         string              `json:"user_data"`
	Tags             []tag               `json:"tags"`
	FirewallRulesets []map[string]string `json:"firewall_rulesets"`
	Workspace        string              `json:"workspace_id"`
}

type region struct {
	Name string `json:"name"`
}

type instance struct {
	ID        string `json:"id"`
	IP        string `json:"ip"`
	Status    string `json:"status"`
	Region    region `json:"region"`
	Workspace string `json:"workspace_id"`
	Tags      []tag  `json:"tags"`
}

type instanceType struct {
	Architecture string `json:"architecture"`
	Specs        struct {
		VCPUs     int   `json:"vcpus"`
		MemoryGiB int   `json:"memory_gib"`
		GPUs      int32 `json:"gpus"`
	} `json:"specs"`
}

type instanceTypeEntry struct {
	Type    instanceType `json:"instance_type"`
	Regions []region     `json:"regions_with_capacity_available"`
}

type response[T any] struct {
	Data      T      `json:"data"`
	PageToken string `json:"page_token"`
}

type apiError struct {
	status    int
	code      string
	requestID string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("lambda API: HTTP %d (%s, request %s)", e.status, e.code, e.requestID)
}

var safeIdentifier = regexp.MustCompile(`^[a-zA-Z0-9_./:-]{1,128}$`)

func safeField(value string) string {
	if safeIdentifier.MatchString(value) {
		return value
	}
	return "unknown"
}

var errRequestNotSent = errors.New("lambda API: request not sent")

func (c *cloudClient) reserveLaunch(ctx context.Context) error {
	if c.launches == nil {
		return nil
	}
	return c.launches.Wait(ctx)
}

func (c *cloudClient) call(ctx context.Context, method, path string, body any, out any) error {
	var input io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("%w: marshal request", errRequestNotSent)
		}
		input = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, input)
	if err != nil {
		return fmt.Errorf("%w: build request", errRequestNotSent)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.requests != nil {
		if err := c.requests.Wait(ctx); err != nil {
			return fmt.Errorf("%w: wait for rate limiter: %w", errRequestNotSent, err)
		}
	}
	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("lambda API transport unavailable: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var failure struct {
			Error struct {
				Code      string `json:"code"`
				RequestID string `json:"request_id"`
			} `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&failure)
		return &apiError{
			status: res.StatusCode, code: safeField(failure.Error.Code), requestID: safeField(failure.Error.RequestID),
		}
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, maxResponseSize+1))
	if err != nil || len(data) > maxResponseSize || json.Unmarshal(data, out) != nil {
		return errors.New("lambda API: invalid success response")
	}
	return nil
}

func (c *cloudClient) launch(ctx context.Context, req launchRequest) (string, error) {
	var reply response[struct {
		IDs []string `json:"instance_ids"`
	}]
	if err := c.call(ctx, http.MethodPost, "/instance-operations/launch", req, &reply); err != nil {
		return "", err
	}
	if len(reply.Data.IDs) != 1 || strings.TrimSpace(reply.Data.IDs[0]) == "" {
		return "", errors.New("lambda API: launch response did not identify exactly one instance")
	}
	return reply.Data.IDs[0], nil
}

func (c *cloudClient) get(ctx context.Context, id string) (*instance, error) {
	var reply response[*instance]
	err := c.call(ctx, http.MethodGet, "/instances/"+url.PathEscape(id), nil, &reply)
	var apiErr *apiError
	if errors.As(err, &apiErr) && apiErr.status == http.StatusNotFound &&
		(apiErr.code == codeObjectMissing || apiErr.code == "global/not-found") {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if reply.Data == nil || reply.Data.ID != id {
		return nil, errors.New("lambda API: invalid instance response")
	}
	return reply.Data, nil
}

func (c *cloudClient) list(ctx context.Context, workspace string) ([]instance, error) {
	var instances []instance
	seen := make(map[string]bool)
	query := make(url.Values)
	if workspace != "" {
		query.Set("workspace_id", workspace)
	}
	for {
		var page response[[]instance]
		if err := c.call(ctx, http.MethodGet, "/instances?"+query.Encode(), nil, &page); err != nil {
			return nil, err
		}
		if page.Data == nil {
			return nil, errors.New("lambda API: missing instance list")
		}
		for _, vm := range page.Data {
			if vm.ID == "" {
				return nil, errors.New("lambda API: instance list contains an empty ID")
			}
		}
		instances = append(instances, page.Data...)
		if page.PageToken == "" {
			return instances, nil
		}
		if seen[page.PageToken] {
			return nil, errors.New("lambda API: repeated instance page token")
		}
		seen[page.PageToken] = true
		query.Set("page_token", page.PageToken)
	}
}

func (c *cloudClient) instanceTypes(ctx context.Context, workspace string) (map[string]instanceTypeEntry, error) {
	var reply response[map[string]instanceTypeEntry]
	path := "/instance-types?workspace_id=" + url.QueryEscape(workspace)
	if err := c.call(ctx, http.MethodGet, path, nil, &reply); err != nil {
		return nil, err
	}
	if reply.Data == nil {
		return nil, errors.New("lambda API: missing instance types")
	}
	return reply.Data, nil
}

func (c *cloudClient) terminate(ctx context.Context, id string) error {
	var reply response[struct {
		Instances []instance `json:"terminated_instances"`
	}]
	request := map[string][]string{"instance_ids": {id}}
	err := c.call(ctx, http.MethodPost, "/instance-operations/terminate", request, &reply)
	if err != nil {
		return err
	}
	if len(reply.Data.Instances) != 1 || reply.Data.Instances[0].ID != id {
		return errors.New("lambda API: termination response did not identify the requested instance")
	}
	return nil
}

func definitelyRejected(err error) bool {
	if errors.Is(err, errRequestNotSent) {
		return true
	}
	var apiErr *apiError
	if !errors.As(err, &apiErr) || apiErr.status >= 500 {
		return false
	}
	switch apiErr.code {
	case "global/invalid-api-key", "global/account-inactive", "global/forbidden",
		"global/invalid-address", "global/invalid-parameters", codeObjectMissing,
		"global/quota-exceeded", "global/rate-limited", "instance-operations/launch/insufficient-capacity":
		return true
	default:
		return false
	}
}
