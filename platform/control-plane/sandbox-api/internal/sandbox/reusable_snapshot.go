/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/proto"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/common"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/core"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/httpx"
)

const reusableSnapshotMasterPath = "/snap-manager/reusable-snapshots"

const reusableSnapshotRequestTimeout = 30 * time.Second

const (
	sandboxCheckpointDefaultTimeoutSeconds = 300
	sandboxCheckpointMaxTimeoutSeconds     = 3600
)

type reusableSnapshotMasterClient interface {
	GetActiveMasterAddr() string
}

type reusableSnapshotDoer interface {
	Do(*http.Request) (*http.Response, error)
}

var (
	newReusableSnapshotMasterClient                      = func(ctx context.Context) reusableSnapshotMasterClient { return snapshotMasterEndpoint{ctx: ctx} }
	reusableSnapshotHTTPClient      reusableSnapshotDoer = snapshotHTTPTransport{}
)

type reusableSnapshotCreateRequest struct {
	Name           string `json:"name"`
	TimeoutSeconds int    `json:"timeoutSeconds"`
}

func resolveSandboxCheckpointTimeout(requested int) (int, error) {
	if requested == 0 {
		return sandboxCheckpointDefaultTimeoutSeconds, nil
	}
	if requested < 0 || requested > sandboxCheckpointMaxTimeoutSeconds {
		return 0, fmt.Errorf("timeoutSeconds must be between 1 and %d", sandboxCheckpointMaxTimeoutSeconds)
	}
	return requested, nil
}

// CreateReusableSnapshotV1Handler creates a non-expiring reusable Snapshot
// while leaving the source sandbox running.
func CreateReusableSnapshotV1Handler(ctx *gin.Context) {
	var request reusableSnapshotCreateRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest,
			fmt.Errorf("invalid request body: %v", err))
		return
	}
	name := strings.TrimSpace(request.Name)
	if request.Name != "" && name == "" {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest,
			errors.New("name must be a non-empty string"))
		return
	}
	timeoutSeconds, err := resolveSandboxCheckpointTimeout(request.TimeoutSeconds)
	if err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, err)
		return
	}
	payload, err := proto.Marshal(&core.SnapOptions{
		Type:                common.SnapType_SNAPSHOT,
		Ttl:                 0,
		LeaveRunning:        true,
		Name:                name,
		CheckpointTimeoutMs: uint64(timeoutSeconds) * uint64(time.Second/time.Millisecond),
	})
	if err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError, err)
		return
	}
	killResponse, err := executeSandboxLifecycleKillWithTimeout(
		ctx, lifecycleKillOptions{
			signal:           sandboxPauseInstanceSignal,
			payload:          payload,
			requestIDPattern: sandboxSnapshotRequestIDPattern,
			operation:        "snapshot",
			timeoutSeconds:   timeoutSeconds,
		},
	)
	if err != nil {
		setSandboxLifecycleError(ctx, err)
		return
	}
	var snapshotInfo core.SnapshotInfo
	if err := proto.Unmarshal(killResponse.GetPayload(), &snapshotInfo); err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError,
			fmt.Errorf("invalid snapshot response: %v", err))
		return
	}
	if strings.TrimSpace(snapshotInfo.GetSnapshotID()) == "" {
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError,
			errors.New("invalid snapshot response identity"))
		return
	}
	httpx.SetCtxResponse(ctx, reusableSnapshotInfo{
		SnapshotID: snapshotInfo.GetSnapshotID(),
		Names:      append([]string{}, snapshotInfo.GetNames()...),
	}, http.StatusOK, nil)
}

// GetReusableSnapshotV1Handler returns one tenant-scoped reusable Snapshot.
func GetReusableSnapshotV1Handler(ctx *gin.Context) {
	snapshotID := strings.TrimSpace(ctx.Param("snapshotID"))
	if snapshotID == "" {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, errors.New("snapshotID is required"))
		return
	}
	query := url.Values{"snapshot_id": {snapshotID}}
	proxyReusableSnapshotRequest(ctx, http.MethodGet, query)
}

// ListReusableSnapshotsV1Handler lists tenant-scoped reusable Snapshots.
func ListReusableSnapshotsV1Handler(ctx *gin.Context) {
	query := url.Values{}
	if name := strings.TrimSpace(ctx.Query("name")); name != "" {
		query.Set("name", name)
	}
	if pageToken := strings.TrimSpace(ctx.Query("pageToken")); pageToken != "" {
		query.Set("pageToken", pageToken)
	}
	if pageSize := strings.TrimSpace(ctx.Query("pageSize")); pageSize != "" {
		query.Set("pageSize", pageSize)
	}
	proxyReusableSnapshotRequest(ctx, http.MethodGet, query)
}

// DeleteReusableSnapshotV1Handler deletes one tenant-scoped reusable Snapshot.
func DeleteReusableSnapshotV1Handler(ctx *gin.Context) {
	snapshotID := strings.TrimSpace(ctx.Param("snapshotID"))
	if snapshotID == "" {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, errors.New("snapshotID is required"))
		return
	}
	requestID := strings.TrimSpace(ctx.GetHeader(sandboxLifecycleRequestIDHeader))
	if requestID == "" {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest,
			fmt.Errorf("%s is required", sandboxLifecycleRequestIDHeader))
		return
	}
	query := url.Values{"snapshot_id": {snapshotID}, "request_id": {requestID}}
	proxyReusableSnapshotRequest(ctx, http.MethodDelete, query)
}

func proxyReusableSnapshotRequest(ctx *gin.Context, method string, query url.Values) {
	activeMasterAddr := strings.TrimSpace(newReusableSnapshotMasterClient(ctx.Request.Context()).GetActiveMasterAddr())
	if activeMasterAddr == "" {
		httpx.SetCtxResponse(ctx, nil, http.StatusServiceUnavailable,
			errors.New("active function master is unavailable"))
		return
	}
	query.Set("tenant_id", reusableSnapshotTenant(ctx))
	upstreamURL := normalizeReusableSnapshotMasterURL(activeMasterAddr) + reusableSnapshotMasterPath
	requestCtx, cancel := context.WithTimeout(ctx.Request.Context(), reusableSnapshotRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, method,
		upstreamURL+"?"+query.Encode(), nil)
	if err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError, err)
		return
	}
	for _, header := range []string{
		httpx.HeaderTraceID, httpx.HeaderTraceParent,
		sandboxLifecycleRequestIDHeader, "Authorization", "X-Auth-Token",
	} {
		if value := ctx.GetHeader(header); value != "" {
			request.Header.Set(header, value)
		}
	}
	response, err := reusableSnapshotHTTPClient.Do(request)
	if err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadGateway,
			fmt.Errorf("active function master request failed: %w", err))
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadGateway,
			fmt.Errorf("active function master response failed: %w", err))
		return
	}
	if response.StatusCode >= http.StatusBadRequest {
		httpx.SetCtxResponse(ctx, nil, response.StatusCode,
			fmt.Errorf("active function master rejected request: %s", strings.TrimSpace(string(body))))
		return
	}
	var payload json.RawMessage = body
	if !json.Valid(payload) {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadGateway,
			errors.New("active function master returned invalid JSON"))
		return
	}
	httpx.SetCtxResponse(ctx, payload, response.StatusCode, nil)
}

func reusableSnapshotTenant(ctx *gin.Context) string {
	if tenant := httpx.GetCompatibleGinHeader(ctx.Request, httpx.HeaderTenantID, "tenantId"); tenant != "" {
		return tenant
	}
	if tenant := tenantClaim(ctx.Request); tenant != "" {
		return tenant
	}
	return "default"
}

func normalizeReusableSnapshotMasterURL(addr string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/")
	}
	return "http://" + strings.TrimRight(addr, "/")
}
