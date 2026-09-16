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
	"errors"
	"fmt"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/proto"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/common"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/core"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/httpx"
)

const reusableSnapshotRequestTimeout = 30 * time.Second

const (
	sandboxCheckpointDefaultTimeoutSeconds = 300
	sandboxCheckpointMaxTimeoutSeconds     = 3600
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

func catalog(ctx *gin.Context) (backend.SnapshotCatalog, context.Context, context.CancelFunc) {
	request, cancel := context.WithTimeout(ctx.Request.Context(), reusableSnapshotRequestTimeout)
	c := backend.Current(request).Snapshots
	if c == nil {
		setSandboxLifecycleError(ctx, status.Error(codes.Unavailable, "snapshot catalog unavailable"))
	}
	return c, request, cancel
}
func GetReusableSnapshotV1Handler(ctx *gin.Context) {
	id := strings.TrimSpace(ctx.Param("snapshotID"))
	if id == "" {
		setSandboxLifecycleError(ctx, status.Error(codes.InvalidArgument, "snapshotID is required"))
		return
	}
	c, request, cancel := catalog(ctx)
	defer cancel()
	if c == nil {
		return
	}
	value, err := c.Get(request, id)
	if err != nil {
		setSandboxLifecycleError(ctx, err)
		return
	}
	httpx.SetCtxResponse(ctx, value, http.StatusOK, nil)
}
func ListReusableSnapshotsV1Handler(ctx *gin.Context) {
	var size uint64
	if raw := ctx.Query("pageSize"); raw != "" {
		var err error
		size, err = strconv.ParseUint(raw, 10, 32)
		if err != nil || size > 1000 {
			setSandboxLifecycleError(ctx, status.Error(codes.InvalidArgument, "invalid pageSize"))
			return
		}
	}
	c, request, cancel := catalog(ctx)
	defer cancel()
	if c == nil {
		return
	}
	value, err := c.List(request, strings.TrimSpace(ctx.Query("name")), strings.TrimSpace(ctx.Query("pageToken")), uint32(size))
	if err != nil {
		setSandboxLifecycleError(ctx, err)
		return
	}
	httpx.SetCtxResponse(ctx, value, http.StatusOK, nil)
}
func DeleteReusableSnapshotV1Handler(ctx *gin.Context) {
	id := strings.TrimSpace(ctx.Param("snapshotID"))
	if id == "" || strings.TrimSpace(ctx.GetHeader(sandboxLifecycleRequestIDHeader)) == "" {
		setSandboxLifecycleError(ctx, status.Error(codes.InvalidArgument, "snapshotID and request ID required"))
		return
	}
	c, request, cancel := catalog(ctx)
	defer cancel()
	if c == nil {
		return
	}
	if err := c.Delete(request, id); err != nil {
		setSandboxLifecycleError(ctx, err)
		return
	}
	httpx.SetCtxResponse(ctx, struct{}{}, http.StatusOK, nil)
}
