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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/common"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/core"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/httpx"
)

func TestCreateReusableSnapshotKeepsSourceRunning(t *testing.T) {
	var captured *core.KillRequest
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(killReq *core.KillRequest, _ testRawOption) ([]byte, error) {
		var ok bool
		captured, ok = proto.Clone(killReq).(*core.KillRequest)
		require.True(t, ok)
		var options core.SnapOptions
		require.NoError(t, proto.Unmarshal(killReq.GetPayload(), &options))
		require.Equal(t, common.SnapType_SNAPSHOT, options.GetType())
		require.True(t, options.GetLeaveRunning())
		require.Zero(t, options.GetTtl())
		require.Equal(t, "base", options.GetName())
		payload, err := proto.Marshal(&core.SnapshotInfo{
			SnapshotID: "snap-deterministic-id",
			Names:      []string{"base"},
		})
		require.NoError(t, err)
		return proto.Marshal(&core.KillResponse{Code: common.ErrorCode_ERR_NONE, Payload: payload})
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "default-source"}}
	ctx.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes/default-source/snapshots",
		bytes.NewBufferString(`{"name":"base"}`),
	)
	ctx.Request.Header.Set("Content-Type", "application/json")
	ctx.Request.Header.Set("X-ADX-Request-ID", "snapshot-create-1")

	CreateReusableSnapshotV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, captured)
	require.Equal(t, int32(18), captured.GetSignal())
	require.Equal(t, "snapshot-create-1", captured.GetRequestID())
	var response httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	var result struct {
		SnapshotID string   `json:"snapshotId"`
		Names      []string `json:"names"`
	}
	require.NoError(t, json.Unmarshal(response.Data, &result))
	require.Equal(t, "snap-deterministic-id", result.SnapshotID)
	require.Equal(t, []string{"base"}, result.Names)
}

func TestCreateUnnamedReusableSnapshotReturnsEmptyNames(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(
		killReq *core.KillRequest,
		_ testRawOption,
	) ([]byte, error) {
		payload, err := proto.Marshal(&core.SnapshotInfo{
			SnapshotID: "snap-without-name",
		})
		require.NoError(t, err)
		return proto.Marshal(&core.KillResponse{
			Code:    common.ErrorCode_ERR_NONE,
			Payload: payload,
		})
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "default-source"}}
	ctx.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes/default-source/snapshots",
		bytes.NewBufferString(`{}`),
	)
	ctx.Request.Header.Set("Content-Type", "application/json")
	ctx.Request.Header.Set("X-ADX-Request-ID", "snapshot-create-unnamed")

	CreateReusableSnapshotV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	var response httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.JSONEq(t, `{"snapshotId":"snap-without-name","names":[]}`, string(response.Data))
}

func TestCreateFromSnapshotForwardsSnapshotID(t *testing.T) {
	var captured *core.CreateRequest
	setAPIClientsForTest(t, &runtimeStub{createInstanceRaw: func(
		createReq *core.CreateRequest,
		_ testRawOption,
	) ([]byte, error) {
		var ok bool
		captured, ok = proto.Clone(createReq).(*core.CreateRequest)
		require.True(t, ok)
		return rawCreateNotify(0, ""), nil
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		bytes.NewBufferString(`{"name":"clone","namespace":"default","snapshotId":"snap-ready"}`),
	)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, captured)
	require.Equal(t, "snap-ready", captured.GetSnapshotID())
}

type snapshotCatalogStub struct{ t *testing.T }

func (s snapshotCatalogStub) Get(ctx context.Context, id string) (backend.Snapshot, error) {
	require.Equal(s.t, "snap-1", id)
	identity, ok := backend.IdentityFromContext(ctx)
	require.True(s.t, ok)
	require.Equal(s.t, "verified", identity.TenantID)
	return backend.Snapshot{SnapshotID: id, Names: []string{}}, nil
}
func (s snapshotCatalogStub) List(ctx context.Context, name, token string, size uint32) (backend.SnapshotPage, error) {
	require.Equal(s.t, "base", name)
	require.Equal(s.t, "cursor", token)
	require.Equal(s.t, uint32(17), size)
	return backend.SnapshotPage{Items: []backend.Snapshot{{SnapshotID: "snap-1", Names: []string{"base"}}}}, nil
}
func (s snapshotCatalogStub) Delete(ctx context.Context, id string) error {
	require.Equal(s.t, "snap-1", id)
	return nil
}
func TestReusableSnapshotResourceHandlersUseTypedCatalog(t *testing.T) {
	for _, tt := range []struct {
		name, method, path, body string
		handler                  gin.HandlerFunc
	}{
		{"get", "GET", "/snapshots/snap-1", `{"snapshotId":"snap-1","names":[]}`, GetReusableSnapshotV1Handler},
		{"list", "GET", "/snapshots?name=base&pageToken=cursor&pageSize=17", `{"items":[{"snapshotId":"snap-1","names":["base"]}],"nextPageToken":""}`, ListReusableSnapshotsV1Handler},
		{"delete", "DELETE", "/snapshots/snap-1", `{}`, DeleteReusableSnapshotV1Handler},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(w)
			ctx.Params = gin.Params{{Key: "snapshotID", Value: "snap-1"}}
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Header.Set(httpx.HeaderTenantID, "spoofed")
			req.Header.Set("X-ADX-Request-ID", "delete-1")
			c := backend.WithIdentity(req.Context(), backend.Identity{TenantID: "verified", Role: backend.RoleTenant})
			c = backend.WithDependencies(c, backend.Dependencies{Snapshots: snapshotCatalogStub{t}})
			ctx.Request = req.WithContext(c)
			tt.handler(ctx)
			require.Equal(t, http.StatusOK, w.Code)
			var response httpx.Response
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
			require.JSONEq(t, tt.body, string(response.Data))
		})
	}
}
