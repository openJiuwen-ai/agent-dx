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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

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

type reusableSnapshotMasterClientStub struct{ addr string }

func (s reusableSnapshotMasterClientStub) GetActiveMasterAddr() string { return s.addr }

type reusableSnapshotHTTPDoer func(*http.Request) (*http.Response, error)

func (f reusableSnapshotHTTPDoer) Do(req *http.Request) (*http.Response, error) { return f(req) }

func TestReusableSnapshotResourceHandlersForwardToActiveMaster(t *testing.T) {
	oldMasterClient, oldHTTPClient := newReusableSnapshotMasterClient, reusableSnapshotHTTPClient
	t.Cleanup(func() { newReusableSnapshotMasterClient, reusableSnapshotHTTPClient = oldMasterClient, oldHTTPClient })
	newReusableSnapshotMasterClient = func(context.Context) reusableSnapshotMasterClient {
		return reusableSnapshotMasterClientStub{addr: "master.internal:8080"}
	}
	tests := []struct {
		name      string
		method    string
		path      string
		wantQuery string
		wantReqID string
		params    gin.Params
		handler   gin.HandlerFunc
	}{
		{
			name: "get", method: http.MethodGet, path: "/api/sandbox/v1/snapshots/snap-1",
			params:    gin.Params{{Key: "snapshotID", Value: "snap-1"}},
			wantQuery: "snapshot_id=snap-1&tenant_id=tenant-a", handler: GetReusableSnapshotV1Handler,
		},
		{
			name: "list", method: http.MethodGet,
			path:      "/api/sandbox/v1/snapshots?name=base&pageToken=next-1&pageSize=17",
			wantQuery: "name=base&pageSize=17&pageToken=next-1&tenant_id=tenant-a",
			handler:   ListReusableSnapshotsV1Handler,
		},
		{
			name: "delete", method: http.MethodDelete, path: "/api/sandbox/v1/snapshots/snap-1",
			params: gin.Params{{Key: "snapshotID", Value: "snap-1"}}, wantReqID: "delete-1",
			wantQuery: "request_id=delete-1&snapshot_id=snap-1&tenant_id=tenant-a",
			handler:   DeleteReusableSnapshotV1Handler,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reusableSnapshotHTTPClient = reusableSnapshotHTTPDoer(func(
				req *http.Request,
			) (*http.Response, error) {
				require.Equal(t, test.method, req.Method)
				require.Equal(t, "http://master.internal:8080/snap-manager/reusable-snapshots",
					req.URL.Scheme+"://"+req.URL.Host+req.URL.Path)
				require.Equal(t, test.wantQuery, req.URL.RawQuery)
				if test.wantReqID != "" {
					require.Equal(t, test.wantReqID, req.Header.Get("X-ADX-Request-ID"))
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body: io.NopCloser(bytes.NewBufferString(
						`{"snapshotId":"snap-1","names":[]}`)),
				}, nil
			})
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Params = test.params
			ctx.Request = httptest.NewRequest(test.method, test.path, nil)
			ctx.Request.Header.Set(httpx.HeaderTenantID, "tenant-a")
			if test.wantReqID != "" {
				ctx.Request.Header.Set("X-ADX-Request-ID", test.wantReqID)
			}
			test.handler(ctx)
			require.Equal(t, http.StatusOK, recorder.Code)
			var response httpx.Response
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			require.JSONEq(t, `{"snapshotId":"snap-1","names":[]}`, string(response.Data))
		})
	}
}
