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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/ugorji/go/codec"
	"google.golang.org/protobuf/proto"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/common"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/core"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/runtime"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/httpx"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/instancecache"
)

const (
	customCreateTimeoutSeconds = 37
	testCPULimit               = 2000
	testInvokeFailureCode      = 2002
	testMemoryLimit            = 4096
	replayTestBaseUnixSeconds  = 100
	replayTestSmallCapacity    = 2
	replayTestCapacity         = 10
	replayTestCleanupLimit     = 2
	replayTestExpiration       = 2 * time.Second
	replayBenchmarkCapacity    = 10000
)

type eofTrackingBody struct {
	reader *bytes.Reader
	sawEOF bool
}

func (b *eofTrackingBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if errors.Is(err, io.EOF) {
		b.sawEOF = true
	}
	return n, err
}

func (b *eofTrackingBody) Close() error {
	return nil
}

type eofCheckingRecorder struct {
	*httptest.ResponseRecorder
	body           *eofTrackingBody
	wroteBeforeEOF bool
}

func (r *eofCheckingRecorder) WriteHeader(statusCode int) {
	if !r.body.sawEOF {
		r.wroteBeforeEOF = true
	}
	r.ResponseRecorder.WriteHeader(statusCode)
}

func (r *eofCheckingRecorder) Write(p []byte) (int, error) {
	if !r.body.sawEOF {
		r.wroteBeforeEOF = true
	}
	return r.ResponseRecorder.Write(p)
}

type sandboxTimeoutTestCase struct {
	name             string
	createTimeout    int
	scheduleTimeout  int
	initTimeout      int
	wantCreate       int
	wantSchedule     int
	wantErrorMessage string
}

type runtimeStub struct {
	createInstance           func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error)
	createInstanceRaw        func(createReq *core.CreateRequest, option testRawOption) ([]byte, error)
	createInstanceRawContext func(
		ctx context.Context,
		createReq *core.CreateRequest,
		option testRawOption,
	) ([]byte, error)
	inspectDirectRawCreate func(req backend.Request)
	invokeInstanceRaw      func(invokeReq *core.InvokeRequest, option testRawOption) ([]byte, error)
	kill                   func(instanceID string, signal int, payload []byte, invokeOpt createOptions) error
	killRaw                func(killReq *core.KillRequest, option testRawOption) ([]byte, error)
}

func setAPIClientsForTest(t *testing.T, runtime *runtimeStub) {
	t.Helper()
	restore := setTransportForTest(t, &directRuntimeStub{runtime: runtime})
	t.Cleanup(restore)
}

type directRuntimeStub struct{ runtime *runtimeStub }

func (r *directRuntimeStub) Create(req backend.Request) ([]byte, error) {
	if r.runtime.inspectDirectRawCreate != nil {
		r.runtime.inspectDirectRawCreate(req)
	}
	return r.runtime.CreateInstanceRawContext(req.Context, req.Payload, testRawOption{TraceParent: req.TraceParent})
}

func (r *directRuntimeStub) Invoke(req backend.Request) ([]byte, error) {
	return r.runtime.InvokeByInstanceIdRaw(req.Payload, testRawOption{TraceParent: req.TraceParent})
}

func (r *directRuntimeStub) KillInstance(req backend.LifecycleRequest) error {
	if r.runtime.killRaw != nil {
		_, err := r.KillInstanceWithResponse(req)
		return err
	}
	return r.runtime.Kill(req.InstanceID, req.Signal, req.Payload, killOptions(req))
}

func (r *directRuntimeStub) KillInstanceWithResponse(req backend.LifecycleRequest) (*core.KillResponse, error) {
	if r.runtime.killRaw != nil {
		return unmarshalDirectKillResponse(r.runtime.killRaw(&core.KillRequest{
			InstanceID: req.InstanceID, Signal: int32(req.Signal), Payload: req.Payload, RequestID: req.RequestID,
		}, testRawOption{TraceParent: req.TraceParent}))
	}
	err := r.KillInstance(req)
	return &core.KillResponse{Code: common.ErrorCode_ERR_NONE}, err
}

func unmarshalDirectKillResponse(raw []byte, err error) (*core.KillResponse, error) {
	if err != nil {
		return nil, err
	}
	response := &core.KillResponse{}
	if err := proto.Unmarshal(raw, response); err != nil {
		return nil, err
	}
	return response, nil
}

func (r *runtimeStub) CreateInstance(
	funcMeta testFunctionMeta,
	args []inlineArg,
	invokeOpt createOptions,
) (string, error) {
	if r.createInstance != nil {
		return r.createInstance(funcMeta, args, invokeOpt)
	}
	return "", nil
}

func (r *runtimeStub) Kill(instanceID string, signal int, payload []byte, invokeOpt createOptions) error {
	if r.kill != nil {
		return r.kill(instanceID, signal, payload, invokeOpt)
	}
	return nil
}

func (r *runtimeStub) CreateInstanceRaw(createReqRaw []byte, option testRawOption) ([]byte, error) {
	var createReq core.CreateRequest
	if err := proto.Unmarshal(createReqRaw, &createReq); err != nil {
		return nil, err
	}
	if r.createInstanceRaw != nil {
		return r.createInstanceRaw(&createReq, option)
	}
	if r.createInstance == nil {
		return rawCreateNotify(0, ""), nil
	}

	_, err := r.createInstance(
		testFunctionMeta{FuncID: createReq.GetFunction()},
		nil,
		invokeOptionsFromRawCreate(&createReq),
	)
	if err == nil {
		return rawCreateNotify(0, ""), nil
	}
	var errInfo operationError
	if !errors.As(err, &errInfo) {
		return nil, err
	}
	return rawCreateNotify(errInfo.Code, err.Error()), nil
}

func (r *runtimeStub) CreateInstanceRawContext(
	ctx context.Context,
	createReqRaw []byte,
	option testRawOption,
) ([]byte, error) {
	if r.createInstanceRawContext == nil {
		return r.CreateInstanceRaw(createReqRaw, option)
	}
	var createReq core.CreateRequest
	if err := proto.Unmarshal(createReqRaw, &createReq); err != nil {
		return nil, err
	}
	return r.createInstanceRawContext(ctx, &createReq, option)
}

func invokeOptionsFromRawCreate(createReq *core.CreateRequest) createOptions {
	scheduling := createReq.GetSchedulingOps()
	resources := scheduling.GetResources()
	customResources := make(map[string]float64, len(resources))
	for name, value := range resources {
		if name != "CPU" && name != "Memory" {
			customResources[name] = value
		}
	}
	return createOptions{
		Cpu:               int(resources["CPU"]),
		Memory:            int(resources["Memory"]),
		CpuLimit:          parseTestInt(scheduling.GetExtension()["CPU_LIMIT"]),
		MemoryLimit:       parseTestInt(scheduling.GetExtension()["MEMORY_LIMIT"]),
		CustomResources:   customResources,
		CustomExtensions:  cloneStringMap(scheduling.GetExtension()),
		CreateOpt:         cloneStringMap(createReq.GetCreateOptions()),
		Labels:            append([]string(nil), createReq.GetLabels()...),
		ScheduleTimeoutMs: scheduling.GetScheduleTimeoutMs(),
		RecoverRetryTimes: parseTestInt(createReq.GetCreateOptions()["RecoverRetryTimes"]),
		Priority:          int(scheduling.GetPriority()),
		TraceID:           createReq.GetTraceID(),
		Timeout:           parseTestInt(createReq.GetCreateOptions()["call_timeout"]),
	}
}

func parseTestInt(value string) int {
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		panic(err)
	}
	return parsed
}

func rawCreateNotify(code int, message string) []byte {
	raw, err := proto.Marshal(&runtime.NotifyRequest{
		Code:    common.ErrorCode(code),
		Message: message,
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func rawInvokeNotify(code int, message string, value []byte) []byte {
	notify := &runtime.NotifyRequest{
		Code:    common.ErrorCode(code),
		Message: message,
	}
	if len(value) != 0 {
		notify.SmallObjects = []*common.SmallObject{{Value: value}}
	}
	raw, err := proto.Marshal(notify)
	if err != nil {
		panic(err)
	}
	return raw
}

func cloneCreateRequest(t *testing.T, request *core.CreateRequest) *core.CreateRequest {
	t.Helper()
	cloned, ok := proto.Clone(request).(*core.CreateRequest)
	require.True(t, ok)
	return cloned
}

func cloneInvokeRequest(t *testing.T, request *core.InvokeRequest) *core.InvokeRequest {
	t.Helper()
	cloned, ok := proto.Clone(request).(*core.InvokeRequest)
	require.True(t, ok)
	return cloned
}

func TestParseSandboxRawCreateResponse(t *testing.T) {
	t.Run("success uses designated instanceID", func(t *testing.T) {
		instanceID, err := parseSandboxRawCreateResponse(
			rawCreateNotify(0, ""),
			"sandbox-demo",
		)

		require.NoError(t, err)
		require.Equal(t, "sandbox-demo", instanceID)
	})

	t.Run("business error preserves designated instanceID and code", func(t *testing.T) {
		instanceID, err := parseSandboxRawCreateResponse(
			rawCreateNotify(createTimeoutSuccessCode, "create instance timeout"),
			"sandbox-designated",
		)

		require.Equal(t, "sandbox-designated", instanceID)
		var errInfo operationError
		require.True(t, errors.As(err, &errInfo))
		require.Equal(t, createTimeoutSuccessCode, errInfo.Code)
		require.EqualError(t, err, "create instance timeout")
	})
}

func TestParseSandboxRawInvokeResponse(t *testing.T) {
	t.Run("success decodes the inline result", func(t *testing.T) {
		result, err := encodeMsgpack(map[string]interface{}{"ok": true})
		require.NoError(t, err)
		result = append(make([]byte, inlineHeaderSize), result...)

		decoded, err := parseSandboxRawInvokeResponse(rawInvokeNotify(0, "", result))

		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{"ok": true}, decoded)
	})

	t.Run("business error preserves the runtime code", func(t *testing.T) {
		decoded, err := parseSandboxRawInvokeResponse(
			rawInvokeNotify(testInvokeFailureCode, "invoke failed", nil),
		)

		require.Nil(t, decoded)
		var errInfo operationError
		require.True(t, errors.As(err, &errInfo))
		require.Equal(t, testInvokeFailureCode, errInfo.Code)
		require.EqualError(t, err, "invoke failed")
	})

	t.Run("success without an inline result is rejected", func(t *testing.T) {
		decoded, err := parseSandboxRawInvokeResponse(rawInvokeNotify(0, "", nil))

		require.Nil(t, decoded)
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires exactly one inline result, got 0")
	})

	t.Run("multiple inline results are rejected instead of truncating", func(t *testing.T) {
		notify := &runtime.NotifyRequest{
			SmallObjects: []*common.SmallObject{{Value: []byte("first")}, {Value: []byte("second")}},
		}
		raw, err := proto.Marshal(notify)
		require.NoError(t, err)

		decoded, err := parseSandboxRawInvokeResponse(raw)

		require.Nil(t, decoded)
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires exactly one inline result, got 2")
	})
}

func TestCreateSandboxInstanceRawUsesIndependentTimeoutContext(t *testing.T) {
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/sandbox/create", nil).
		WithContext(requestCtx)

	const createTimeoutSeconds = 2
	setAPIClientsForTest(t, &runtimeStub{
		inspectDirectRawCreate: func(req backend.Request) {
			require.Equal(t, createTimeoutSeconds, req.TimeoutSeconds)
		},
		createInstanceRawContext: func(
			createCtx context.Context,
			_ *core.CreateRequest,
			_ testRawOption,
		) ([]byte, error) {
			require.NoError(t, createCtx.Err())
			deadline, ok := createCtx.Deadline()
			require.True(t, ok)
			remaining := time.Until(deadline)
			require.True(t, remaining > 0)
			require.True(t, remaining <= time.Duration(createTimeoutSeconds)*time.Second)
			return rawCreateNotify(0, ""), nil
		},
	})

	instanceID, err := createSandboxInstanceRaw(
		ctx,
		sandboxInvocation{
			funcID: defaultSandboxFunctionID,
			invokeOpts: createOptions{
				Timeout:          createTimeoutSeconds,
				CreateOpt:        map[string]string{},
				CustomExtensions: map[string]string{},
			},
		},
		"sandbox-context",
		"default",
	)

	require.NoError(t, err)
	require.Equal(t, "default-sandbox-context", instanceID)
}

func TestCreateV1HandlerForwardsFailover(t *testing.T) {
	request := CreateV1Request{Name: "sandbox-a", Namespace: "default", Failover: true}
	create := createRequestFromV1(request, "python:3.12-slim")
	raw, err := buildSandboxRawCreateRequest(
		sandboxInvocation{
			invokeOpts: createOptions{
				CreateOpt:        map[string]string{},
				CustomExtensions: map[string]string{},
			},
			snapshotID: create.SnapshotID,
			failover:   create.Failover,
		},
		create.Name,
		create.Namespace,
	)

	require.NoError(t, err)
	require.True(t, raw.GetFailover())
}

func TestCreateResourcesPreservesSnapshotInheritance(t *testing.T) {
	tests := []struct {
		name       string
		request    CreateRequest
		wantCPU    int
		wantMemory int
	}{
		{
			name:       "ordinary create uses defaults",
			request:    CreateRequest{},
			wantCPU:    sandboxDefaultCPU,
			wantMemory: sandboxDefaultMemory,
		},
		{
			name:       "snapshot create inherits omitted resources",
			request:    CreateRequest{SnapshotID: "snapshot-4g"},
			wantCPU:    -1,
			wantMemory: -1,
		},
		{
			name: "snapshot create preserves explicit resources",
			request: CreateRequest{
				SnapshotID: "snapshot-4g",
				Cpu:        2000,
				Memory:     4096,
			},
			wantCPU:    2000,
			wantMemory: 4096,
		},
		{
			name: "snapshot create can mix inheritance and override",
			request: CreateRequest{
				SnapshotID: "snapshot-4g",
				Memory:     8192,
			},
			wantCPU:    -1,
			wantMemory: 8192,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cpu, memory := createResources(test.request)
			require.Equal(t, test.wantCPU, cpu)
			require.Equal(t, test.wantMemory, memory)
		})
	}
}

func (r *runtimeStub) InvokeByInstanceIdRaw(invokeReqRaw []byte, option testRawOption) ([]byte, error) {
	var invokeReq core.InvokeRequest
	if err := proto.Unmarshal(invokeReqRaw, &invokeReq); err != nil {
		return nil, err
	}
	if r.invokeInstanceRaw != nil {
		return r.invokeInstanceRaw(&invokeReq, option)
	}
	return rawInvokeNotify(0, "", nil), nil
}

func TestCreateHandlerPropagatesHeaderTenantID(t *testing.T) {

	var capturedInvokeOpt createOptions
	var capturedFuncMeta testFunctionMeta
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "instance-from-header", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-a",
		Namespace: "sandbox",
		Tenant:    "body-tenant",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(httpx.HeaderTenantID, "header-tenant")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, defaultSandboxFunctionID, capturedFuncMeta.FuncID)
	require.Equal(t, sandboxCreateTimeoutSeconds, capturedInvokeOpt.Timeout)
	require.Equal(t, defaultSandboxFunctionID, capturedInvokeOpt.CreateOpt[httpx.FunctionKeyNote])
	require.Equal(t, "header-tenant", capturedInvokeOpt.CreateOpt["tenantId"])
	require.Empty(t, capturedInvokeOpt.CreateOpt[httpx.SchedulerIDNote])

	var resp httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, http.StatusOK, resp.Code)
}

func TestCreateHandlerFallsBackToBodyTenant(t *testing.T) {

	var capturedInvokeOpt createOptions
	var capturedFuncMeta testFunctionMeta
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "instance-from-body", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-b",
		Namespace: "sandbox",
		Tenant:    "body-tenant",
		CpuLimit:  testCPULimit,
		MemLimit:  testMemoryLimit,
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(httpx.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(httpx.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(httpx.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, defaultSandboxFunctionID, capturedFuncMeta.FuncID)
	require.Equal(t, sandboxCreateTimeoutSeconds, capturedInvokeOpt.Timeout)
	require.Equal(t, defaultSandboxFunctionID, capturedInvokeOpt.CreateOpt[httpx.FunctionKeyNote])
	require.Equal(t, "body-tenant", capturedInvokeOpt.CreateOpt["tenantId"])
	require.Equal(t, strconv.Itoa(testCPULimit), capturedInvokeOpt.CustomExtensions["CPU_LIMIT"])
	require.Equal(t, strconv.Itoa(testMemoryLimit), capturedInvokeOpt.CustomExtensions["Memory_LIMIT"])
	require.Equal(t, testCPULimit, capturedInvokeOpt.CpuLimit)
	require.Equal(t, testMemoryLimit, capturedInvokeOpt.MemoryLimit)
	require.Empty(t, capturedInvokeOpt.CreateOpt[httpx.SchedulerIDNote])
}

func TestCreateHandlerDoesNotTrustUnverifiedTokenTenant(t *testing.T) {
	var capturedInvokeOpt createOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(_ testFunctionMeta, _ []inlineArg, invokeOpt createOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-from-token", nil
		},
	})

	encode := func(value interface{}) string {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		return base64.RawURLEncoding.EncodeToString(data)
	}
	token := encode(testJWTHeader{Alg: "none", Typ: "JWT"}) + "." +
		encode(testJWTPayload{Sub: "token-tenant"}) + ".sig"

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{Name: "sandbox-token", Namespace: "sandbox"})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set("X-Auth-Token", token)

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Empty(t, capturedInvokeOpt.CreateOpt["tenantId"])
}

func TestCreateHandlerReturnsInstanceIDWhenCreateTimesOutAfterScheduling(t *testing.T) {
	oldWaitForSandboxInstanceRunning := waitForSandboxInstanceRunning
	waitCalled := false
	waitForSandboxInstanceRunning = func(ctx context.Context, instanceID, functionID, resourceSpecNote string) bool {
		waitCalled = true
		require.Equal(t, "sandbox-sandbox-c", instanceID)
		require.Equal(t, defaultSandboxFunctionID, functionID)
		require.NotEmpty(t, resourceSpecNote)
		return true
	}
	defer func() {
		waitForSandboxInstanceRunning = oldWaitForSandboxInstanceRunning
	}()

	installCreateTimeoutRuntimeStub(t)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-c",
		Namespace: "sandbox",
		Tenant:    "body-tenant",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(httpx.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(httpx.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(httpx.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	assertCreateTimeoutResponse(t, recorder, waitCalled)
}

func installCreateTimeoutRuntimeStub(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			return "instance-created-late", operationError{
				Code: createTimeoutSuccessCode,
				Err:  fmt.Errorf("create instance timeout"),
			}
		},
	})
}

func assertCreateTimeoutResponse(
	t *testing.T,
	recorder *httptest.ResponseRecorder,
	waitCalled bool,
) {
	t.Helper()
	require.Equal(t, http.StatusOK, recorder.Code)
	var resp httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, http.StatusOK, resp.Code)

	var data map[string]string
	require.NoError(t, json.Unmarshal(resp.Data, &data))
	require.Equal(t, "sandbox-sandbox-c", requireStringMapValue(t, data, "instance_id"))
	require.True(t, waitCalled)
}

func TestSandboxFunctionIDUsesRuntime(t *testing.T) {
	tests := []struct {
		name     string
		runtime  string
		wantFunc string
	}{
		{
			name:     "python310",
			runtime:  "python3.10",
			wantFunc: "default/0-defaultservice-py310/$latest",
		},
		{
			name:     "python39",
			runtime:  "python3.9",
			wantFunc: "default/0-defaultservice-py39/$latest",
		},
		{
			name:     "py310",
			runtime:  "py310",
			wantFunc: "default/0-defaultservice-py310/$latest",
		},
		{
			name:     "rust",
			runtime:  "rust",
			wantFunc: "default/0-defaultservice-rrt/$latest",
		},
		{
			name:     "rrt",
			runtime:  "rrt",
			wantFunc: "default/0-defaultservice-rrt/$latest",
		},
		{
			name:     "empty uses default (rust)",
			runtime:  "",
			wantFunc: "default/0-defaultservice-rrt/$latest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sandboxFunctionIDForRuntime(tt.runtime)
			require.NoError(t, err)
			require.Equal(t, tt.wantFunc, got)
		})
	}
}

func TestSandboxFunctionIDRejectsUnsupportedRuntime(t *testing.T) {
	_, err := sandboxFunctionIDForRuntime("python3.11")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported sandbox runtime")
}

func TestDefaultSandboxFunctionIDUsesRustService(t *testing.T) {
	// Default sandbox backend is the dedicated Rust (rrt) slot.
	require.Equal(t, "default/0-defaultservice-rrt/$latest", defaultSandboxFunctionID)
}

func TestCreateHandlerUsesRequestedRuntime(t *testing.T) {
	var capturedInvokeOpt createOptions
	var capturedFuncMeta testFunctionMeta
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "instance-runtime", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-runtime",
		Namespace: "sandbox",
		Tenant:    "body-tenant",
		Runtime:   "python3.9",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(httpx.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(httpx.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(httpx.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "default/0-defaultservice-py39/$latest", capturedFuncMeta.FuncID)
	require.Equal(t, "default/0-defaultservice-py39/$latest", capturedInvokeOpt.CreateOpt[httpx.FunctionKeyNote])
}

func TestCreateHandlerRejectsUnsupportedRuntime(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			t.Fatalf("createInstance should not be called for unsupported runtime")
			return "", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-runtime",
		Namespace: "sandbox",
		Tenant:    "body-tenant",
		Runtime:   "python3.11",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(httpx.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(httpx.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(httpx.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "unsupported sandbox runtime")
}

func TestCreateHandlerAddsSchedulerCreateOptions(t *testing.T) {

	var capturedInvokeOpt createOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-with-options", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-d",
		Namespace: "sandbox",
		Tenant:    "body-tenant",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(httpx.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(httpx.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(httpx.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	assertSchedulerCreateOptions(t, capturedInvokeOpt)
}

func assertSchedulerCreateOptions(t *testing.T, capturedInvokeOpt createOptions) {
	t.Helper()
	require.Equal(t, "detached", capturedInvokeOpt.CustomExtensions["lifecycle"])
	require.Equal(t, sandboxConcurrency, capturedInvokeOpt.CustomExtensions["Concurrency"])
	require.Equal(t, "reserved", capturedInvokeOpt.CreateOpt[httpx.InstanceTypeNote])
	require.Equal(t, "false", capturedInvokeOpt.CreateOpt[httpx.SchedulerManagedNote])
	_, hasStaticOwner := capturedInvokeOpt.CreateOpt["resource.owner"]
	require.False(t, hasStaticOwner)
	require.Equal(t, fmt.Sprintf("%d", sandboxCreateTimeoutSeconds), capturedInvokeOpt.CreateOpt["call_timeout"])
	require.Equal(t, "30", capturedInvokeOpt.CreateOpt["init_call_timeout"])
	require.Equal(t, "5", capturedInvokeOpt.CreateOpt["GRACEFUL_SHUTDOWN_TIME"])
	require.Equal(t, "/tmp", capturedInvokeOpt.CreateOpt["DELEGATE_DIRECTORY_INFO"])
	require.Equal(t, "512", capturedInvokeOpt.CreateOpt["DELEGATE_DIRECTORY_QUOTA"])
	require.Equal(t, "1", capturedInvokeOpt.CreateOpt["ConcurrentNum"])

	var resSpec sandboxResourceSpec
	require.NoError(t, json.Unmarshal([]byte(capturedInvokeOpt.CreateOpt[httpx.ResourceSpecNote]), &resSpec))
	require.EqualValues(t, sandboxDefaultCPU, resSpec.CPU)
	require.EqualValues(t, sandboxDefaultMemory, resSpec.Memory)
	require.Equal(t, "", resSpec.InvokeLabel)
	require.Nil(t, resSpec.CustomResources)

}

func TestCreateHandlerPassesRootfsToSandboxCustomExtensions(t *testing.T) {

	var capturedInvokeOpt createOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-with-rootfs", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-rootfs",
		Namespace: "sandbox",
		Rootfs:    "python:3.12-slim",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(httpx.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(httpx.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(httpx.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "python:3.12-slim", capturedInvokeOpt.CustomExtensions["rootfs"])
}

func TestCreateHandlerAcceptsImageAliasForRootfs(t *testing.T) {

	var capturedInvokeOpt createOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "instance-with-image", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-image",
		Namespace: "sandbox",
		Image:     "ubuntu:22.04",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(httpx.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(httpx.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(httpx.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "ubuntu:22.04", capturedInvokeOpt.CustomExtensions["rootfs"])
}

func TestCreateHandlerPassesPortForwardingsToNetworkCreateOption(t *testing.T) {

	var capturedCreateReq *core.CreateRequest
	setAPIClientsForTest(t, &runtimeStub{
		createInstanceRaw: func(
			createReq *core.CreateRequest,
			_ testRawOption,
		) ([]byte, error) {
			capturedCreateReq = cloneCreateRequest(t, createReq)
			return rawCreateNotify(0, ""), nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-ports",
		Namespace: "sandbox",
		Ports:     []string{"8080", "https:9090"},
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(httpx.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(httpx.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(httpx.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(
		t,
		`{
			"portForwardings": [
				{"port": 8080, "protocol": "http", "routeKind": "public"},
				{"port": 9090, "protocol": "https", "routeKind": "public"}
			]
		}`,
		capturedCreateReq.GetCreateOptions()["network"],
	)
}

func TestCreateHandlerRejectsInvalidPortForwarding(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			t.Fatalf("createInstance should not be called for invalid port forwarding")
			return "", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-ports",
		Namespace: "sandbox",
		Ports:     []string{"sctp:8080"},
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(httpx.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(httpx.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(httpx.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "port scheme must be http or https")
}

func TestCreateHandlerBuildsBuiltinDetachedSandboxRequest(t *testing.T) {

	var capturedCreateReq *core.CreateRequest
	var capturedRawOption testRawOption
	setAPIClientsForTest(t, &runtimeStub{
		createInstanceRaw: func(
			createReq *core.CreateRequest,
			option testRawOption,
		) ([]byte, error) {
			capturedCreateReq = createReq
			capturedRawOption = option
			return rawCreateNotify(0, ""), nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateRequest{
		Name:      "sandbox-f",
		Namespace: "sandbox",
		Tenant:    "contract-tenant",
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/create", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set(httpx.HeaderTraceID, "trace-create")
	ctx.Request.Header.Set(httpx.HeaderRequestID, t.Name())
	ctx.Request.Header.Set(httpx.HeaderTraceParent, "00-123e4567e89b12d3a456426614174000-0123456789abcdef-01")

	CreateHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	assertBuiltinDetachedSandboxRequest(t, capturedCreateReq, capturedRawOption, recorder)
}

func assertBuiltinDetachedSandboxRequest(
	t *testing.T,
	capturedCreateReq *core.CreateRequest,
	capturedRawOption testRawOption,
	recorder *httptest.ResponseRecorder,
) {
	t.Helper()
	require.NotNil(t, capturedCreateReq)
	require.Equal(t, defaultSandboxFunctionID, capturedCreateReq.GetFunction())
	require.Empty(t, capturedCreateReq.GetArgs())
	require.Equal(t, "sandbox-sandbox-f", capturedCreateReq.GetDesignatedInstanceID())
	require.Len(t, capturedCreateReq.GetRequestID(), sandboxRawRequestIDLength)
	require.True(t, strings.HasSuffix(capturedCreateReq.GetRequestID(), sandboxRawRequestSequence))
	require.Equal(t, "trace-create", capturedCreateReq.GetTraceID())
	require.EqualValues(t, sandboxDefaultCPU, capturedCreateReq.GetSchedulingOps().GetResources()["CPU"])
	require.EqualValues(t, sandboxDefaultMemory, capturedCreateReq.GetSchedulingOps().GetResources()["Memory"])
	require.Equal(t, "detached", capturedCreateReq.GetSchedulingOps().GetExtension()["lifecycle"])
	require.Equal(t, sandboxConcurrency, capturedCreateReq.GetSchedulingOps().GetExtension()["Concurrency"])
	require.Equal(
		t,
		"00-123e4567e89b12d3a456426614174000-0123456789abcdef-01",
		capturedRawOption.TraceParent,
	)
	require.Equal(t, "trace-create", recorder.Header().Get(httpx.HeaderTraceID))
	require.Equal(t, "contract-tenant", capturedCreateReq.GetCreateOptions()["tenantId"])
	require.Equal(t, defaultSandboxFunctionID, capturedCreateReq.GetCreateOptions()[httpx.FunctionKeyNote])
	require.Equal(t, sandboxModuleName, capturedCreateReq.GetCreateOptions()["moduleName"])
	require.Equal(t, sandboxClassName, capturedCreateReq.GetCreateOptions()["className"])
	_, hasStaticOwner := capturedCreateReq.GetCreateOptions()["resource.owner"]
	require.False(t, hasStaticOwner)
	require.Equal(t, sandboxInstanceType, capturedCreateReq.GetCreateOptions()[httpx.InstanceTypeNote])
	require.Equal(t, "false", capturedCreateReq.GetCreateOptions()[httpx.SchedulerManagedNote])
	require.Empty(t, capturedCreateReq.GetCreateOptions()[httpx.SchedulerIDNote])
}

func TestCreateV1HandlerDefaultsAndReturnsSandboxID(t *testing.T) {
	var capturedInvokeOpt createOptions
	var capturedCreateReq *core.CreateRequest
	setAPIClientsForTest(t, &runtimeStub{
		createInstanceRaw: func(createReq *core.CreateRequest, _ testRawOption) ([]byte, error) {
			capturedCreateReq = createReq
			capturedInvokeOpt = invokeOptionsFromRawCreate(createReq)
			return rawCreateNotify(0, ""), nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateV1Request{
		Image:              "ubuntu:22.04",
		IdleTimeoutSeconds: 123,
		DataPlane: &DataPlanePolicy{
			TunnelSecurityMode:      DataPlaneSecurityTLSToken,
			PortForwardSecurityMode: DataPlaneSecurityTLS,
		},
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, capturedCreateReq)
	require.Equal(t, defaultSandboxFunctionID, capturedCreateReq.GetFunction())
	require.True(t, strings.HasPrefix(capturedCreateReq.GetDesignatedInstanceID(), "default-sandbox-"))
	require.Equal(t, "123", capturedInvokeOpt.CustomExtensions["idle_timeout"])
	require.Equal(t, "tls-token", capturedInvokeOpt.CustomExtensions["data_plane_tunnel_security_mode"])
	require.Equal(t, "tls", capturedInvokeOpt.CustomExtensions["data_plane_port_forward_security_mode"])
	require.JSONEq(
		t,
		`{"runtime":"runsc","type":"image","imageurl":"ubuntu:22.04"}`,
		capturedInvokeOpt.CustomExtensions["rootfs"],
	)

	var resp httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	var data map[string]string
	require.NoError(t, json.Unmarshal(resp.Data, &data))
	require.Equal(
		t,
		capturedCreateReq.GetDesignatedInstanceID(),
		requireStringMapValue(t, data, "sandboxId"),
	)
	require.Equal(t, "running", requireStringMapValue(t, data, "status"))
}

func TestCreateV1HandlerUsesRRTForKataIsolationRuntime(t *testing.T) {
	var capturedInvokeOpt createOptions
	var capturedFuncMeta testFunctionMeta
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "sandbox-kata", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{
		"rootfs":{
			"runtime":"kata",
			"type":"image",
			"imageurl":"ubuntu:22.04"
		}
	}`)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		bytes.NewReader(body),
	)
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, defaultSandboxFunctionID, capturedFuncMeta.FuncID)
	require.JSONEq(
		t,
		`{"runtime":"kata","type":"image","imageurl":"ubuntu:22.04"}`,
		capturedInvokeOpt.CustomExtensions["rootfs"],
	)
}

func TestCreateV1HandlerRejectsRootfsImageAlias(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", strings.NewReader(`{
		"rootfs":{"type":"image","image":"ubuntu:22.04"}
	}`))

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "image rootfs requires imageurl")
}

func TestCreateV1HandlerForwardsInheritedImageEntrypoint(t *testing.T) {
	var capturedInvokeOpt createOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "sandbox-entrypoint", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"rootfs":{"type":"image","imageurl":"example/image:latest"},"inheritEntrypoint":true}`)
	var err error
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "true", capturedInvokeOpt.CustomExtensions["inherit_entrypoint"])
}

func TestCreateV1HandlerOmitsInheritedImageEntrypointByDefault(t *testing.T) {
	var capturedInvokeOpt createOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "sandbox-no-entrypoint", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"rootfs":{"type":"image","imageurl":"example/image:latest"}}`)
	var err error
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotContains(t, capturedInvokeOpt.CustomExtensions, "inherit_entrypoint")
}

func TestCreateV1HandlerRejectsInheritedEntrypointWithoutFreshImage(t *testing.T) {
	tests := []string{
		`{"rootfs":{"type":"local","path":"/rootfs"},"inheritEntrypoint":true}`,
		`{"rootfs":{"type":"s3","storageInfo":{"endpoint":"e","bucket":"b","object":"o"}},"inheritEntrypoint":true}`,
		`{"rootfs":{"type":"image","imageurl":"example/image:latest"},"snapshotId":"snapshot-1","inheritEntrypoint":true}`,
	}
	for _, body := range tests {
		t.Run(body, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			var err error
			ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", strings.NewReader(body))
			require.NoError(t, err)

			CreateV1Handler(ctx)

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Contains(t, recorder.Body.String(), "inheritEntrypoint")
		})
	}
}

func TestCreateV1HandlerPreservesRuntimeOnlyRootfsOverlay(t *testing.T) {
	var capturedInvokeOpt createOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(_ testFunctionMeta, _ []inlineArg, invokeOpt createOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "sandbox-kata-runtime-only", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", strings.NewReader(`{
		"rootfs":{"runtime":"kata"}
	}`))

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"runtime":"kata"}`, capturedInvokeOpt.CustomExtensions["rootfs"])
}

func TestCreateV1HandlerAcceptsDeprecatedTopLevelRuntime(t *testing.T) {
	var capturedInvokeOpt createOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(_ testFunctionMeta, _ []inlineArg, invokeOpt createOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "sandbox-nested-kata", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", strings.NewReader(`{
		"runtime":"kata"
	}`))

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"runtime":"kata"}`, capturedInvokeOpt.CustomExtensions["rootfs"])
}

func TestCreateV1HandlerRejectsConflictingRuntimeFields(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", strings.NewReader(`{
		"runtime":"kata",
		"rootfs":{"runtime":"runsc"}
	}`))

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "deprecated top-level runtime")
	require.Contains(t, recorder.Body.String(), "conflicts with rootfs.runtime")
}

func TestCreateV1HandlerRejectsIncompleteRootfsSource(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", strings.NewReader(`{
		"rootfs":{"type":"local"}
	}`))

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "local rootfs requires path")
}

func TestCreateV1HandlerPassesS3RootfsAndRequiredNodeAffinity(t *testing.T) {
	var capturedCreateReq *core.CreateRequest
	setAPIClientsForTest(t, &runtimeStub{
		createInstanceRaw: func(
			createReq *core.CreateRequest,
			_ testRawOption,
		) ([]byte, error) {
			capturedCreateReq = cloneCreateRequest(t, createReq)
			return rawCreateNotify(0, ""), nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{
			"env":{"SANDBOX_ENV":"enabled"},
			"mounts":[{
				"type":"bind",
				"target":"/opt/tools",
				"image_url":"registry.example/tools:v1",
				"options":["ro"]
			}],
			"extra_config":{"seccomp":"strict"},
			"scheduleAffinities":[{
			"kind":0,
			"affinity":2,
			"labelOps":[{
				"type":0,
				"labelKey":"NODE_ID",
				"labelValues":["node-a"]
			}]
		}],
		"rootfs":{
			"runtime":"runsc",
			"type":"s3",
			"storageInfo":{
				"endpoint":"https://s3.example",
				"bucket":"rootfs",
				"object":"images/base"
			}
		}
	}`)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		bytes.NewReader(body),
	)
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, capturedCreateReq)
	createOptions := capturedCreateReq.GetCreateOptions()
	require.JSONEq(
		t,
		`{
			"runtime":"runsc",
			"type":"s3",
			"storageInfo":{
				"endpoint":"https://s3.example",
				"bucket":"rootfs",
				"object":"images/base"
				}
			}`,
		createOptions["rootfs"],
	)
	require.JSONEq(
		t,
		`[{
			"type":"bind",
			"target":"/opt/tools",
			"image_url":"registry.example/tools:v1",
			"options":["ro"]
		}]`,
		createOptions["mounts"],
	)
	require.JSONEq(t, `{"seccomp":"strict"}`, createOptions["extra_config"])
	require.JSONEq(
		t,
		`{
			"RRT_HTTP_PORT":"50090",
			"RRT_COMMAND_RESULT_TTL_SECS":"3600",
			"RRT_COMMAND_STDOUT_LIMIT_BYTES":"4194304",
			"RRT_COMMAND_STDERR_LIMIT_BYTES":"4194304",
			"RRT_COMMAND_REGISTRY_MAX_RECORDS":"4096",
			"RRT_COMMAND_REGISTRY_MAX_BYTES":"268435456",
			"RRT_COMMAND_REGISTRY_MEMORY_HIGH_WATERMARK_BYTES":"201326592",
			"RRT_COMMAND_ACTIVITY_HEARTBEAT_SECS":"10",
			"RRT_COMMAND_WATCH_MAX_SUBSCRIPTIONS":"4096",
			"RRT_COMMAND_WATCH_MAX_FRAME_BYTES":"1048576",
			"SANDBOX_ENV":"enabled"
		}`,
		createOptions[httpx.DelegateEnvVar],
	)

	rawAffinity := capturedCreateReq.GetSchedulingOps().GetScheduleAffinity()
	require.NotNil(t, rawAffinity)
	requiredNode := rawAffinity.GetResource().GetRequiredAffinity()
	require.NotNil(t, requiredNode)
	subConditions := requiredNode.GetCondition().GetSubConditions()
	require.Len(t, subConditions, 1)
	expressions := subConditions[0].GetExpressions()
	require.Len(t, expressions, 1)
	require.Equal(t, "NODE_ID", expressions[0].GetKey())
	require.Equal(
		t,
		[]string{"node-a"},
		expressions[0].GetOp().GetIn().GetValues(),
	)
}

func TestCreateV1HandlerConvertsNormalizedXPUToFunctionSystemResource(t *testing.T) {
	var capturedCreateReq *core.CreateRequest
	setAPIClientsForTest(t, &runtimeStub{
		createInstanceRaw: func(
			createReq *core.CreateRequest,
			_ testRawOption,
		) ([]byte, error) {
			capturedCreateReq = cloneCreateRequest(t, createReq)
			return rawCreateNotify(0, ""), nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"image":"ubuntu:22.04","xpu":"GPU:L20:2"}`)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		bytes.NewReader(body),
	)
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, capturedCreateReq)
	require.Equal(
		t,
		float64(2),
		capturedCreateReq.GetSchedulingOps().GetResources()["GPU/L20/count"],
	)
	var resourceSpec sandboxResourceSpec
	require.NoError(
		t,
		json.Unmarshal(
			[]byte(capturedCreateReq.GetCreateOptions()[httpx.ResourceSpecNote]),
			&resourceSpec,
		),
	)
	require.Equal(t, int64(2), resourceSpec.CustomResources["GPU/L20/count"])
}

func TestCreateV1HandlerPassesStorageToFunctionSystem(t *testing.T) {
	const storageMb int64 = 153600
	const storageLimitMb int64 = 204800

	var capturedCreateReq *core.CreateRequest
	setAPIClientsForTest(t, &runtimeStub{
		createInstanceRaw: func(
			createReq *core.CreateRequest,
			_ testRawOption,
		) ([]byte, error) {
			capturedCreateReq = cloneCreateRequest(t, createReq)
			return rawCreateNotify(0, ""), nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{
		"image":"ubuntu:22.04",
		"xpu":"gpu:L20:1",
		"storageMb":153600,
		"storage_limit_mb":204800
	}`)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		bytes.NewReader(body),
	)
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, capturedCreateReq)
	resources := capturedCreateReq.GetSchedulingOps().GetResources()
	require.Equal(t, float64(1), resources["GPU/L20/count"])
	require.Equal(
		t,
		float64(storageMb*bytesPerMiB),
		resources[sandboxStorageResourceName],
	)
	require.Equal(
		t,
		strconv.FormatInt(storageLimitMb*bytesPerMiB, decimalRadix),
		capturedCreateReq.GetSchedulingOps().GetExtension()[sandboxStorageLimitExtension],
	)
	var resourceSpec sandboxResourceSpec
	require.NoError(
		t,
		json.Unmarshal(
			[]byte(capturedCreateReq.GetCreateOptions()[httpx.ResourceSpecNote]),
			&resourceSpec,
		),
	)
	require.Equal(
		t,
		storageMb*bytesPerMiB,
		resourceSpec.CustomResources[sandboxStorageResourceName],
	)
}

func TestCreateV1HandlerUsesStandaloneStorageLimitMbAsReservation(t *testing.T) {
	const storageLimitMb int64 = 1024

	var capturedCreateReq *core.CreateRequest
	setAPIClientsForTest(t, &runtimeStub{
		createInstanceRaw: func(
			createReq *core.CreateRequest,
			_ testRawOption,
		) ([]byte, error) {
			capturedCreateReq = cloneCreateRequest(t, createReq)
			return rawCreateNotify(0, ""), nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request, err := http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		strings.NewReader(`{"image":"ubuntu:22.04","storage_limit_mb":1024}`),
	)
	require.NoError(t, err)
	ctx.Request = request

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, capturedCreateReq)
	require.Equal(
		t,
		float64(storageLimitMb*bytesPerMiB),
		capturedCreateReq.GetSchedulingOps().GetResources()[sandboxStorageResourceName],
	)
	require.Equal(
		t,
		strconv.FormatInt(storageLimitMb*bytesPerMiB, decimalRadix),
		capturedCreateReq.GetSchedulingOps().GetExtension()[sandboxStorageLimitExtension],
	)
}

func TestCreateV1HandlerRejectsInvalidStorage(t *testing.T) {
	invalidBodies := []string{
		`{"storageMb":0}`,
		`{"storageMb":-1}`,
		`{"storageMb":"1024"}`,
		`{"storage_limit_mb":-1}`,
		`{"storage_limit_mb":"1024"}`,
		`{"storageMb":2048,"storage_limit_mb":1024}`,
	}
	for _, body := range invalidBodies {
		t.Run(body, func(t *testing.T) {
			createCalled := false
			setAPIClientsForTest(t, &runtimeStub{
				createInstanceRaw: func(
					_ *core.CreateRequest,
					_ testRawOption,
				) ([]byte, error) {
					createCalled = true
					return rawCreateNotify(0, ""), nil
				},
			})

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			request, err := http.NewRequest(
				http.MethodPost,
				"/api/sandbox/v1/sandboxes",
				strings.NewReader(body),
			)
			require.NoError(t, err)
			ctx.Request = request

			CreateV1Handler(ctx)

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.False(t, createCalled)
		})
	}
}

func TestCreateV1HandlerEscapesXPUModelAsRegexLiteral(t *testing.T) {
	var capturedCreateReq *core.CreateRequest
	setAPIClientsForTest(t, &runtimeStub{
		createInstanceRaw: func(
			createReq *core.CreateRequest,
			_ testRawOption,
		) ([]byte, error) {
			capturedCreateReq = cloneCreateRequest(t, createReq)
			return rawCreateNotify(0, ""), nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"xpu":"gpu:L20.*[PRO]:1"}`)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		bytes.NewReader(body),
	)
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(
		t,
		capturedCreateReq.GetSchedulingOps().GetResources(),
		`GPU/L20\.\*\[PRO\]/count`,
	)
}

func TestParseSandboxXPUDerivesUppercaseFunctionSystemPrefix(t *testing.T) {
	xpu, err := parseSandboxXPU("npu:Ascend-910B:1")

	require.NoError(t, err)
	require.Equal(t, "npu:Ascend-910B:1", xpu.normalized)
	require.Equal(t, "NPU/Ascend-910B/count", xpu.resourceName)
	require.Equal(t, int64(1), xpu.count)
}

func TestCreateV1HandlerUsesFunctionSystemWildcardWhenXPUModelIsEmpty(t *testing.T) {
	var capturedCreateReq *core.CreateRequest
	setAPIClientsForTest(t, &runtimeStub{
		createInstanceRaw: func(
			createReq *core.CreateRequest,
			_ testRawOption,
		) ([]byte, error) {
			capturedCreateReq = cloneCreateRequest(t, createReq)
			return rawCreateNotify(0, ""), nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"xpu":"gpu::2"}`)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		bytes.NewReader(body),
	)
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, capturedCreateReq)
	require.Equal(
		t,
		float64(2),
		capturedCreateReq.GetSchedulingOps().GetResources()["GPU/.+/count"],
	)
	var resourceSpec sandboxResourceSpec
	require.NoError(
		t,
		json.Unmarshal(
			[]byte(capturedCreateReq.GetCreateOptions()[httpx.ResourceSpecNote]),
			&resourceSpec,
		),
	)
	require.Equal(t, int64(2), resourceSpec.CustomResources["GPU/.+/count"])
}

func TestCreateV1HandlerRejectsInvalidXPU(t *testing.T) {
	invalidValues := []string{
		"gpu:l20",
		"gpu:l20:1:0",
		":l20:1",
		"gpu:l20:",
		"g.*:l20:1",
		"gpu:l20:0",
		"gpu:l20:-1",
		"gpu:l20:1.5",
		"gpu:l20:1,gpu:h100:1",
		" gpu:l20:1",
	}
	for _, value := range invalidValues {
		t.Run(value, func(t *testing.T) {
			createCalled := false
			setAPIClientsForTest(t, &runtimeStub{
				createInstanceRaw: func(
					_ *core.CreateRequest,
					_ testRawOption,
				) ([]byte, error) {
					createCalled = true
					return rawCreateNotify(0, ""), nil
				},
			})

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			body, err := json.Marshal(CreateV1Request{XPU: value})
			require.NoError(t, err)
			ctx.Request, err = http.NewRequest(
				http.MethodPost,
				"/api/sandbox/v1/sandboxes",
				bytes.NewReader(body),
			)
			require.NoError(t, err)

			CreateV1Handler(ctx)

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.False(t, createCalled)
			require.Contains(t, recorder.Body.String(), "xpu")
		})
	}
}

func TestCreateV1HandlerRejectsInvalidScheduleAffinity(t *testing.T) {
	createCalled := false
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			createCalled = true
			return "sandbox-invalid-affinity", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{
		"image":"ubuntu:22.04",
		"scheduleAffinities":[{
			"kind":9,
			"affinity":2,
			"labelOps":[{
				"type":0,
				"labelKey":"NODE_ID",
				"labelValues":["node-a"]
			}]
		}]
	}`)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		bytes.NewReader(body),
	)
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.False(t, createCalled)
	require.Contains(t, recorder.Body.String(), "scheduleAffinities[0].kind")
}

func TestCreateV1HandlerSSEUsesRequestedTimeoutAndReturnsFinal(t *testing.T) {
	const sdkInitCallTimeoutSeconds = 45
	var capturedInvokeOpt createOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "sandbox-sse", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateV1Request{
		Name:                   "sandbox-sse",
		Namespace:              "default",
		Image:                  "ubuntu:22.04",
		CreateTimeoutSeconds:   customCreateTimeoutSeconds,
		InitCallTimeoutSeconds: sdkInitCallTimeoutSeconds,
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set("Accept", "text/event-stream")
	ctx.Request.Header.Set(httpx.HeaderRequestID, "create-request-sse")

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "text/event-stream", recorder.Header().Get("Content-Type"))
	require.Contains(t, recorder.Body.String(), "event: accepted")
	require.Contains(t, recorder.Body.String(), `"status":"creating"`)
	require.Contains(t, recorder.Body.String(), `"requestId":"create-request-sse"`)
	require.Contains(t, recorder.Body.String(), "event: final")
	require.Contains(t, recorder.Body.String(), `"sandboxId":"default-sandbox-sse"`)
	require.Contains(t, recorder.Body.String(), `"status":"running"`)
	expectedCreateTimeout := customCreateTimeoutSeconds - sandboxScheduleBufferSeconds +
		sdkInitCallTimeoutSeconds + sandboxScheduleBufferSeconds
	require.Equal(t, expectedCreateTimeout, capturedInvokeOpt.Timeout)
	expectedScheduleMs := int64(customCreateTimeoutSeconds-sandboxScheduleBufferSeconds) * millisecondsPerSecond
	require.Equal(t, expectedScheduleMs, capturedInvokeOpt.ScheduleTimeoutMs)
	require.Equal(t, strconv.Itoa(expectedCreateTimeout), capturedInvokeOpt.CreateOpt["call_timeout"])
	require.Equal(t, strconv.Itoa(sdkInitCallTimeoutSeconds), capturedInvokeOpt.CreateOpt["init_call_timeout"])
}

func TestCreateV1HandlerReadsRequestBodyToEOFBeforeSSE(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(testFunctionMeta, []inlineArg, createOptions) (string, error) {
			return "sandbox-body-eof", nil
		},
	})
	body, err := json.Marshal(CreateV1Request{
		Name:      "sandbox-body-eof",
		Namespace: "default",
		Image:     "ubuntu:22.04",
	})
	require.NoError(t, err)
	requestBody := &eofTrackingBody{reader: bytes.NewReader(body)}
	recorder := &eofCheckingRecorder{
		ResponseRecorder: httptest.NewRecorder(),
		body:             requestBody,
	}
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", requestBody)
	require.NoError(t, err)
	ctx.Request.Header.Set("Accept", "text/event-stream")

	CreateV1Handler(ctx)

	require.True(t, requestBody.sawEOF)
	require.False(t, recorder.wroteBeforeEOF)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), "event: accepted")
}

func TestCreateV1HandlerReadsRequestBodyToEOFBeforeJSONResponse(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(testFunctionMeta, []inlineArg, createOptions) (string, error) {
			return "sandbox-json-body-eof", nil
		},
	})
	body, err := json.Marshal(CreateV1Request{
		Name:      "sandbox-json-body-eof",
		Namespace: "default",
		Image:     "ubuntu:22.04",
	})
	require.NoError(t, err)
	requestBody := &eofTrackingBody{reader: bytes.NewReader(body)}
	recorder := &eofCheckingRecorder{
		ResponseRecorder: httptest.NewRecorder(),
		body:             requestBody,
	}
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", requestBody)
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.True(t, requestBody.sawEOF)
	require.False(t, recorder.wroteBeforeEOF)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotEqual(t, "text/event-stream", recorder.Header().Get("Content-Type"))
}

func TestCreateV1HandlerRejectsOversizedBodyBeforeSSE(t *testing.T) {
	createCalled := false
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(testFunctionMeta, []inlineArg, createOptions) (string, error) {
			createCalled = true
			return "", nil
		},
	})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := strings.NewReader(strings.Repeat(" ", sandboxCreateRequestBodyLimit+1))
	var err error
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", body)
	require.NoError(t, err)
	ctx.Request.Header.Set("Accept", "text/event-stream")

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusRequestEntityTooLarge, recorder.Code)
	require.False(t, createCalled)
	require.NotEqual(t, "text/event-stream", recorder.Header().Get("Content-Type"))
	require.NotContains(t, recorder.Body.String(), "event: accepted")
	require.Contains(t, recorder.Body.String(), "request body exceeds 1048576 bytes")
}

func TestCreateV1HandlerRejectsTrailingJSONBeforeSSE(t *testing.T) {
	createCalled := false
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(testFunctionMeta, []inlineArg, createOptions) (string, error) {
			createCalled = true
			return "", nil
		},
	})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		strings.NewReader(`{"image":"ubuntu:22.04"}{"unexpected":"document"}`),
	)
	require.NoError(t, err)
	ctx.Request.Header.Set("Accept", "text/event-stream")

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.False(t, createCalled)
	require.NotEqual(t, "text/event-stream", recorder.Header().Get("Content-Type"))
	require.NotContains(t, recorder.Body.String(), "event: accepted")
	require.Contains(t, recorder.Body.String(), "invalid request body")
}

func TestCreateV1HandlerRejectsConcurrentExplicitNameWithDifferentRequestID(t *testing.T) {
	var createCalls atomic.Int32
	createStarted := make(chan struct{})
	releaseCreate := make(chan struct{})
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			if createCalls.Add(1) == 1 {
				close(createStarted)
			}
			<-releaseCreate
			return "sandbox-singleflight", nil
		},
	})

	type response struct {
		recorder *httptest.ResponseRecorder
	}
	runCreate := func(requestID, tenant string, responses chan<- response) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		body := []byte(fmt.Sprintf(`{
			"name":"sandbox-singleflight",
			"namespace":"default",
			"tenant":%q
		}`, tenant))
		ctx.Request = httptest.NewRequest(
			http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body),
		)
		ctx.Request.Header.Set(httpx.HeaderRequestID, requestID)
		CreateV1Handler(ctx)
		responses <- response{recorder: recorder}
	}

	responses := make(chan response, 2)
	var requests sync.WaitGroup
	requests.Add(2)
	go func() {
		defer requests.Done()
		runCreate("create-leader", "tenant-a", responses)
	}()
	<-createStarted
	go func() {
		defer requests.Done()
		runCreate("create-duplicate", "tenant-b", responses)
	}()

	time.Sleep(50 * time.Millisecond)
	close(releaseCreate)
	requests.Wait()
	close(responses)

	require.Equal(t, int32(1), createCalls.Load())
	statuses := map[int]*httptest.ResponseRecorder{}
	for result := range responses {
		statuses[result.recorder.Code] = result.recorder
	}
	require.Contains(t, statuses, http.StatusOK)
	require.Contains(t, statuses, http.StatusConflict)
	requireCreateV1SandboxID(t, statuses[http.StatusOK], "default-sandbox-singleflight")
	require.Contains(
		t,
		statuses[http.StatusConflict].Body.String(),
		"sandbox 'default/sandbox-singleflight' is already being created by request create-leader",
	)
	require.Contains(t, statuses[http.StatusConflict].Body.String(), "request create-duplicate")
}

func TestCreateV1HandlerReplaysCompletedCreateByRequestID(t *testing.T) {
	var createCalls atomic.Int32
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			createCalls.Add(1)
			return "sandbox-request-replay", nil
		},
	})

	body := []byte(`{
		"name":"sandbox-request-replay",
		"namespace":"default",
		"tenant":"tenant-replay"
	}`)
	runCreate := func() *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(
			http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body),
		)
		ctx.Request.Header.Set(httpx.HeaderRequestID, "create-request-replay")
		CreateV1Handler(ctx)
		return recorder
	}

	first := runCreate()
	second := runCreate()

	require.Equal(t, int32(1), createCalls.Load())
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	requireCreateV1SandboxID(t, first, "default-sandbox-request-replay")
	requireCreateV1SandboxID(t, second, "default-sandbox-request-replay")
}

func TestCreateV1HandlerRejectsRequestIDBodyConflict(t *testing.T) {
	var createCalls atomic.Int32
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			createCalls.Add(1)
			return "sandbox-request-conflict", nil
		},
	})

	runCreate := func(name string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		body, err := json.Marshal(CreateV1Request{
			Name:      name,
			Namespace: "default",
			Tenant:    "tenant-conflict",
		})
		require.NoError(t, err)
		ctx.Request = httptest.NewRequest(
			http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body),
		)
		ctx.Request.Header.Set(httpx.HeaderRequestID, "create-request-conflict")
		CreateV1Handler(ctx)
		return recorder
	}

	first := runCreate("sandbox-request-conflict-a")
	second := runCreate("sandbox-request-conflict-b")

	require.Equal(t, int32(1), createCalls.Load())
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusConflict, second.Code)
	require.Contains(
		t,
		second.Body.String(),
		"requestId 'create-request-conflict' has already been used to create a sandbox with different parameters",
	)
}

func TestCreateV1HandlerRejectsExplicitNameAlreadyInSandboxRouterCache(t *testing.T) {
	const (
		tenantID  = "tenant-existing"
		namespace = "default"
		name      = "sandbox-existing"
		instance  = namespace + "-" + name
	)
	instancecache.Default().PutSummary(instancecache.Summary{
		InstanceID: instance,
		TenantID:   "another-tenant",
		StatusCode: 3,
	})
	defer instancecache.Default().Delete(instance)

	var createCalls atomic.Int32
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			createCalls.Add(1)
			return instance, nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateV1Request{
		Name:      name,
		Namespace: namespace,
		Tenant:    tenantID,
	})
	require.NoError(t, err)
	ctx.Request = httptest.NewRequest(
		http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body),
	)
	ctx.Request.Header.Set(httpx.HeaderRequestID, "create-existing")

	CreateV1Handler(ctx)

	require.Equal(t, int32(0), createCalls.Load())
	require.Equal(t, http.StatusConflict, recorder.Code)
	require.Contains(
		t,
		recorder.Body.String(),
		"sandbox 'default/sandbox-existing' already exists",
	)
	require.Contains(t, recorder.Body.String(), "sandboxId=default-sandbox-existing")
	require.Contains(t, recorder.Body.String(), "requestId=create-existing")
}

func TestCreateV1HandlerRetainedFailureAllowsAuthoritativeRecreate(t *testing.T) {
	for _, tc := range []struct {
		name             string
		state            int32
		backendDuplicate bool
		wantCalls        int32
		wantHTTP         int
	}{
		{"fatal", instancecache.StatusFatal, false, 1, http.StatusOK},
		{"schedule-failed", instancecache.StatusScheduleFailed, false, 1, http.StatusOK},
		{"authoritative-conflict", instancecache.StatusFatal, true, 1, http.StatusConflict},
		{"recovering", instancecache.StatusFailed, false, 0, http.StatusConflict},
		{"paused", instancecache.StatusPaused, false, 0, http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "retained-" + tc.name
			instance := "default-" + name
			instancecache.Default().PutSummary(instancecache.Summary{
				InstanceID: instance, TenantID: "tenant-recreate", StatusCode: tc.state,
			})
			defer instancecache.Default().Delete(instance)
			var calls atomic.Int32
			setAPIClientsForTest(t, &runtimeStub{
				createInstanceRaw: func(_ *core.CreateRequest, _ testRawOption) ([]byte, error) {
					calls.Add(1)
					if tc.backendDuplicate {
						return rawCreateNotify(sandboxInstanceDuplicatedCode, "instance still exists"), nil
					}
					return rawCreateNotify(0, ""), nil
				},
			})
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			body, err := json.Marshal(CreateV1Request{Name: name, Namespace: "default", Tenant: "tenant-recreate"})
			require.NoError(t, err)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
			ctx.Request.Header.Set(httpx.HeaderRequestID, "create-"+name)
			CreateV1Handler(ctx)
			require.Equal(t, tc.wantCalls, calls.Load())
			require.Equal(t, tc.wantHTTP, recorder.Code, recorder.Body.String())
			// The failed-runtime diagnostic remains available until a newer observation.
			summary, ok := instancecache.Default().GetSummary(instance)
			require.True(t, ok)
			require.Equal(t, tc.state, summary.StatusCode)
		})
	}
}

func TestCreateV1HandlerReplaysUnnamedCreateByRequestID(t *testing.T) {
	var createCalls atomic.Int32
	var createdInstanceID string
	setAPIClientsForTest(t, &runtimeStub{
		createInstanceRaw: func(
			createReq *core.CreateRequest,
			_ testRawOption,
		) ([]byte, error) {
			createCalls.Add(1)
			createdInstanceID = createReq.GetDesignatedInstanceID()
			return rawCreateNotify(0, ""), nil
		},
	})

	runCreate := func() *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(
			http.MethodPost,
			"/api/sandbox/v1/sandboxes",
			bytes.NewReader([]byte(`{"namespace":"default","tenant":"tenant-unnamed"}`)),
		)
		ctx.Request.Header.Set(httpx.HeaderRequestID, "create-request-unnamed")
		CreateV1Handler(ctx)
		return recorder
	}

	first := runCreate()
	second := runCreate()

	require.Equal(t, int32(1), createCalls.Load())
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	require.Regexp(t, `^default-sandbox-`, createdInstanceID)
	requireCreateV1SandboxID(t, first, createdInstanceID)
	requireCreateV1SandboxID(t, second, createdInstanceID)
}

func TestGeneratedSandboxNamesUseIndependentUUIDs(t *testing.T) {
	first := newSandboxName()
	second := newSandboxName()

	require.NotEqual(t, first, second)
	require.Regexp(
		t,
		`^sandbox-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
		first,
	)
}

func requireCreateV1SandboxID(
	t *testing.T,
	recorder *httptest.ResponseRecorder,
	expected string,
) {
	t.Helper()
	var resp httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	var data map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Data, &data))
	require.Equal(t, expected, data["sandboxId"])
}

func TestSandboxCreateReplayStoreExpiresCompletedResults(t *testing.T) {
	now := time.Unix(100, 0)
	store := newSandboxCreateReplayStore(time.Second, 10)
	store.now = func() time.Time { return now }
	digest := [32]byte{1}
	createCalls := 0
	create := func() (sandboxCreateResult, error) {
		createCalls++
		return sandboxCreateResult{
			instanceID: fmt.Sprintf("sandbox-%d", createCalls),
			status:     sandboxCreateStatusRunning,
		}, nil
	}

	first, firstErr, firstReuse := store.do("tenant\x00request", "request", digest, create)
	second, secondErr, secondReuse := store.do("tenant\x00request", "request", digest, create)
	now = now.Add(2 * time.Second)
	third, thirdErr, thirdReuse := store.do("tenant\x00request", "request", digest, create)

	require.NoError(t, firstErr)
	require.NoError(t, secondErr)
	require.NoError(t, thirdErr)
	require.Equal(t, sandboxCreateReuseNone, firstReuse)
	require.Equal(t, sandboxCreateReuseCompleted, secondReuse)
	require.Equal(t, sandboxCreateReuseNone, thirdReuse)
	require.Equal(t, "sandbox-1", first.instanceID)
	require.Equal(t, "sandbox-1", second.instanceID)
	require.Equal(t, "sandbox-2", third.instanceID)
	require.Equal(t, 2, createCalls)
}

func TestSandboxCreateReplayStoreReplaysCompletedError(t *testing.T) {
	store := newSandboxCreateReplayStore(time.Minute, 10)
	digest := [32]byte{2}
	createCalls := 0
	create := func() (sandboxCreateResult, error) {
		createCalls++
		return sandboxCreateResult{
			instanceID: "sandbox-error",
			status:     sandboxCreateStatusFailed,
		}, fmt.Errorf("runtime outcome unknown")
	}

	first, firstErr, firstReuse := store.do("tenant\x00request", "request", digest, create)
	second, secondErr, secondReuse := store.do("tenant\x00request", "request", digest, create)

	require.EqualError(t, firstErr, "runtime outcome unknown")
	require.EqualError(t, secondErr, "runtime outcome unknown")
	require.Equal(t, sandboxCreateReuseNone, firstReuse)
	require.Equal(t, sandboxCreateReuseCompleted, secondReuse)
	require.Equal(t, first, second)
	require.Equal(t, 1, createCalls)
}

func TestSandboxCreateReplayStoreEvictsOldestCompletedResult(t *testing.T) {
	now := time.Unix(replayTestBaseUnixSeconds, 0)
	store := newSandboxCreateReplayStore(time.Minute, replayTestSmallCapacity)
	store.now = func() time.Time { return now }
	createCalls := make(map[string]int)
	create := func(key string) func() (sandboxCreateResult, error) {
		return func() (sandboxCreateResult, error) {
			createCalls[key]++
			return sandboxCreateResult{
				instanceID: fmt.Sprintf("%s-%d", key, createCalls[key]),
				status:     sandboxCreateStatusRunning,
			}, nil
		}
	}

	_, _, _ = store.do("a", "a", [32]byte{1}, create("a"))
	now = now.Add(time.Second)
	_, _, _ = store.do("b", "b", [32]byte{2}, create("b"))
	now = now.Add(time.Second)
	_, _, _ = store.do("c", "c", [32]byte{3}, create("c"))

	b, bErr, bReuse := store.do("b", "b", [32]byte{2}, create("b"))
	a, aErr, aReuse := store.do("a", "a", [32]byte{1}, create("a"))

	require.NoError(t, bErr)
	require.NoError(t, aErr)
	require.Equal(t, sandboxCreateReuseCompleted, bReuse)
	require.Equal(t, sandboxCreateReuseNone, aReuse)
	require.Equal(t, "b-1", b.instanceID)
	require.Equal(t, "a-2", a.instanceID)
}

func TestSandboxCreateReplayStoreBackgroundCleanupExpiresCompletedResults(t *testing.T) {
	now := time.Unix(replayTestBaseUnixSeconds, 0)
	store := newSandboxCreateReplayStore(time.Second, replayTestCapacity)
	store.now = func() time.Time { return now }
	_, _, _ = store.do("expired", "expired", [32]byte{1}, func() (sandboxCreateResult, error) {
		return sandboxCreateResult{instanceID: "sandbox-expired"}, nil
	})

	now = now.Add(replayTestExpiration)
	store.cleanup(now, sandboxCreateReplayCleanupBatch)

	store.mu.Lock()
	_, exists := store.entries["expired"]
	store.mu.Unlock()
	require.False(t, exists)
}

func TestSandboxCreateReplayStoreCleanupIsBounded(t *testing.T) {
	now := time.Unix(replayTestBaseUnixSeconds, 0)
	store := newSandboxCreateReplayStore(time.Second, replayTestCapacity)
	store.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("expired-%d", i)
		_, _, _ = store.do(key, key, [32]byte{byte(i + 1)}, func() (sandboxCreateResult, error) {
			return sandboxCreateResult{instanceID: key}, nil
		})
	}

	now = now.Add(replayTestExpiration)
	store.cleanup(now, replayTestCleanupLimit)

	store.mu.Lock()
	require.Len(t, store.entries, 1)
	store.mu.Unlock()
	store.cleanup(now, replayTestCleanupLimit)
	store.mu.Lock()
	require.Empty(t, store.entries)
	store.mu.Unlock()
}

func TestSandboxCreateReplayStoreCoalescesConcurrentRequest(t *testing.T) {
	store := newSandboxCreateReplayStore(time.Minute, replayTestCapacity)
	started := make(chan struct{})
	release := make(chan struct{})
	var createCalls atomic.Int32
	create := func() (sandboxCreateResult, error) {
		createCalls.Add(1)
		close(started)
		<-release
		return sandboxCreateResult{instanceID: "sandbox-concurrent"}, nil
	}

	type replayResult struct {
		result sandboxCreateResult
		err    error
		reuse  sandboxCreateReuse
	}
	results := make(chan replayResult, 2)
	go func() {
		result, err, reuse := store.do("same", "same", [32]byte{1}, create)
		results <- replayResult{result: result, err: err, reuse: reuse}
	}()
	<-started
	go func() {
		result, err, reuse := store.do("same", "same", [32]byte{1}, create)
		results <- replayResult{result: result, err: err, reuse: reuse}
	}()
	close(release)

	first := <-results
	second := <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.Equal(t, "sandbox-concurrent", first.result.instanceID)
	require.Equal(t, "sandbox-concurrent", second.result.instanceID)
	reuses := []sandboxCreateReuse{first.reuse, second.reuse}
	require.Contains(t, reuses, sandboxCreateReuseNone)
	if first.reuse != sandboxCreateReuseNone {
		require.Contains(t, []sandboxCreateReuse{sandboxCreateReuseInflight, sandboxCreateReuseCompleted}, first.reuse)
	}
	if second.reuse != sandboxCreateReuseNone {
		require.Contains(t, []sandboxCreateReuse{sandboxCreateReuseInflight, sandboxCreateReuseCompleted}, second.reuse)
	}
	require.Equal(t, int32(1), createCalls.Load())
}

func BenchmarkSandboxCreateReplayStoreAtCapacity(b *testing.B) {
	store := newSandboxCreateReplayStore(time.Hour, replayBenchmarkCapacity)
	digest := [32]byte{1}
	for i := 0; i < store.maxEntries; i++ {
		key := fmt.Sprintf("seed-%d", i)
		_, _, _ = store.do(key, key, digest, func() (sandboxCreateResult, error) {
			return sandboxCreateResult{instanceID: key}, nil
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("benchmark-%d", i)
		_, _, _ = store.do(key, key, digest, func() (sandboxCreateResult, error) {
			return sandboxCreateResult{instanceID: key}, nil
		})
	}
}

func BenchmarkSandboxCreateReplayStoreAtCapacityParallel(b *testing.B) {
	store := newSandboxCreateReplayStore(time.Hour, replayBenchmarkCapacity)
	digest := [32]byte{1}
	for i := 0; i < store.maxEntries; i++ {
		key := fmt.Sprintf("parallel-seed-%d", i)
		_, _, _ = store.do(key, key, digest, func() (sandboxCreateResult, error) {
			return sandboxCreateResult{instanceID: key}, nil
		})
	}

	var sequence atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := sequence.Add(1)
			key := fmt.Sprintf("parallel-benchmark-%d", id)
			_, _, _ = store.do(key, key, digest, func() (sandboxCreateResult, error) {
				return sandboxCreateResult{instanceID: key}, nil
			})
		}
	})
}

var timeoutTestCases = []sandboxTimeoutTestCase{
	{
		name:         "default create derives schedule",
		wantCreate:   90,
		wantSchedule: 30,
	},
	{
		name:          "create derives schedule",
		createTimeout: 425,
		wantCreate:    455,
		wantSchedule:  395,
	},
	{
		name:            "schedule derives create",
		scheduleTimeout: 90,
		wantCreate:      150,
		wantSchedule:    90,
	},
	{
		name:            "sdk init budget derives create",
		scheduleTimeout: 90,
		initTimeout:     45,
		wantCreate:      165,
		wantSchedule:    90,
	},
	{
		name:             "create must exceed legacy response reserve",
		createTimeout:    30,
		wantErrorMessage: "createTimeoutSeconds must be greater than 30",
	},
	{
		name:          "legacy create expands outer budget",
		createTimeout: 60,
		wantCreate:    90,
		wantSchedule:  30,
	},
	{
		name:             "create must be positive",
		createTimeout:    -1,
		wantErrorMessage: "createTimeoutSeconds must be a positive integer",
	},
	{
		name:             "schedule must be positive",
		scheduleTimeout:  -1,
		wantErrorMessage: "scheduleTimeoutSeconds must be a positive integer",
	},
	{
		name:             "init must be positive",
		initTimeout:      -1,
		wantErrorMessage: "initCallTimeoutSeconds must be a positive integer",
	},
	{
		name:             "schedule must not exceed create",
		createTimeout:    400,
		scheduleTimeout:  410,
		wantErrorMessage: "scheduleTimeoutSeconds must be less than or equal to createTimeoutSeconds",
	},
	{
		name:             "explicit timeouts must reserve legacy response budget",
		createTimeout:    100,
		scheduleTimeout:  80,
		wantErrorMessage: "createTimeoutSeconds - scheduleTimeoutSeconds must be at least 30",
	},
	{
		name:            "explicit timeouts preserve exact total reserve",
		createTimeout:   180,
		scheduleTimeout: 120,
		wantCreate:      180,
		wantSchedule:    120,
	},
	{
		name:            "explicit timeouts preserve caller budgets",
		createTimeout:   500,
		scheduleTimeout: 100,
		wantCreate:      500,
		wantSchedule:    100,
	},
}

func TestResolveSandboxCreateTimeouts(t *testing.T) {
	t.Setenv("ADX_SANDBOX_CREATE_TIMEOUT", "")
	for _, tt := range timeoutTestCases {
		t.Run(tt.name, func(t *testing.T) {
			createTimeout, scheduleTimeout, err := resolveSandboxCreateTimeouts(
				tt.createTimeout, tt.scheduleTimeout, tt.initTimeout,
			)
			if tt.wantErrorMessage != "" {
				require.EqualError(t, err, tt.wantErrorMessage)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantCreate, createTimeout)
			require.Equal(t, tt.wantSchedule, scheduleTimeout)
		})
	}
}

func TestResolveSandboxCreateTimeoutsNormalizesLegacyEnvironment(t *testing.T) {
	t.Setenv("ADX_SANDBOX_CREATE_TIMEOUT", "60")
	createTimeout, scheduleTimeout, err := resolveSandboxCreateTimeouts(0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, sandboxCreateTimeoutSeconds, createTimeout)
	require.Equal(t, sandboxDefaultScheduleSeconds, scheduleTimeout)
}

func TestCreateV1HandlerSSEDoesNotReportUnconfirmedTimeoutAsRunning(t *testing.T) {
	oldWaitForSandboxInstanceRunning := waitForSandboxInstanceRunning
	waitForSandboxInstanceRunning = func(ctx context.Context, instanceID, functionID, resourceSpecNote string) bool {
		return false
	}
	defer func() { waitForSandboxInstanceRunning = oldWaitForSandboxInstanceRunning }()

	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			return "sandbox-timeout", operationError{Code: 3002, Err: fmt.Errorf("create instance timeout")}
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateV1Request{Name: "sandbox-timeout", Namespace: "default"})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
	require.NoError(t, err)
	ctx.Request.Header.Set("Accept", "text/event-stream")
	ctx.Request.Header.Set(httpx.HeaderRequestID, "create-request-timeout")

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), "event: final")
	require.Contains(t, recorder.Body.String(), `"sandboxId":"default-sandbox-timeout"`)
	require.Contains(t, recorder.Body.String(), `"status":"timeout"`)
	require.Contains(t, recorder.Body.String(), `"errorCode":3002`)
	require.Contains(t, recorder.Body.String(), `"requestId":"create-request-timeout"`)
	require.NotContains(t, recorder.Body.String(), `"status":"running"`)
}

func TestCreateV1HandlerFrontendOwnsTunnelSetup(t *testing.T) {
	var capturedInvokeOpt createOptions
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			capturedInvokeOpt = invokeOpt
			return "default/sandbox_demo.1", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateV1Request{
		Name:      "sandbox_demo.1",
		Namespace: "default",
		Image:     "ubuntu:22.04",
		Env: map[string]string{
			"RRT_HTTP_PORT":      "19000",
			"RRT_TUNNEL_WS_PORT": "19001",
			"USER_ENV":           "ok",
		},
		Tunnel: TunnelSpec{Enabled: true},
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	assertTunnelCreateOptions(t, capturedInvokeOpt)
	assertTunnelResponse(t, recorder)
}

func TestPrepareSandboxRRTHTTPUsesPlatformCommandBudgets(t *testing.T) {
	t.Setenv("ADX_RRT_COMMAND_STDOUT_LIMIT_BYTES", "1024")
	req := &CreateV1Request{Env: map[string]string{
		"RRT_COMMAND_STDOUT_LIMIT_BYTES": "untrusted-user-value",
	}}
	prepareSandboxRRTHTTP(req)
	require.Equal(t, "1024", req.Env["RRT_COMMAND_STDOUT_LIMIT_BYTES"])
	require.Equal(t, "50090", req.Env["RRT_HTTP_PORT"])
}

func assertTunnelCreateOptions(t *testing.T, capturedInvokeOpt createOptions) {
	t.Helper()
	require.JSONEq(
		t,
		`{
			"portForwardings":[
				{"port":50090,"protocol":"http","routeKind":"direct"},
				{"port":8765,"protocol":"http","routeKind":"tunnel"},
				{"port":8766,"protocol":"http","routeKind":"direct"}
			]
		}`,
		capturedInvokeOpt.CreateOpt["network"],
	)
	require.JSONEq(
		t,
		`{
			"RRT_HTTP_PORT":"50090",
			"RRT_TUNNEL_WS_PORT":"8765",
			"RRT_TUNNEL_HTTP_PORT":"8766",
			"RRT_COMMAND_RESULT_TTL_SECS":"3600",
			"RRT_COMMAND_STDOUT_LIMIT_BYTES":"4194304",
			"RRT_COMMAND_STDERR_LIMIT_BYTES":"4194304",
			"RRT_COMMAND_REGISTRY_MAX_RECORDS":"4096",
			"RRT_COMMAND_REGISTRY_MAX_BYTES":"268435456",
			"RRT_COMMAND_REGISTRY_MEMORY_HIGH_WATERMARK_BYTES":"201326592",
			"RRT_COMMAND_ACTIVITY_HEARTBEAT_SECS":"10",
			"RRT_COMMAND_WATCH_MAX_SUBSCRIPTIONS":"4096",
			"RRT_COMMAND_WATCH_MAX_FRAME_BYTES":"1048576",
			"USER_ENV":"ok"
		}`,
		capturedInvokeOpt.CreateOpt[httpx.DelegateEnvVar],
	)
}

func assertTunnelResponse(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	var resp httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	var data map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Data, &data))
	tunnelValue := requireInterfaceMapValue(t, data, "tunnel")
	tunnel, ok := tunnelValue.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "/tunnel/default-sandbox-demo-1", tunnel["url"])
	require.Equal(t, "/tunnel/default-sandbox-demo-1", tunnel["path"])
	require.Equal(t, "http://127.0.0.1:8766", tunnel["proxyUrl"])
}

func TestCreateV1HandlerForwardsIsolationRuntimeWithoutOwningRegistry(t *testing.T) {
	var capturedInvokeOpt createOptions
	var capturedFuncMeta testFunctionMeta
	setAPIClientsForTest(t, &runtimeStub{
		createInstance: func(funcMeta testFunctionMeta, args []inlineArg, invokeOpt createOptions) (string, error) {
			capturedFuncMeta = funcMeta
			capturedInvokeOpt = invokeOpt
			return "sandbox-next-runtime", nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body, err := json.Marshal(CreateV1Request{
		Rootfs: RootfsSpec{
			Runtime:  "gvisor-next",
			Type:     "image",
			ImageURL: "ubuntu:22.04",
		},
	})
	require.NoError(t, err)
	ctx.Request, err = http.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes", bytes.NewReader(body))
	require.NoError(t, err)

	CreateV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, defaultSandboxFunctionID, capturedFuncMeta.FuncID)
	require.JSONEq(
		t,
		`{"runtime":"gvisor-next","type":"image","imageurl":"ubuntu:22.04"}`,
		capturedInvokeOpt.CustomExtensions["rootfs"],
	)
}

func TestNormalizeJSONValuePreservesFractionalAndConvertsIntegers(t *testing.T) {
	normalized := normalizeJSONValue(map[string]interface{}{
		"pid":     float64(123),
		"timeout": float64(0.5),
		"nested":  []interface{}{float64(7)},
	})
	got, ok := normalized.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, int64(123), got["pid"])
	require.Equal(t, float64(0.5), got["timeout"])
	nested, ok := got["nested"].([]interface{})
	require.True(t, ok)
	require.Equal(t, int64(7), nested[0])
}

func TestInvokeV1HandlerRoutesEnvelopeToRRTSandboxInvoke(t *testing.T) {
	capture := setupInvokeV1RuntimeStub(t)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "sandbox-123"}}
	body := []byte(`{"action":"process.exec","args":{"cmd":"echo hi","cwd":"/tmp"}}`)
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes/sandbox-123/invoke",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	ctx.Request.Header.Set(httpx.HeaderTraceID, "trace-invoke")
	ctx.Request.Header.Set(httpx.HeaderTraceParent, "00-abcdefabcdefabcdefabcdefabcdefab-0123456789abcdef-01")

	InvokeV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "sandbox-123", capture.invokeReq.GetInstanceID())
	require.Equal(t, defaultSandboxFunctionID, capture.invokeReq.GetFunction())
	require.Equal(t, "trace-invoke", capture.invokeReq.GetTraceID())
	require.NotEmpty(t, capture.invokeReq.GetRequestID())
	require.Equal(
		t,
		"00-abcdefabcdefabcdefabcdefabcdefab-0123456789abcdef-01",
		capture.rawOption.TraceParent,
	)
	require.Equal(t, "trace-invoke", recorder.Header().Get(httpx.HeaderTraceID))
	require.Nil(t, capture.invokeReq.GetInvokeOptions())
	require.Len(t, capture.invokeReq.GetArgs(), 1)
	decodedEnvelope, err := decodePackedArg(capture.invokeReq.GetArgs()[0].GetValue())
	require.NoError(t, err)
	envelope, ok := decodedEnvelope.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "sandbox_invoke", envelope["sandbox_method"])
	require.Equal(t, "process.exec", envelope["action"])
	require.Equal(t, map[string]interface{}{
		"cmd": "echo hi",
		"cwd": "/tmp",
	}, envelope["args"])

	var resp httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.JSONEq(t, `{"ok":true}`, string(resp.Data))
}

type invokeV1Capture struct {
	invokeReq *core.InvokeRequest
	rawOption testRawOption
}

func setupInvokeV1RuntimeStub(t *testing.T) *invokeV1Capture {
	t.Helper()
	capture := &invokeV1Capture{}
	setAPIClientsForTest(t, &runtimeStub{
		invokeInstanceRaw: func(
			invokeReq *core.InvokeRequest,
			option testRawOption,
		) ([]byte, error) {
			capture.invokeReq = cloneInvokeRequest(t, invokeReq)
			capture.rawOption = option
			result, err := encodeMsgpack(map[string]interface{}{"ok": true})
			require.NoError(t, err)
			return rawInvokeNotify(
				0,
				"",
				append(make([]byte, inlineHeaderSize), result...),
			), nil
		},
	})
	return capture
}

func decodePackedArg(data []byte) (interface{}, error) {
	if len(data) < inlineHeaderSize {
		return nil, fmt.Errorf("arg too short: %d", len(data))
	}
	var out interface{}
	dec := codec.NewDecoderBytes(data[inlineHeaderSize:], &msgpackHandle)
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return normalizeMsgpack(out), nil
}

func requireStringMapValue(t *testing.T, data map[string]string, key string) string {
	t.Helper()
	value, ok := data[key]
	require.True(t, ok)
	return value
}

func requireInterfaceMapValue(t *testing.T, data map[string]interface{}, key string) interface{} {
	t.Helper()
	value, ok := data[key]
	require.True(t, ok)
	return value
}

func TestInvokeV1HandlerRejectsMissingAction(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "sandbox-123"}}
	var err error
	ctx.Request, err = http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes/sandbox-123/invoke",
		bytes.NewReader([]byte(`{"args":{}}`)),
	)
	require.NoError(t, err)

	InvokeV1Handler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "action is required")
}

func TestPauseV1HandlerUsesSDKRequestIDForSignal18AndReturnsSnapshot(t *testing.T) {
	const requestID = "pause-123e4567-e89b-12d3-a456-426614174000"
	instancecache.Default().PutSummary(instancecache.Summary{
		InstanceID: "default-sandbox-1", TenantID: "default", Function: "default/sandbox/$latest",
		NodeID: "proxy-before-pause", StatusCode: 3, StatusMsg: "running",
	})
	t.Cleanup(func() {
		instancecache.Default().Delete("default-sandbox-1")
	})
	var captured *core.KillRequest
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(
		killReq *core.KillRequest,
		_ testRawOption,
	) ([]byte, error) {
		var ok bool
		captured, ok = proto.Clone(killReq).(*core.KillRequest)
		require.True(t, ok)
		var options core.SnapOptions
		require.NoError(t, proto.Unmarshal(killReq.GetPayload(), &options))
		require.Equal(t, common.SnapType_PAUSE_RESUME, options.GetType())
		require.Equal(t, int32(90_000), options.GetTtl())
		payload, err := proto.Marshal(&core.SnapInfo{SnapshotID: requestID, Size: 8192})
		require.NoError(t, err)
		return proto.Marshal(&core.KillResponse{
			Code:    common.ErrorCode_ERR_NONE,
			Payload: payload,
		})
	}})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "default-sandbox-1"}}
	ctx.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes/default-sandbox-1/pause",
		bytes.NewBufferString(`{"ttlSeconds":90000}`),
	)
	ctx.Request.Header.Set("X-ADX-Request-ID", requestID)
	startedAt := time.Now().Unix()

	PauseV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, captured)
	require.Equal(t, "default-sandbox-1", captured.GetInstanceID())
	require.Equal(t, int32(18), captured.GetSignal())
	require.Equal(t, requestID, captured.GetRequestID())
	var response httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	var result struct {
		SandboxID  string  `json:"sandboxId"`
		SnapshotID string  `json:"snapshotId"`
		Size       float64 `json:"size"`
		State      string  `json:"state"`
		ExpiresAt  float64 `json:"expiresAt"`
	}
	require.NoError(t, json.Unmarshal(response.Data, &result))
	require.Equal(t, "default-sandbox-1", result.SandboxID)
	require.Equal(t, requestID, result.SnapshotID)
	require.Equal(t, float64(8192), result.Size)
	require.Equal(t, "paused", result.State)
	require.GreaterOrEqual(t, int64(result.ExpiresAt), startedAt+90_000)
	summary, ok := instancecache.Default().GetSummary("default-sandbox-1")
	require.True(t, ok)
	require.Equal(t, int32(13), summary.StatusCode)
	require.Equal(t, "paused", summary.StatusMsg)
}

func TestResumeV1HandlerUsesSDKRequestIDForSignal19AndReturnsRoute(t *testing.T) {
	const requestID = "resume-123e4567-e89b-12d3-a456-426614174000"
	instancecache.Default().PutSummary(instancecache.Summary{
		InstanceID: "default-sandbox-1", TenantID: "default", Function: "default/sandbox/$latest",
		NodeID: "InstanceManagerOwner", StatusCode: instancecache.StatusPaused, StatusMsg: "paused",
	})
	t.Cleanup(func() { instancecache.Default().Delete("default-sandbox-1") })
	var captured *core.KillRequest
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(
		killReq *core.KillRequest,
		_ testRawOption,
	) ([]byte, error) {
		var ok bool
		captured, ok = proto.Clone(killReq).(*core.KillRequest)
		require.True(t, ok)
		var options core.SnapStartOptions
		require.NoError(t, proto.Unmarshal(killReq.GetPayload(), &options))
		require.Equal(t, common.SnapType_PAUSE_RESUME, options.GetType())
		payload, err := proto.Marshal(&core.SnapStartedInfo{
			InstanceID:      "default-sandbox-1",
			RouteAddress:    "10.0.0.8:9000",
			PortMappings:    `{"8080":41080}`,
			FunctionProxyID: "proxy-a",
			NodeID:          "node-a",
		})
		require.NoError(t, err)
		return proto.Marshal(&core.KillResponse{
			Code:    common.ErrorCode_ERR_NONE,
			Payload: payload,
		})
	}})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "default-sandbox-1"}}
	ctx.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes/default-sandbox-1/resume",
		nil,
	)
	ctx.Request.Header.Set("X-ADX-Request-ID", requestID)

	ResumeV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, captured)
	require.Equal(t, "default-sandbox-1", captured.GetInstanceID())
	require.Equal(t, int32(19), captured.GetSignal())
	require.Equal(t, requestID, captured.GetRequestID())
	var response httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	var result struct {
		State           string         `json:"state"`
		RouteAddress    string         `json:"routeAddress"`
		FunctionProxyID string         `json:"functionProxyId"`
		NodeID          string         `json:"nodeId"`
		PortMappings    map[string]int `json:"portMappings"`
	}
	require.NoError(t, json.Unmarshal(response.Data, &result))
	require.Equal(t, "running", result.State)
	require.Equal(t, "10.0.0.8:9000", result.RouteAddress)
	require.Equal(t, "proxy-a", result.FunctionProxyID)
	require.Equal(t, "node-a", result.NodeID)
	require.Equal(t, map[string]int{"8080": 41080}, result.PortMappings)
	summary, ok := instancecache.Default().GetSummary("default-sandbox-1")
	require.True(t, ok)
	require.Equal(t, int32(instancecache.StatusPaused), summary.StatusCode)
	require.Equal(t, "paused", summary.StatusMsg)
	require.Equal(t, "InstanceManagerOwner", summary.NodeID)
}

func TestReloadV1HandlerUsesSignal25AndReturnsBoolean(t *testing.T) {
	const requestID = "reload-123e4567-e89b-12d3-a456-426614174000"
	var captured *core.KillRequest
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(
		killReq *core.KillRequest,
		_ testRawOption,
	) ([]byte, error) {
		cloned, ok := proto.Clone(killReq).(*core.KillRequest)
		require.True(t, ok)
		captured = cloned
		return proto.Marshal(&core.KillResponse{Code: common.ErrorCode_ERR_NONE})
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "default-sandbox-1"}}
	ctx.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes/default-sandbox-1/reload",
		nil,
	)
	ctx.Request.Header.Set(sandboxLifecycleRequestIDHeader, requestID)

	ReloadV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, captured)
	require.Equal(t, int32(25), captured.GetSignal())
	require.Equal(t, requestID, captured.GetRequestID())
	var response httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	var result reloadV1Response
	require.NoError(t, json.Unmarshal(response.Data, &result))
	require.True(t, result.Success)
}

func TestReloadV1HandlerReturnsFalseOnBusinessFailure(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(
		_ *core.KillRequest,
		_ testRawOption,
	) ([]byte, error) {
		return proto.Marshal(&core.KillResponse{
			Code:    common.ErrorCode_ERR_INNER_SYSTEM_ERROR,
			Message: "local snapshot is unavailable",
		})
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "default-sandbox-1"}}
	ctx.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes/default-sandbox-1/reload",
		nil,
	)
	ctx.Request.Header.Set(
		sandboxLifecycleRequestIDHeader,
		"reload-123e4567-e89b-12d3-a456-426614174001",
	)

	ReloadV1Handler(ctx)

	require.Equal(t, http.StatusConflict, recorder.Code)
	var response httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	var result reloadV1Response
	require.NoError(t, json.Unmarshal(response.Data, &result))
	require.False(t, result.Success)
}

func TestUpdateNetworkV1HandlerUsesSignal26AndCanonicalPolicy(t *testing.T) {
	const requestID = "network-123e4567-e89b-12d3-a456-426614174000"
	var captured *core.KillRequest
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(
		killReq *core.KillRequest,
		_ testRawOption,
	) ([]byte, error) {
		cloned, ok := proto.Clone(killReq).(*core.KillRequest)
		require.True(t, ok)
		captured = cloned
		return proto.Marshal(&core.KillResponse{Code: common.ErrorCode_ERR_NONE})
	}})
	body := `{
		"schemaVersion": 2,
		"traffic": {
			"ingressDefaultAction": "allow",
			"egressDefaultAction": "deny",
			"mode": "stateful",
			"rules": [{
				"action": "allow",
				"direction": "egress",
				"protocol": "tcp",
				"peer": {"domain": "API.GitHub.COM."},
				"priority": 100
			}]
		}
	}`
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "default-sandbox-1"}}
	ctx.Request = httptest.NewRequest(
		http.MethodPut,
		"/api/sandbox/v1/sandboxes/default-sandbox-1/network",
		strings.NewReader(body),
	)
	ctx.Request.Header.Set(sandboxLifecycleRequestIDHeader, requestID)

	UpdateNetworkV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, captured)
	require.Equal(t, int32(26), captured.GetSignal())
	require.Equal(t, requestID, captured.GetRequestID())
	var policy SandboxNetworkPolicy
	require.NoError(t, json.Unmarshal(captured.GetPayload(), &policy))
	require.Equal(t, uint32(2), policy.SchemaVersion)
	require.Equal(t, "api.github.com", policy.Traffic.Rules[0].Peer.Domain)
	var response httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	var result updateNetworkV1Response
	require.NoError(t, json.Unmarshal(response.Data, &result))
	require.True(t, result.Success)
}

func TestUpdateNetworkV1HandlerClearsWithEmptyPolicy(t *testing.T) {
	var captured *core.KillRequest
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(
		killReq *core.KillRequest,
		_ testRawOption,
	) ([]byte, error) {
		cloned, ok := proto.Clone(killReq).(*core.KillRequest)
		require.True(t, ok)
		captured = cloned
		return proto.Marshal(&core.KillResponse{Code: common.ErrorCode_ERR_NONE})
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "default-sandbox-1"}}
	ctx.Request = httptest.NewRequest(
		http.MethodPut,
		"/api/sandbox/v1/sandboxes/default-sandbox-1/network",
		strings.NewReader("{}"),
	)
	ctx.Request.Header.Set(
		sandboxLifecycleRequestIDHeader,
		"network-123e4567-e89b-12d3-a456-426614174001",
	)

	UpdateNetworkV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, captured)
	require.JSONEq(t, "{}", string(captured.GetPayload()))
}

func TestUpdateNetworkV1HandlerRejectsUnknownFields(t *testing.T) {
	called := false
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(
		_ *core.KillRequest,
		_ testRawOption,
	) ([]byte, error) {
		called = true
		return nil, nil
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "default-sandbox-1"}}
	ctx.Request = httptest.NewRequest(
		http.MethodPut,
		"/api/sandbox/v1/sandboxes/default-sandbox-1/network",
		strings.NewReader(`{"blockNetwrok":true}`),
	)
	ctx.Request.Header.Set(
		sandboxLifecycleRequestIDHeader,
		"network-123e4567-e89b-12d3-a456-426614174002",
	)

	UpdateNetworkV1Handler(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.False(t, called)
}

func TestResumeV1HandlerDoesNotRequireLocalSandboxRouter(t *testing.T) {
	const instanceID = "default-sandbox-resume-without-local-router"
	const requestID = "resume-123e4567-e89b-12d3-a456-426614174002"
	instancecache.Default().PutSummary(instancecache.Summary{
		InstanceID: instanceID, TenantID: "default", Function: "default/sandbox/$latest",
		NodeID: "InstanceManagerOwner", StatusCode: instancecache.StatusPaused, StatusMsg: "paused",
	})
	t.Cleanup(func() { instancecache.Default().Delete(instanceID) })

	runtimeClient := &runtimeStub{killRaw: func(
		_ *core.KillRequest, _ testRawOption,
	) ([]byte, error) {
		payload, err := proto.Marshal(&core.SnapStartedInfo{
			InstanceID: instanceID, RouteAddress: "10.0.0.10:22772",
			PortMappings:    `["public+http:43080:8080"]`,
			FunctionProxyID: "target-proxy", NodeID: "target-node",
		})
		require.NoError(t, err)
		return proto.Marshal(&core.KillResponse{Code: common.ErrorCode_ERR_NONE, Payload: payload})
	}}
	restoreDirectClient := setTransportForTest(t, &directRuntimeStub{runtime: runtimeClient})
	t.Cleanup(restoreDirectClient)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: instanceID}}
	ctx.Request = httptest.NewRequest(http.MethodPost, "/sandboxes/"+instanceID+"/resume", nil)
	ctx.Request.Header.Set(sandboxLifecycleRequestIDHeader, requestID)

	ResumeV1Handler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var response httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	var result struct {
		State           string         `json:"state"`
		FunctionProxyID string         `json:"functionProxyId"`
		PortMappings    map[string]int `json:"portMappings"`
	}
	require.NoError(t, json.Unmarshal(response.Data, &result))
	require.Equal(t, "running", result.State)
	require.Equal(t, "target-proxy", result.FunctionProxyID)
	require.Equal(t, map[string]int{"8080": 43080}, result.PortMappings)
}

func TestPauseV1HandlerRejectsSnapshotFromDifferentSDKRequest(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(
		_ *core.KillRequest,
		_ testRawOption,
	) ([]byte, error) {
		payload, err := proto.Marshal(&core.SnapInfo{
			SnapshotID: "pause-different-request",
			Size:       8192,
		})
		require.NoError(t, err)
		return proto.Marshal(&core.KillResponse{Code: common.ErrorCode_ERR_NONE, Payload: payload})
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "sandbox-1"}}
	ctx.Request = httptest.NewRequest(http.MethodPost, "/sandboxes/sandbox-1/pause",
		bytes.NewBufferString(`{"ttlSeconds":90000}`))
	ctx.Request.Header.Set(sandboxLifecycleRequestIDHeader,
		"pause-123e4567-e89b-12d3-a456-426614174000")

	PauseV1Handler(ctx)

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.Contains(t, recorder.Body.String(), "invalid pause response identity")
}

func TestResumeV1HandlerRejectsDifferentWinnerInstance(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(
		_ *core.KillRequest,
		_ testRawOption,
	) ([]byte, error) {
		payload, err := proto.Marshal(&core.SnapStartedInfo{
			InstanceID:      "different-sandbox",
			RouteAddress:    "10.0.0.8:9000",
			FunctionProxyID: "proxy-a",
		})
		require.NoError(t, err)
		return proto.Marshal(&core.KillResponse{Code: common.ErrorCode_ERR_NONE, Payload: payload})
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "sandbox-1"}}
	ctx.Request = httptest.NewRequest(http.MethodPost, "/sandboxes/sandbox-1/resume", nil)
	ctx.Request.Header.Set(sandboxLifecycleRequestIDHeader,
		"resume-123e4567-e89b-12d3-a456-426614174000")

	ResumeV1Handler(ctx)

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.Contains(t, recorder.Body.String(), "invalid resume response identity")
}

func TestPauseResumeV1HandlersRejectMissingSDKRequestID(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		path    string
		handler gin.HandlerFunc
	}{
		{name: "pause", path: "/api/sandbox/v1/sandboxes/sandbox-1/pause", handler: PauseV1Handler},
		{name: "resume", path: "/api/sandbox/v1/sandboxes/sandbox-1/resume", handler: ResumeV1Handler},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Params = gin.Params{{Key: "sandboxID", Value: "sandbox-1"}}
			body := bytes.NewBuffer(nil)
			if testCase.name == "pause" {
				body = bytes.NewBufferString(`{}`)
			}
			ctx.Request = httptest.NewRequest(http.MethodPost, testCase.path, body)

			testCase.handler(ctx)

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Contains(t, recorder.Body.String(), "X-ADX-Request-ID")
		})
	}
}

func TestPauseV1HandlerMapsUnknownDispatchToRetryableServiceUnavailable(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(
		_ *core.KillRequest,
		_ testRawOption,
	) ([]byte, error) {
		return nil, errors.New("frontend proxy response lost")
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "sandbox-1"}}
	ctx.Request = httptest.NewRequest(http.MethodPost, "/sandboxes/sandbox-1/pause",
		bytes.NewBufferString(`{"ttlSeconds":90000}`))
	ctx.Request.Header.Set(sandboxLifecycleRequestIDHeader,
		"pause-123e4567-e89b-12d3-a456-426614174000")

	PauseV1Handler(ctx)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}

func TestResumeV1HandlerDoesNotMakeBusinessFailureTransportRetryable(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{killRaw: func(
		_ *core.KillRequest,
		_ testRawOption,
	) ([]byte, error) {
		return proto.Marshal(&core.KillResponse{
			Code:    common.ErrorCode_ERR_STATE_MACHINE_ERROR,
			Message: "instance is not paused",
		})
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "sandboxID", Value: "sandbox-1"}}
	ctx.Request = httptest.NewRequest(http.MethodPost, "/sandboxes/sandbox-1/resume", nil)
	ctx.Request.Header.Set(sandboxLifecycleRequestIDHeader,
		"resume-123e4567-e89b-12d3-a456-426614174000")

	ResumeV1Handler(ctx)

	require.Equal(t, http.StatusConflict, recorder.Code)
}

func TestDeleteHandlerDeletesSandboxInstance(t *testing.T) {
	var (
		capturedInstanceID string
		capturedSignal     int
		capturedPayload    []byte
	)
	setAPIClientsForTest(t, &runtimeStub{
		kill: func(instanceID string, signal int, payload []byte, invokeOpt createOptions) error {
			capturedInstanceID = instanceID
			capturedSignal = signal
			capturedPayload = append([]byte(nil), payload...)
			require.NotEmpty(t, invokeOpt.TraceID)
			return nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "instanceId", Value: "sandbox-delete-ok"}}
	req, err := http.NewRequest(http.MethodDelete, "/api/sandbox/sandbox-delete-ok", nil)
	require.NoError(t, err)
	ctx.Request = req

	DeleteHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "sandbox-delete-ok", capturedInstanceID)
	require.Equal(t, httpx.KillSignalVal, capturedSignal)
	require.Equal(t, []byte("sandbox deleted"), capturedPayload)

	var resp httpx.Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, http.StatusOK, resp.Code)

	var data map[string]string
	require.NoError(t, json.Unmarshal(resp.Data, &data))
	require.Equal(t, "deleted", requireStringMapValue(t, data, "status"))
}

func TestDeleteHandlerForwardsClientRequestID(t *testing.T) {
	const requestID = "delete-123e4567-e89b-12d3-a456-426614174000"
	var captured *core.KillRequest
	setAPIClientsForTest(t, &runtimeStub{
		killRaw: func(request *core.KillRequest, _ testRawOption) ([]byte, error) {
			cloned, ok := proto.Clone(request).(*core.KillRequest)
			require.True(t, ok)
			captured = cloned
			return proto.Marshal(&core.KillResponse{Code: common.ErrorCode_ERR_NONE})
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "instanceId", Value: "sandbox-delete-request-id"}}
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/api/sandbox/sandbox-delete-request-id", nil)
	ctx.Request.Header.Set(httpx.HeaderRequestID, requestID)

	DeleteHandler(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, captured)
	require.Equal(t, requestID, captured.GetRequestID())
}

func TestDeleteHandlerReturns500WhenKillFails(t *testing.T) {
	setAPIClientsForTest(t, &runtimeStub{
		kill: func(instanceID string, signal int, payload []byte, invokeOpt createOptions) error {
			return fmt.Errorf("kill failed")
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "instanceId", Value: "sandbox-delete-fail"}}
	req, err := http.NewRequest(http.MethodDelete, "/api/sandbox/sandbox-delete-fail", nil)
	require.NoError(t, err)
	ctx.Request = req

	DeleteHandler(ctx)

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.Contains(t, recorder.Body.String(), "failed to delete sandbox")
}

func setupDeleteTenantSummary(t *testing.T) string {
	t.Helper()
	targetInstance := "sandbox-delete-rbac-target"
	instancecache.Default().PutSummary(instancecache.Summary{
		InstanceID: targetInstance,
		TenantID:   "tenant-owner",
	})
	t.Cleanup(func() { instancecache.Default().Delete(targetInstance) })
	return targetInstance
}

func deleteTestContext(t *testing.T, targetInstance, sub, role string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "instanceId", Value: targetInstance}}
	ctx.Set("jwt_sub", sub)
	ctx.Set("jwt_role", role)
	req, err := http.NewRequest(http.MethodDelete, "/api/sandbox/"+targetInstance, nil)
	require.NoError(t, err)
	ctx.Request = req
	return ctx, recorder
}

func TestDeleteHandlerRejectsUnauthenticatedPlaceholderToken(t *testing.T) {

	targetInstance := "sandbox-delete-auth-disabled"
	killCalled := false
	setAPIClientsForTest(t, &runtimeStub{kill: func(instanceID string, _ int, _ []byte, _ createOptions) error {
		killCalled = true
		require.Equal(t, targetInstance, instanceID)
		return nil
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "instanceId", Value: targetInstance}}
	req, err := http.NewRequest(http.MethodDelete, "/api/sandbox/"+targetInstance, nil)
	require.NoError(t, err)
	req.Header.Set("X-Auth-Token", "ci")
	ctx.Request = req

	DeleteHandler(ctx)

	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.False(t, killCalled)
}

func TestDeleteHandlerRejectsCrossTenant(t *testing.T) {
	targetInstance := setupDeleteTenantSummary(t)
	killCalled := false
	setAPIClientsForTest(t, &runtimeStub{kill: func(string, int, []byte, createOptions) error {
		killCalled = true
		return nil
	}})
	ctx, recorder := deleteTestContext(t, targetInstance, "tenant-other", backend.RoleTenant)
	DeleteHandler(ctx)
	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.False(t, killCalled)
}

func TestDeleteHandlerAllowsSameTenant(t *testing.T) {
	targetInstance := setupDeleteTenantSummary(t)
	killCalled := false
	setAPIClientsForTest(t, &runtimeStub{kill: func(instanceID string, _ int, _ []byte, _ createOptions) error {
		killCalled = true
		require.Equal(t, targetInstance, instanceID)
		return nil
	}})
	ctx, recorder := deleteTestContext(t, targetInstance, "tenant-owner", backend.RoleTenant)
	DeleteHandler(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, killCalled)
}

func TestDeleteHandlerEnforcesHeaderWithoutMiddleware(t *testing.T) {
	targetInstance := setupDeleteTenantSummary(t)
	killCalled := false
	setAPIClientsForTest(t, &runtimeStub{kill: func(string, int, []byte, createOptions) error {
		killCalled = true
		return nil
	}})
	ctx, recorder := deleteTestContext(t, targetInstance, "", "")
	ctx.Request.Header.Set("X-Auth-Token", testJWT("tenant-other", backend.RoleTenant))
	DeleteHandler(ctx)
	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.False(t, killCalled)
}

func TestDeleteHandlerAllowsAdministrator(t *testing.T) {
	targetInstance := setupDeleteTenantSummary(t)
	killCalled := false
	setAPIClientsForTest(t, &runtimeStub{kill: func(instanceID string, _ int, _ []byte, _ createOptions) error {
		killCalled = true
		require.Equal(t, targetInstance, instanceID)
		return nil
	}})
	ctx, recorder := deleteTestContext(t, targetInstance, "0", backend.RoleAdmin)
	DeleteHandler(ctx)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, killCalled)
}

func TestAuthorizeSandboxLifecycleAllowsAdministratorWithoutRunningSummary(t *testing.T) {
	targetInstance := "sandbox-paused-not-in-exec-cache"
	ctx, _ := deleteTestContext(t, targetInstance, "0", backend.RoleAdmin)

	status, err := authorizeSandboxDelete(ctx, targetInstance)

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
}

func testJWT(sub, role string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(
		`{"sub":%q,"role":%q,"exp":%d}`,
		sub, role, time.Now().Add(time.Hour).Unix(),
	)))
	return header + "." + payload + ".signature"
}

func invokeCreateV1ForNetworkPolicyTest(
	t *testing.T,
	body string,
) (*httptest.ResponseRecorder, *core.CreateRequest, bool) {
	t.Helper()
	var capturedCreateReq *core.CreateRequest
	createCalled := false
	setAPIClientsForTest(t, &runtimeStub{
		createInstanceRaw: func(
			createReq *core.CreateRequest,
			_ testRawOption,
		) ([]byte, error) {
			createCalled = true
			capturedCreateReq = cloneCreateRequest(t, createReq)
			return rawCreateNotify(0, ""), nil
		},
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request, err := http.NewRequest(
		http.MethodPost,
		"/api/sandbox/v1/sandboxes",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	ctx.Request = request

	CreateV1Handler(ctx)
	return recorder, capturedCreateReq, createCalled
}

func TestCreateV1HandlerPassesBlockNetworkPolicy(t *testing.T) {
	recorder, captured, called := invokeCreateV1ForNetworkPolicyTest(
		t,
		`{"network":{"blockNetwork":true}}`,
	)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, called)
	require.NotNil(t, captured)
	require.JSONEq(
		t,
		`{"blockNetwork":true}`,
		captured.GetCreateOptions()["network_policy"],
	)
}

func TestCreateV1HandlerNormalizesDNSBlacklist(t *testing.T) {
	recorder, captured, called := invokeCreateV1ForNetworkPolicyTest(
		t,
		`{"network":{"dnsBlacklist":["GitHub.COM.","*.GitHub.com","github.com"]}}`,
	)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, called)
	require.NotNil(t, captured)
	require.JSONEq(
		t,
		`{"dnsBlacklist":["github.com","*.github.com"]}`,
		captured.GetCreateOptions()["network_policy"],
	)
}

func TestCreateV1HandlerNormalizesACLVersion2(t *testing.T) {
	recorder, captured, called := invokeCreateV1ForNetworkPolicyTest(
		t,
		`{
            "network": {
                "schemaVersion": 2,
                "traffic": {
                    "ingressDefaultAction": "ALLOW",
                    "egressDefaultAction": "deny",
                    "mode": "stateful",
                    "rules": [
                        {
                            "action": "allow",
                            "direction": "egress",
                            "protocol": "tcp",
                            "peer": {
                                "cidr": "192.0.2.129/24",
                                "portRange": {"first": 80, "last": 443}
                            },
                            "priority": 110
                        },
                        {
                            "action": "deny",
                            "direction": "egress",
                            "protocol": "udp",
                            "peer": {"domain": "BÜCHER.example."},
                            "sandboxPortRange": {"first": 5000, "last": 5010}
                        }
                    ]
                },
                "dns": {
                    "defaultAction": "allow",
                    "rules": [
                        {"action": "deny", "pattern": "*.Example.COM."}
                    ]
                }
            }
        }`,
	)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, called)
	require.NotNil(t, captured)
	require.JSONEq(
		t,
		`{
            "schemaVersion": 2,
            "traffic": {
                "ingressDefaultAction": "allow",
                "egressDefaultAction": "deny",
                "mode": "stateful",
                "rules": [
                    {
                        "action": "allow",
                        "direction": "egress",
                        "protocol": "tcp",
                        "peer": {
                            "cidr": "192.0.2.0/24",
                            "portRange": {"first": 80, "last": 443}
                        },
                        "priority": 110
                    },
                    {
                        "action": "deny",
                        "direction": "egress",
                        "protocol": "udp",
                        "peer": {"domain": "xn--bcher-kva.example"},
                        "sandboxPortRange": {"first": 5000, "last": 5010},
                        "priority": 100
                    }
                ]
            },
            "dns": {
                "defaultAction": "allow",
                "rules": [
                    {"action": "deny", "pattern": "*.example.com"}
                ]
            }
        }`,
		captured.GetCreateOptions()["network_policy"],
	)
}

func TestCreateV1HandlerOmitsEmptyACLVersion2(t *testing.T) {
	recorder, captured, called := invokeCreateV1ForNetworkPolicyTest(
		t,
		`{"network":{"schemaVersion":2}}`,
	)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, called)
	require.NotNil(t, captured)
	_, exists := captured.GetCreateOptions()["network_policy"]
	require.False(t, exists)
}

func TestCreateV1HandlerOmitsEmptyNetworkPolicy(t *testing.T) {
	recorder, captured, called := invokeCreateV1ForNetworkPolicyTest(
		t,
		`{"network":{}}`,
	)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, called)
	require.NotNil(t, captured)
	_, exists := captured.GetCreateOptions()["network_policy"]
	require.False(t, exists)
}

func TestCreateV1HandlerRejectsInvalidNetworkPolicy(t *testing.T) {
	invalidBodies := []string{
		`{"network":{"blockNetwork":true,"dnsBlacklist":["github.com"]}}`,
		`{"network":{"dnsBlacklist":["github.*"]}}`,
		`{"network":{"dnsBlacklist":["github..com"]}}`,
		`{"network":{"dnsBlacklist":["github.com.."]}}`,
		`{"network":{"traffic":{"ingressDefaultAction":"allow"}}}`,
		`{"network":{"schemaVersion":2,"blockNetwork":true}}`,
		`{"network":{"schemaVersion":3}}`,
		`{"network":{"schemaVersion":2,"unknown":true}}`,
		`{"network":{"schemaVersion":2,"traffic":{"ingressDefaultAction":"allow","egressDefaultAction":"deny","unknown":true}}}`,
		`{"network":{"schemaVersion":2,"traffic":{"ingressDefaultAction":"allow","egressDefaultAction":"deny","rules":[{"action":"allow","direction":"egress","protocol":"tcp","unknown":true}]}}}`,
		`{"network":{"schemaVersion":2,"traffic":{"ingressDefaultAction":"allow","egressDefaultAction":"deny","rules":[{"action":"allow","direction":"egress","protocol":"tcp","peer":{"unknown":true}}]}}}`,
		`{"network":{"schemaVersion":2,"traffic":{"ingressDefaultAction":"allow","egressDefaultAction":"deny","rules":[{"action":"allow","direction":"egress","protocol":"tcp","peer":{"portRange":{"first":80,"last":443,"unknown":true}}}]}}}`,
		`{"network":{"schemaVersion":2,"dns":{"defaultAction":"deny","rules":[{"action":"allow","pattern":"example.com","unknown":true}]}}}`,
		`{"network":{"schemaVersion":2,"traffic":{"ingressDefaultAction":"allow","egressDefaultAction":"deny","mode":"stateful","rules":[{"action":"allow","direction":"ingress","protocol":"tcp","peer":{"domain":"example.com"},"priority":100}]}}}`,
		`{"network":{"schemaVersion":2,"traffic":{"ingressDefaultAction":"allow","egressDefaultAction":"deny","mode":"stateful","rules":[{"action":"allow","direction":"egress","protocol":"tcp","peer":{"cidr":"2001:db8::/32"},"priority":100}]}}}`,
		`{"network":{"schemaVersion":2,"traffic":{"ingressDefaultAction":"allow","egressDefaultAction":"deny","mode":"stateful","rules":[{"action":"allow","direction":"egress","protocol":"any","peer":{"portRange":{"first":443,"last":443}},"priority":100}]}}}`,
		`{"network":{"schemaVersion":2,"traffic":{"ingressDefaultAction":"allow","egressDefaultAction":"deny","mode":"stateful","rules":[{"action":"allow","direction":"egress","protocol":"tcp","priority":4294967295}]}}}`,
	}
	for _, body := range invalidBodies {
		t.Run(body, func(t *testing.T) {
			recorder, captured, called := invokeCreateV1ForNetworkPolicyTest(t, body)

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.False(t, called)
			require.Nil(t, captured)
		})
	}
}

func TestAffinityWeightAndModeValidation(t *testing.T) {
	conditions := []Affinity{{Kind: AffinityKindResource, Affinity: PreferredAffinity, PreferredPriority: true, Weight: 9, LabelOps: []LabelOperator{{Type: LabelOpExists, LabelKey: "a"}}}}
	require.NoError(t, validateScheduleAffinities(conditions))
	raw, err := buildSandboxRawScheduleAffinity(conditions)
	require.NoError(t, err)
	require.Equal(t, int64(9), raw.Resource.PreferredAffinity.Condition.SubConditions[0].Weight)
	conditions = append(conditions, Affinity{Kind: AffinityKindResource, Affinity: PreferredAffinity, LabelOps: []LabelOperator{{Type: LabelOpExists, LabelKey: "b"}}})
	_, err = buildSandboxRawScheduleAffinity(conditions)
	require.Error(t, err)
	conditions[0].Weight = -1
	require.Error(t, validateScheduleAffinities(conditions))
}
