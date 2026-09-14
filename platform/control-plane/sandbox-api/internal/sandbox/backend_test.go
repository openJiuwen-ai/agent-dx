// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package sandbox

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
)

type testRawOption struct{ TraceParent string }
type testFunctionMeta struct{ FuncID string }
type testJWTHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}
type testJWTPayload struct {
	Sub string `json:"sub"`
}

func killOptions(req backend.LifecycleRequest) createOptions {
	return createOptions{TraceID: req.TraceID, Timeout: req.TimeoutSeconds, CustomExtensions: map[string]string{"traceparent": req.TraceParent}}
}
func (r *directRuntimeStub) Lifecycle(req backend.LifecycleRequest) (backend.LifecycleResponse, error) {
	v, err := r.KillInstanceWithResponse(req)
	if err != nil {
		return backend.LifecycleResponse{}, err
	}
	return backend.LifecycleResponse{Code: int32(v.GetCode()), Message: v.GetMessage(), Payload: v.GetPayload()}, nil
}

type testInstances struct{}

func (testInstances) Read(context.Context, string) (*backend.Instance, error) {
	return nil, backend.ErrInstanceNotFound
}
func (testInstances) ConfirmDeleted(context.Context, string) (bool, error) {
	return false, backend.ErrUnavailable
}
func (testInstances) IsRunning(string) bool { return false }
func setTransportForTest(t *testing.T, transport backend.Transport) func() {
	t.Helper()
	restore, err := backend.Configure(backend.Dependencies{Transport: transport, Instances: testInstances{},
		Authenticate: func(context.Context, string) (backend.Identity, error) {
			return backend.Identity{}, errors.New("invalid credential")
		},
		MasterAddress: func() string { return "" }, SnapshotHTTPClient: http.DefaultClient})
	if err != nil {
		t.Fatal(err)
	}
	return restore
}
