// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package sandbox

import (
	"context"
	"net/http"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/common"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/core"
)

func createOnBackend(r backend.Request) ([]byte, error) {
	d := backend.Current(r.Context)
	if d.Transport == nil {
		return nil, backend.ErrUnavailable
	}
	return d.Transport.Create(r)
}
func invokeOnBackend(r backend.Request) ([]byte, error) {
	d := backend.Current(r.Context)
	if d.Transport == nil {
		return nil, backend.ErrUnavailable
	}
	return d.Transport.Invoke(r)
}
func newLifecycleRequest(ctx context.Context, id string, signal int, payload []byte, tenant string, opt createOptions) backend.LifecycleRequest {
	return backend.LifecycleRequest{Context: ctx, InstanceID: id, Signal: signal, Payload: payload, TenantID: tenant, TraceID: opt.TraceID, TraceParent: opt.CustomExtensions["traceparent"], TimeoutSeconds: opt.Timeout}
}
func lifecycleOnBackend(r backend.LifecycleRequest) (*core.KillResponse, error) {
	d := backend.Current(r.Context)
	if d.Transport == nil {
		return nil, backend.ErrUnavailable
	}
	v, err := d.Transport.Lifecycle(r)
	if err != nil {
		return nil, err
	}
	return &core.KillResponse{Code: common.ErrorCode(v.Code), Message: v.Message, Payload: v.Payload}, nil
}
func deleteOnBackend(r backend.LifecycleRequest) error {
	v, err := lifecycleOnBackend(r)
	if err != nil {
		return err
	}
	if v.GetCode() != common.ErrorCode_ERR_NONE {
		return &sandboxLifecycleBusinessError{operation: "delete", code: v.GetCode(), message: v.GetMessage()}
	}
	return nil
}

type snapshotMasterEndpoint struct{ ctx context.Context }

func (s snapshotMasterEndpoint) GetActiveMasterAddr() string {
	d := backend.Current(s.ctx)
	if d.MasterAddress == nil {
		return ""
	}
	return d.MasterAddress()
}

type snapshotHTTPTransport struct{}

func (snapshotHTTPTransport) Do(r *http.Request) (*http.Response, error) {
	d := backend.Current(r.Context())
	if d.SnapshotHTTPClient == nil {
		return nil, backend.ErrUnavailable
	}
	return d.SnapshotHTTPClient.Do(r)
}
