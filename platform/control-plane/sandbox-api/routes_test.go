// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
package sandboxapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
)

type recordingBackend struct {
	calls int
	last  backend.LifecycleRequest
}

func (b *recordingBackend) Create(backend.Request) ([]byte, error) {
	return nil, errors.New("unexpected create")
}
func (b *recordingBackend) Invoke(backend.Request) ([]byte, error) {
	return nil, errors.New("unexpected invoke")
}
func (b *recordingBackend) Lifecycle(r backend.LifecycleRequest) (backend.LifecycleResponse, error) {
	b.calls++
	b.last = r
	return backend.LifecycleResponse{}, nil
}

type ownerReader struct {
	tenant string
	err    error
}

func (r ownerReader) Read(context.Context, string) (*backend.Instance, error) {
	if r.err != nil {
		return nil, r.err
	}
	return &backend.Instance{TenantID: r.tenant, State: "running", CPU: 500, Memory: 512, Image: "image:test"}, nil
}
func (r ownerReader) ConfirmDeleted(context.Context, string) (bool, error) { return false, nil }
func (r ownerReader) IsRunning(string) bool                                { return true }
func testDependencies(b *recordingBackend, tenant string) backend.Dependencies {
	return backend.Dependencies{Transport: b, Instances: ownerReader{tenant: tenant},
		Authenticate: func(_ context.Context, key string) (backend.Identity, error) {
			if key != "key-"+tenant {
				return backend.Identity{}, errors.New("invalid")
			}
			return backend.Identity{TenantID: tenant, Role: backend.RoleTenant}, nil
		},
		MasterAddress: func() string { return "http://master.invalid" }, SnapshotHTTPClient: http.DefaultClient}
}
func serveDelete(r http.Handler, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodDelete, "/api/sandbox/owned-instance", nil)
	req.Header.Set("X-Auth-Token", key)
	req.Header.Set("X-Tenant-Id", "forged-owner")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}
func TestRegisterRequiresDependencies(t *testing.T) {
	r := gin.New()
	require.ErrorIs(t, RegisterRoutes(r, backend.Dependencies{}), backend.ErrUnavailable)
	require.Empty(t, r.Routes())
}
func TestAuthenticationAndTenantAttribution(t *testing.T) {
	b := &recordingBackend{}
	r := gin.New()
	require.NoError(t, RegisterRoutes(r, testDependencies(b, "owner")))
	for _, key := range []string{"", "bad"} {
		require.Equal(t, http.StatusUnauthorized, serveDelete(r, key).Code)
	}
	require.Zero(t, b.calls)
	require.Equal(t, http.StatusOK, serveDelete(r, "key-owner").Code)
	require.Equal(t, 1, b.calls)
	require.Equal(t, "owner", b.last.TenantID)
}
func TestOwnershipFailureDoesNotExecute(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reader ownerReader
		status int
	}{
		{"cross tenant", ownerReader{tenant: "other"}, http.StatusForbidden},
		{"missing", ownerReader{err: backend.ErrInstanceNotFound}, http.StatusNotFound},
		{"unavailable", ownerReader{err: backend.ErrUnavailable}, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &recordingBackend{}
			d := testDependencies(b, "owner")
			d.Instances = tc.reader
			r := gin.New()
			require.NoError(t, RegisterRoutes(r, d))
			require.Equal(t, tc.status, serveDelete(r, "key-owner").Code)
			require.Zero(t, b.calls)
		})
	}
}
func TestRegisteredRoutersKeepTheirOwnDependencies(t *testing.T) {
	a, b := &recordingBackend{}, &recordingBackend{}
	ra, rb := gin.New(), gin.New()
	require.NoError(t, RegisterRoutes(ra, testDependencies(a, "a")))
	require.NoError(t, RegisterRoutes(rb, testDependencies(b, "b")))
	require.Equal(t, http.StatusOK, serveDelete(ra, "key-a").Code)
	require.Equal(t, http.StatusOK, serveDelete(rb, "key-b").Code)
	require.Equal(t, "a", a.last.TenantID)
	require.Equal(t, "b", b.last.TenantID)
	require.Equal(t, 1, a.calls)
	require.Equal(t, 1, b.calls)
}

func TestInvokeRejectsCrossTenantBeforeBackend(t *testing.T) {
	b := &recordingBackend{}
	d := testDependencies(b, "owner")
	d.Instances = ownerReader{tenant: "other"}
	r := gin.New()
	require.NoError(t, RegisterRoutes(r, d))
	req := httptest.NewRequest(http.MethodPost, "/api/sandbox/v1/sandboxes/owned-instance/invoke", strings.NewReader(`{"action":"exec"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Auth-Token", "key-owner")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestSDKInstanceSummaryCompatibilityAndAuthorization(t *testing.T) {
	for _, tc := range []struct {
		key, query, owner string
		code              int
	}{
		{"key-owner", "?instance_id=i", "owner", 200},
		{"key-owner", "?instance_id=i", "other", 403},
		{"bad", "?instance_id=i", "owner", 401},
		{"key-owner", "", "owner", 400},
	} {
		d := testDependencies(&recordingBackend{}, "owner")
		d.Instances = ownerReader{tenant: tc.owner}
		r := gin.New()
		require.NoError(t, RegisterRoutes(r, d))
		request := httptest.NewRequest("GET", "/api/instances"+tc.query, nil)
		request.Header.Set("X-Auth-Token", tc.key)
		response := httptest.NewRecorder()
		r.ServeHTTP(response, request)
		require.Equal(t, tc.code, response.Code)
		if tc.code == 200 {
			var items []map[string]any
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &items))
			require.Len(t, items, 1)
			require.Equal(t, "i", items[0]["id"])
			require.Equal(t, "running", items[0]["status"])
			require.Equal(t, float64(512), items[0]["required_mem"])
		}
	}
}
