// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
package sandboxapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
)

func TestAgentRouteContractAndStreamingPassthrough(t *testing.T) {
	routes := []struct{ method, path string }{
		{"POST", "/api/agent"}, {"POST", "/api/agent/id/invoke"}, {"DELETE", "/api/agent/id"},
		{"GET", "/api/agent"}, {"GET", "/api/agent/id"}, {"POST", "/api/agent/id/files/upload"},
		{"GET", "/api/agent/id/files/download"}, {"GET", "/api/agent/id/files/list"}, {"POST", "/api/agent/id/files/mkdir"},
	}
	for _, route := range routes {
		t.Run(route.method+route.path, func(t *testing.T) {
			r := gin.New()
			calls := 0
			handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls++
				require.Equal(t, route.path, req.URL.Path)
				require.Equal(t, "a=1", req.URL.RawQuery)
				body, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.Equal(t, "body\x00data", string(body))
				identity, ok := backend.IdentityFromContext(req.Context())
				require.True(t, ok)
				require.Equal(t, "owner", identity.TenantID)
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusAccepted)
				_, _ = io.WriteString(w, "data: first\n\n")
				w.(http.Flusher).Flush()
				_, _ = io.WriteString(w, "data: second\n\n")
			})
			verify := func(context.Context, string) (backend.Identity, error) {
				return backend.Identity{TenantID: "owner", Role: backend.RoleTenant}, nil
			}
			require.NoError(t, RegisterAgentRoutes(r, verify, handler))
			req := httptest.NewRequest(route.method, route.path+"?a=1", strings.NewReader("body\x00data"))
			req.Header.Set("X-Auth-Token", "key")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			require.Equal(t, 1, calls)
			require.Equal(t, http.StatusAccepted, rec.Code)
			require.True(t, rec.Flushed)
			require.Equal(t, "data: first\n\ndata: second\n\n", rec.Body.String())
		})
	}
}
func TestAgentRoutesRequireConfiguredHandler(t *testing.T) {
	r := gin.New()
	require.Error(t, RegisterAgentRoutes(r, nil, nil))
	require.Empty(t, r.Routes())
}
