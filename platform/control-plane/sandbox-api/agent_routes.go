// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
package sandboxapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
)

// RegisterAgentRoutes preserves the existing Agent HTTP entrypoints. The supplied
// handler belongs to the Agent layer (or proxies to its service); this package
// does not implement Agent business logic. Bodies, query strings, status codes
// and streamed responses are passed through without a Sandbox response wrapper.
func RegisterAgentRoutes(r gin.IRouter, verify func(context.Context, string) (backend.Identity, error), handler http.Handler) error {
	if verify == nil || handler == nil {
		return errors.New("agent authentication and handler are required")
	}
	group := r.Group("/api/agent", authenticate(backend.Dependencies{Authenticate: verify}))
	forward := gin.WrapH(handler)
	group.POST("", forward)
	group.POST("/:instanceId/invoke", forward)
	group.DELETE("/:instanceId", forward)
	group.GET("", forward)
	group.GET("/:instanceId", forward)
	group.POST("/:instanceId/files/upload", forward)
	group.GET("/:instanceId/files/download", forward)
	group.GET("/:instanceId/files/list", forward)
	group.POST("/:instanceId/files/mkdir", forward)
	return nil
}
