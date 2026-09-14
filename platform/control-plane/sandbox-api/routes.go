// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Package sandboxapi mounts the existing Sandbox HTTP contract.
// The host must initialize the backend and authentication configuration before serving.
package sandboxapi

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/httpx"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/sandbox"
)

// RegisterRoutes mounts lifecycle and reusable-snapshot endpoints on the supplied
// router. Authentication is provided by the host and is required for every route.
func RegisterRoutes(r gin.IRouter, dependencies backend.Dependencies) error {
	if err := dependencies.Validate(); err != nil {
		return err
	}
	r = r.Group("", authenticate(dependencies))
	legacy := r.Group("/api/sandbox")
	legacy.POST("/create", sandbox.CreateHandler)
	legacy.DELETE("/:instanceId", sandbox.DeleteHandler)
	instances := r.Group("/api/sandbox/v1/sandboxes")
	instances.POST("", sandbox.CreateV1Handler)
	instances.DELETE("/:sandboxID", sandbox.DeleteHandler)
	instances.POST("/:sandboxID/pause", sandbox.PauseV1Handler)
	instances.POST("/:sandboxID/resume", sandbox.ResumeV1Handler)
	instances.POST("/:sandboxID/reload", sandbox.ReloadV1Handler)
	instances.PUT("/:sandboxID/network", sandbox.UpdateNetworkV1Handler)
	instances.POST("/:sandboxID/snapshots", sandbox.CreateReusableSnapshotV1Handler)
	instances.POST("/:sandboxID/invoke", sandbox.InvokeV1Handler)
	snapshots := r.Group("/api/sandbox/v1/snapshots")
	snapshots.GET("", sandbox.ListReusableSnapshotsV1Handler)
	snapshots.GET("/:snapshotID", sandbox.GetReusableSnapshotV1Handler)
	snapshots.DELETE("/:snapshotID", sandbox.DeleteReusableSnapshotV1Handler)
	return nil
}

func authenticate(d backend.Dependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request = c.Request.WithContext(backend.WithDependencies(c.Request.Context(), d))
		token := strings.TrimSpace(c.GetHeader("X-Auth-Token"))
		if authorization := c.GetHeader("Authorization"); strings.HasPrefix(authorization, "Bearer ") {
			token = strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer "))
		}
		if token == "" {
			httpx.SetCtxResponse(c, nil, http.StatusUnauthorized, errMissingCredential)
			c.Abort()
			return
		}
		identity, err := d.Authenticate(c.Request.Context(), token)
		if err != nil || identity.TenantID == "" || (identity.Role != backend.RoleTenant && identity.Role != backend.RoleAdmin) {
			httpx.SetCtxResponse(c, nil, http.StatusUnauthorized, errInvalidCredential)
			c.Abort()
			return
		}
		c.Request = c.Request.WithContext(backend.WithIdentity(c.Request.Context(), identity))
		c.Set("jwt_sub", identity.TenantID)
		c.Set("jwt_role", identity.Role)
		c.Request.Header.Del("tenantId")
		c.Request.Header.Set(httpx.HeaderTenantID, identity.TenantID)
		c.Next()
	}
}

var errMissingCredential = errors.New("missing API key")
var errInvalidCredential = errors.New("invalid API key")
