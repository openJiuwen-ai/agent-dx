// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package httpx

import (
	"encoding/json"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	DelegateEnvVar = "DELEGATE_ENV_VAR"

	HeaderRequestID      = "X-Request-Id"
	HeaderTraceID        = "X-Trace-Id"
	HeaderTraceParent    = "Traceparent"
	HeaderTenantID       = "X-Tenant-Id"
	KillSignalVal        = 1
	FunctionKeyNote      = "FUNCTION_KEY_NOTE"
	ResourceSpecNote     = "RESOURCE_SPEC_NOTE"
	SchedulerIDNote      = "SCHEDULER_ID_NOTE"
	SchedulerManagedNote = "SCHEDULER_MANAGED"
	InstanceTypeNote     = "INSTANCE_TYPE_NOTE"

	ContentTypeHeaderKey = "Content-Type"
	AcceptEventStream    = "text/event-stream"
)

type Response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    []byte `json:"data"`
}

func SetCtxResponse(c *gin.Context, data any, code int, err error) {
	if code == http.StatusInternalServerError && err != nil {
		switch status.Code(err) {
		case codes.InvalidArgument:
			code = http.StatusBadRequest
		case codes.Unauthenticated:
			code = http.StatusUnauthorized
		case codes.PermissionDenied:
			code = http.StatusForbidden
		case codes.NotFound:
			code = http.StatusNotFound
		case codes.AlreadyExists, codes.FailedPrecondition, codes.Aborted:
			code = http.StatusConflict
		case codes.ResourceExhausted:
			code = http.StatusTooManyRequests
		case codes.Unavailable:
			code = http.StatusServiceUnavailable
		case codes.DeadlineExceeded:
			code = http.StatusGatewayTimeout
		case codes.Unimplemented:
			code = http.StatusNotImplemented
		}
	}
	body, marshalErr := json.Marshal(data)
	response := Response{Code: code}
	if marshalErr != nil {
		response.Code = http.StatusInternalServerError
		response.Message = fmt.Sprintf("marshal response failed, err: %v", marshalErr)
	} else {
		if data != nil {
			response.Data = body
		}
		if err != nil {
			response.Message = err.Error()
		}
	}
	c.JSON(code, response)
}
func GetCompatibleGinHeader(r *http.Request, primary, secondary string) string {
	if v := r.Header.Get(primary); v != "" {
		return v
	}
	return r.Header.Get(secondary)
}
func InitTraceID(c *gin.Context) string {
	id := c.GetHeader(HeaderTraceID)
	if id == "" {
		id = c.GetHeader(HeaderRequestID)
	}
	if id == "" {
		id = uuid.NewString()
	}
	if len(id) > 128 {
		id = id[:128]
	}
	c.Request.Header.Set(HeaderTraceID, id)
	return id
}
func Logger() *zap.SugaredLogger { return zap.S() }
