// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package httpx

import (
	"encoding/json"
	"fmt"
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
