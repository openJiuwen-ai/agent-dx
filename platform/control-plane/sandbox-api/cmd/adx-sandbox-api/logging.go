package main

import (
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"time"
)

func requestLogging(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "<unmatched>"
		}
		span := trace.SpanContextFromContext(c.Request.Context())
		logger.InfoContext(c.Request.Context(), "HTTP request completed", "event", "http_request_completed", "trace_id", span.TraceID().String(), "span_id", span.SpanID().String(), "method", c.Request.Method, "route", route, "status", c.Writer.Status(), "duration_seconds", time.Since(start).Seconds())
	}
}
