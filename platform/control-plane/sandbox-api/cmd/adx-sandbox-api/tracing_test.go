package main

import (
	"context"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTraceHTTPToRPC(t *testing.T) {
	t.Setenv("ADX_TRACE_ENABLED", "false")
	shutdown, err := initTracing(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown()
	r := gin.New()
	r.Use(traceHTTP())
	var parent string
	r.GET("/instances/:id", func(c *gin.Context) {
		err := traceUnary(c.Request.Context(), "/adx.control.v1.NodeService/DeleteInstance", nil, nil, nil, func(ctx context.Context, _ string, _, _ interface{}, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			md, _ := metadata.FromOutgoingContext(ctx)
			parent = md.Get("traceparent")[0]
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		c.Status(200)
	})
	request := httptest.NewRequest(http.MethodGet, "/instances/private-instance?token=private-secret", nil)
	request.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
	r.ServeHTTP(httptest.NewRecorder(), request)
	if len(parent) != 55 || parent[3:35] != "11111111111111111111111111111111" {
		t.Fatalf("context not propagated: %q", parent)
	}
	if parent[36:52] == "2222222222222222" {
		t.Fatal("outgoing request must be a child span")
	}
}
