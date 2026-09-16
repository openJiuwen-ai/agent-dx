package main

import (
	"bytes"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"log/slog"
	"net/http/httptest"
	"testing"
)

func TestRequestLogUsesRouteTemplateWithoutCredentials(t *testing.T) {
	var b bytes.Buffer
	r := gin.New()
	r.Use(requestLogging(slog.New(slog.NewJSONHandler(&b, nil))))
	r.GET("/instances/:id", func(c *gin.Context) { c.Status(200) })
	req := httptest.NewRequest("GET", "/instances/private-id?token=private-query", nil)
	req.Header.Set("Authorization", "Bearer private-key")
	r.ServeHTTP(httptest.NewRecorder(), req)
	var row map[string]any
	if err := json.Unmarshal(b.Bytes(), &row); err != nil {
		t.Fatal(err)
	}
	if row["route"] != "/instances/:id" || row["status"] != float64(200) {
		t.Fatal(row)
	}
	for _, secret := range []string{"private-id", "private-query", "private-key"} {
		if bytes.Contains(b.Bytes(), []byte(secret)) {
			t.Fatal("sensitive request data in log")
		}
	}
}
