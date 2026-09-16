package sandboxapi

import (
	"context"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type keyStore struct{ calls int }

func (s *keyStore) Create(ctx context.Context, tenant string, expiry uint64) (backend.CreatedKey, error) {
	s.calls++
	identity, _ := backend.IdentityFromContext(ctx)
	if identity.Role != backend.RoleAdmin {
		panic("unverified caller")
	}
	return backend.CreatedKey{Key: backend.TenantKey{ID: "id", TenantID: tenant, ExpiresAt: expiry}, APIKey: "created-once"}, nil
}
func (s *keyStore) List(context.Context, string, string, uint32) (backend.KeyPage, error) {
	s.calls++
	return backend.KeyPage{Items: []backend.TenantKey{{ID: "id", TenantID: "t"}}}, nil
}
func (s *keyStore) Revoke(context.Context, string) error { s.calls++; return nil }
func TestCredentialRoutesRequireAdminAndDoNotLeakKeysOnList(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &keyStore{}
	router := func(role string) *gin.Engine {
		r := gin.New()
		r.Use(func(c *gin.Context) {
			ctx := backend.WithDependencies(c.Request.Context(), backend.Dependencies{Keys: store})
			ctx = backend.WithIdentity(ctx, backend.Identity{TenantID: "verified", Role: role})
			c.Request = c.Request.WithContext(ctx)
			c.Next()
		})
		registerCredentialRoutes(r)
		return r
	}
	request := func(role, method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("tenantId", "spoofed")
		router(role).ServeHTTP(w, req)
		return w
	}
	if w := request(backend.RoleTenant, "POST", "/api/admin/v1/keys", `{"tenantId":"t"}`); w.Code != http.StatusForbidden || store.calls != 0 {
		t.Fatalf("tenant accepted: %d", w.Code)
	}
	if w := request(backend.RoleAdmin, "POST", "/api/admin/v1/keys", `{"tenantId":"t","administrator":true}`); w.Code != 400 || store.calls != 0 {
		t.Fatalf("unknown field accepted: %d", w.Code)
	}
	if w := request(backend.RoleAdmin, "POST", "/api/admin/v1/keys", `{"tenantId":"t"}`); w.Code != 201 || !strings.Contains(w.Body.String(), "created-once") || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	if w := request(backend.RoleAdmin, "GET", "/api/admin/v1/keys", ""); w.Code != 200 || strings.Contains(w.Body.String(), "apiKey") {
		t.Fatalf("list: %d %s", w.Code, w.Body.String())
	}
	if w := request(backend.RoleAdmin, "GET", "/api/admin/v1/keys?pageSize=1001", ""); w.Code != 400 {
		t.Fatalf("page limit: %d", w.Code)
	}
	if w := request(backend.RoleAdmin, "DELETE", "/api/admin/v1/keys/id", ""); w.Code != 204 {
		t.Fatalf("delete: %d", w.Code)
	}
}
