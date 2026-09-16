package sandboxapi

import (
	"encoding/json"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"net/http"
	"strconv"
)

func registerCredentialRoutes(r gin.IRouter) {
	group := r.Group("/api/admin/v1/keys", func(c *gin.Context) {
		identity, ok := backend.IdentityFromContext(c.Request.Context())
		if !ok || identity.Role != backend.RoleAdmin {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "administrator required"})
			return
		}
		if backend.Current(c.Request.Context()).Keys == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "key management unavailable"})
			return
		}
		c.Header("Cache-Control", "no-store")
		c.Next()
	})
	group.POST("", func(c *gin.Context) {
		var input struct {
			TenantID  string `json:"tenantId"`
			ExpiresAt uint64 `json:"expiresAtUnixSeconds"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 8192))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || input.TenantID == "" {
			c.JSON(400, gin.H{"error": "invalid key request"})
			return
		}
		result, err := backend.Current(c.Request.Context()).Keys.Create(c.Request.Context(), input.TenantID, input.ExpiresAt)
		if err != nil {
			credentialError(c, err)
			return
		}
		c.JSON(http.StatusCreated, result)
	})
	group.GET("", func(c *gin.Context) {
		var size uint64
		if raw := c.Query("pageSize"); raw != "" {
			var err error
			size, err = strconv.ParseUint(raw, 10, 32)
			if err != nil || size > 1000 {
				c.JSON(400, gin.H{"error": "invalid page size"})
				return
			}
		}
		result, err := backend.Current(c.Request.Context()).Keys.List(c.Request.Context(), c.Query("tenantId"), c.Query("pageToken"), uint32(size))
		if err != nil {
			credentialError(c, err)
			return
		}
		c.JSON(200, result)
	})
	group.DELETE("/:keyID", func(c *gin.Context) {
		if err := backend.Current(c.Request.Context()).Keys.Revoke(c.Request.Context(), c.Param("keyID")); err != nil {
			credentialError(c, err)
			return
		}
		c.Status(http.StatusNoContent)
	})
}
func credentialError(c *gin.Context, err error) {
	code := http.StatusServiceUnavailable
	message := "key management unavailable"
	switch status.Code(err) {
	case codes.InvalidArgument:
		code = 400
		message = "invalid key request"
	case codes.PermissionDenied:
		code = 403
		message = "administrator operation denied"
	case codes.Unauthenticated:
		code = 401
		message = "authentication required"
	case codes.NotFound:
		code = 404
		message = "key not found"
	}
	c.JSON(code, gin.H{"error": message})
}
