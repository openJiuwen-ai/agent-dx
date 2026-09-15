package controlbackend

import (
	"context"
	"crypto/sha256"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
	"time"
)

type authEntry struct {
	identity backend.Identity
	until    time.Time
}
type Authenticator struct {
	client       pb.AuthServiceClient
	ttl, timeout time.Duration
	limit        int
	mu           sync.Mutex
	entries      map[[32]byte]authEntry
}

func NewAuthenticator(client pb.AuthServiceClient, ttl, timeout time.Duration, limit int) (*Authenticator, error) {
	if client == nil || ttl <= 0 || timeout <= 0 || limit < 1 {
		return nil, status.Error(codes.InvalidArgument, "invalid auth cache configuration")
	}
	return &Authenticator{client: client, ttl: ttl, timeout: timeout, limit: limit, entries: map[[32]byte]authEntry{}}, nil
}
func (a *Authenticator) Verify(ctx context.Context, key string) (backend.Identity, error) {
	if len(key) < 32 || len(key) > 512 {
		return backend.Identity{}, status.Error(codes.Unauthenticated, "invalid credential")
	}
	hash := sha256.Sum256([]byte(key))
	now := time.Now()
	a.mu.Lock()
	cached, ok := a.entries[hash]
	a.mu.Unlock()
	if ok && now.Before(cached.until) {
		return cached.identity, nil
	}
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	value, err := a.client.VerifyApiKey(ctx, &pb.VerifyApiKeyRequest{ApiKey: key})
	if err != nil {
		return backend.Identity{}, err
	}
	if value == nil || value.Caller == nil || value.Caller.TenantId == "" {
		return backend.Identity{}, status.Error(codes.Unauthenticated, "invalid identity response")
	}
	until := now.Add(a.ttl)
	if value.ExpiresAtUnixSeconds != 0 {
		if value.ExpiresAtUnixSeconds > 1<<63-1 {
			return backend.Identity{}, status.Error(codes.Unauthenticated, "invalid credential expiry")
		}
		expiry := time.Unix(int64(value.ExpiresAtUnixSeconds), 0)
		if !expiry.After(time.Now()) {
			return backend.Identity{}, status.Error(codes.Unauthenticated, "expired credential")
		}
		if expiry.Before(until) {
			until = expiry
		}
	}
	role := backend.RoleTenant
	if value.Caller.Administrator {
		role = backend.RoleAdmin
	}
	identity := backend.Identity{TenantID: value.Caller.TenantId, Role: role}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.entries) >= a.limit {
		var oldest [32]byte
		var deadline time.Time
		for k, v := range a.entries {
			if deadline.IsZero() || v.until.Before(deadline) {
				oldest, deadline = k, v.until
			}
		}
		delete(a.entries, oldest)
	}
	a.entries[hash] = authEntry{identity: identity, until: until}
	return identity, nil
}
