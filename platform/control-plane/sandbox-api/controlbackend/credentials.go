package controlbackend

import (
	"context"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type keyManager struct {
	client  pb.CredentialServiceClient
	timeout time.Duration
}

func NewKeyManager(client pb.CredentialServiceClient, timeout time.Duration) backend.KeyManager {
	return &keyManager{client, timeout}
}
func adminCaller(ctx context.Context) (*pb.CallerContext, error) {
	c, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	if !c.Administrator {
		return nil, status.Error(codes.PermissionDenied, "administrator required")
	}
	return c, nil
}
func keyValue(key *pb.TenantKey) (backend.TenantKey, error) {
	if key == nil || key.Id == "" || key.TenantId == "" {
		return backend.TenantKey{}, status.Error(codes.DataLoss, "invalid key metadata")
	}
	return backend.TenantKey{ID: key.Id, TenantID: key.TenantId, ExpiresAt: key.ExpiresAtUnixSeconds}, nil
}
func (k *keyManager) Create(ctx context.Context, tenant string, expiry uint64) (backend.CreatedKey, error) {
	c, err := adminCaller(ctx)
	if err != nil {
		return backend.CreatedKey{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()
	r, err := k.client.CreateTenantKey(ctx, &pb.CreateTenantKeyRequest{Caller: c, TenantId: tenant, ExpiresAtUnixSeconds: expiry})
	if err != nil {
		return backend.CreatedKey{}, err
	}
	if r == nil || len(r.ApiKey) < 32 || r.GetKey().GetTenantId() != tenant {
		return backend.CreatedKey{}, status.Error(codes.DataLoss, "invalid key creation response")
	}
	value, err := keyValue(r.Key)
	return backend.CreatedKey{Key: value, APIKey: r.ApiKey}, err
}
func (k *keyManager) List(ctx context.Context, tenant, token string, size uint32) (backend.KeyPage, error) {
	c, err := adminCaller(ctx)
	if err != nil {
		return backend.KeyPage{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()
	r, err := k.client.ListTenantKeys(ctx, &pb.ListTenantKeysRequest{Caller: c, TenantId: tenant, PageToken: token, PageSize: size})
	if err != nil {
		return backend.KeyPage{}, err
	}
	if r == nil {
		return backend.KeyPage{}, status.Error(codes.DataLoss, "missing key page")
	}
	result := backend.KeyPage{Items: []backend.TenantKey{}, NextPageToken: r.NextPageToken}
	for _, key := range r.Keys {
		value, e := keyValue(key)
		if e != nil {
			return backend.KeyPage{}, e
		}
		if tenant != "" && value.TenantID != tenant {
			return backend.KeyPage{}, status.Error(codes.DataLoss, "key tenant mismatch")
		}
		result.Items = append(result.Items, value)
	}
	return result, nil
}
func (k *keyManager) Revoke(ctx context.Context, id string) error {
	c, err := adminCaller(ctx)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()
	_, err = k.client.RevokeTenantKey(ctx, &pb.RevokeTenantKeyRequest{Caller: c, Id: id})
	return err
}
