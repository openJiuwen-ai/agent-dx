package backend

import "context"

type TenantKey struct {
	ID        string `json:"id"`
	TenantID  string `json:"tenantId"`
	ExpiresAt uint64 `json:"expiresAtUnixSeconds"`
}
type CreatedKey struct {
	Key    TenantKey `json:"key"`
	APIKey string    `json:"apiKey"`
}
type KeyPage struct {
	Items         []TenantKey `json:"items"`
	NextPageToken string      `json:"nextPageToken"`
}
type KeyManager interface {
	Create(context.Context, string, uint64) (CreatedKey, error)
	List(context.Context, string, string, uint32) (KeyPage, error)
	Revoke(context.Context, string) error
}
