// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Package backend defines the host-supplied dependencies of the Sandbox HTTP adapter.
// Payloads currently use the imported core protobuf contract. This boundary does
// not implement a runtime SDK or the future control-plane protocol.
package backend

import (
	"context"
	"errors"
	"sync"
)

var ErrUnavailable = errors.New("sandbox backend is not configured")
var ErrInstanceNotFound = errors.New("instance not found")

type Request struct {
	Context        context.Context
	Payload        []byte
	TraceParent    string
	TimeoutSeconds int
}
type LifecycleRequest struct {
	Context                                               context.Context
	InstanceID, TenantID, TraceID, TraceParent, RequestID string
	Signal                                                int
	Payload                                               []byte
	TimeoutSeconds                                        int
}
type LifecycleResponse struct {
	Code    int32
	Message string
	Payload []byte
}
type Transport interface {
	Create(Request) ([]byte, error)
	Invoke(Request) ([]byte, error)
	Lifecycle(LifecycleRequest) (LifecycleResponse, error)
}
type Instance struct {
	InstanceID, TenantID, State, Image string
	CPU, Memory                        uint64
}
type Instances interface {
	Read(context.Context, string) (*Instance, error)
	ConfirmDeleted(context.Context, string) (bool, error)
	IsRunning(instanceID string) bool
}
type Identity struct{ TenantID, Role string }

const RoleTenant = "tenant"
const RoleAdmin = "admin"

type Snapshot struct {
	SnapshotID string   `json:"snapshotId"`
	Names      []string `json:"names"`
}
type SnapshotPage struct {
	Items         []Snapshot `json:"items"`
	NextPageToken string     `json:"nextPageToken"`
}
type SnapshotCatalog interface {
	Get(context.Context, string) (Snapshot, error)
	List(context.Context, string, string, uint32) (SnapshotPage, error)
	Delete(context.Context, string) error
}
type Dependencies struct {
	Transport    Transport
	Instances    Instances
	Authenticate func(context.Context, string) (Identity, error)
	Snapshots    SnapshotCatalog
	Keys         KeyManager
}

func (d Dependencies) Validate() error {
	if d.Transport == nil || d.Instances == nil || d.Authenticate == nil {
		return ErrUnavailable
	}
	return nil
}

// Configure installs process-wide dependencies before serving. Configure only
// at startup; handlers use the same backend for the lifetime of the process.
func Configure(d Dependencies) (func(), error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	mu.Lock()
	previous := current
	current = d
	mu.Unlock()
	return func() { mu.Lock(); current = previous; mu.Unlock() }, nil
}

var mu sync.RWMutex
var current Dependencies

type contextKey struct{}

func WithDependencies(ctx context.Context, d Dependencies) context.Context {
	return context.WithValue(ctx, contextKey{}, d)
}
func Current(contexts ...context.Context) Dependencies {
	if len(contexts) > 0 && contexts[0] != nil {
		if d, ok := contexts[0].Value(contextKey{}).(Dependencies); ok {
			return d
		}
	}
	mu.RLock()
	defer mu.RUnlock()
	return current
}

type identityKey struct{}

func WithIdentity(ctx context.Context, identity Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, identity)
}
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	identity, ok := ctx.Value(identityKey{}).(Identity)
	return identity, ok
}
