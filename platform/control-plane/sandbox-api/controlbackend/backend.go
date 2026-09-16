// Package controlbackend adapts the public HTTP compatibility layer to Instance RPCs.
package controlbackend

import (
	"container/list"
	"context"
	"strings"
	"sync"
	"time"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	CacheTTL     time.Duration
	CacheEntries int
	RPCTimeout   time.Duration
}
type NodeDialer func(context.Context, string) (pb.NodeServiceClient, error)
type entry struct {
	owner   *pb.GetInstanceResponse
	expires time.Time
	element *list.Element
}
type operation struct {
	assignment *pb.Assignment
	result     *pb.InstanceResult
	mu         sync.Mutex
}
type Backend struct {
	master pb.MasterServiceClient
	dial   NodeDialer
	config Config
	mu     sync.Mutex
	cache  map[string]*entry
	lru    *list.List
	// Only in-flight deletes are retained here. Their target cannot change after an ambiguous reply.
	deletes     map[string]*operation
	checkpoints map[string]*checkpointOperation
}

func New(master pb.MasterServiceClient, dial NodeDialer, config Config) (*Backend, error) {
	if master == nil || dial == nil || config.CacheTTL <= 0 || config.CacheEntries <= 0 || config.RPCTimeout <= 0 {
		return nil, status.Error(codes.InvalidArgument, "backend clients and positive cache/RPC bounds required")
	}
	return &Backend{master: master, dial: dial, config: config, cache: map[string]*entry{}, lru: list.New(), deletes: map[string]*operation{}, checkpoints: map[string]*checkpointOperation{}}, nil
}
func caller(ctx context.Context) (*pb.CallerContext, error) {
	i, ok := backend.IdentityFromContext(ctx)
	if !ok || i.TenantID == "" || (i.Role != backend.RoleAdmin && i.Role != backend.RoleTenant) {
		return nil, status.Error(codes.Unauthenticated, "verified identity required")
	}
	return &pb.CallerContext{TenantId: i.TenantID, Administrator: i.Role == backend.RoleAdmin}, nil
}
func authorize(c *pb.CallerContext, r *pb.InstanceRecord) error {
	if r == nil || r.Spec == nil || r.Assignment == nil || r.Spec.Id != r.Assignment.InstanceId || r.Assignment.Generation == 0 {
		return status.Error(codes.DataLoss, "invalid ownership record")
	}
	if !c.Administrator && r.Spec.TenantId != c.TenantId {
		return status.Error(codes.PermissionDenied, "instance belongs to another tenant")
	}
	return nil
}
func (b *Backend) forget(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e := b.cache[id]; e != nil {
		b.lru.Remove(e.element)
		delete(b.cache, id)
	}
}
func (b *Backend) put(v *pb.GetInstanceResponse) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := v.Record.Spec.Id
	if old := b.cache[id]; old != nil {
		a, z := old.owner.Record, v.Record
		if a.Assignment.Generation > z.Assignment.Generation || (a.Assignment.Generation == z.Assignment.Generation && a.Revision > z.Revision) {
			return
		}
		b.lru.Remove(old.element)
	}
	b.cache[id] = &entry{owner: proto.Clone(v).(*pb.GetInstanceResponse), expires: time.Now().Add(b.config.CacheTTL), element: b.lru.PushFront(id)}
	for len(b.cache) > b.config.CacheEntries {
		last := b.lru.Back()
		delete(b.cache, last.Value.(string))
		b.lru.Remove(last)
	}
}
func (b *Backend) owner(ctx context.Context, id string, refresh bool) (*pb.GetInstanceResponse, error) {
	c, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	if !refresh {
		b.mu.Lock()
		e := b.cache[id]
		var v *pb.GetInstanceResponse
		if e != nil && time.Now().Before(e.expires) {
			v = proto.Clone(e.owner).(*pb.GetInstanceResponse)
			b.lru.MoveToFront(e.element)
		}
		b.mu.Unlock()
		if v != nil {
			return v, authorize(c, v.Record)
		}
	}
	rpcCtx, cancel := context.WithTimeout(ctx, b.config.RPCTimeout)
	defer cancel()
	v, err := b.master.GetInstance(rpcCtx, &pb.GetInstanceRequest{InstanceId: id, Caller: c})
	if err != nil {
		return nil, err
	}
	if v == nil || v.Record == nil || v.Record.Spec == nil || v.Record.Spec.Id != id || v.NodeAddress == "" {
		return nil, status.Error(codes.DataLoss, "incomplete owner response")
	}
	if err = authorize(c, v.Record); err != nil {
		return nil, err
	}
	b.put(v)
	return proto.Clone(v).(*pb.GetInstanceResponse), nil
}
func (b *Backend) Read(ctx context.Context, id string) (*backend.Instance, error) {
	v, err := b.owner(ctx, id, false)
	if status.Code(err) == codes.NotFound {
		return nil, backend.ErrInstanceNotFound
	}
	if err != nil {
		return nil, err
	}
	spec := v.Record.Spec
	state := strings.ToLower(strings.TrimPrefix(v.Record.State.String(), "INSTANCE_STATE_"))
	return &backend.Instance{InstanceID: id, TenantID: spec.TenantId, State: state, Image: spec.Image, CPU: spec.GetResources().GetCpuMillis(), Memory: spec.GetResources().GetMemoryBytes() / 1048576}, nil
}
func (b *Backend) ConfirmDeleted(ctx context.Context, id string) (bool, error) {
	v, err := b.owner(ctx, id, true)
	if err != nil {
		return false, err
	}
	return v.Record.State == pb.InstanceState_INSTANCE_STATE_DELETED, nil
}
func (b *Backend) IsRunning(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	v := b.cache[id]
	return v != nil && time.Now().Before(v.expires) && v.owner.Record.State == pb.InstanceState_INSTANCE_STATE_RUNNING
}
func (b *Backend) Invoke(backend.Request) ([]byte, error) {
	return nil, status.Error(codes.Unimplemented, "use the RRT HTTP data endpoint")
}
func (b *Backend) Lifecycle(r backend.LifecycleRequest) (backend.LifecycleResponse, error) {
	if r.Signal == 18 || r.Signal == 19 {
		return b.checkpoint(r)
	}
	if r.Signal != httpx.KillSignalVal {
		return backend.LifecycleResponse{}, status.Error(codes.Unimplemented, "lifecycle operation is not connected yet")
	}
	c, err := caller(r.Context)
	if err != nil {
		return backend.LifecycleResponse{}, err
	}
	v, err := b.owner(r.Context, r.InstanceID, false)
	if err != nil {
		return backend.LifecycleResponse{}, err
	}
	key := c.TenantId + "\x00" + r.InstanceID + "\x00" + r.RequestID
	b.mu.Lock()
	op := b.deletes[key]
	if op == nil {
		if len(b.deletes) >= b.config.CacheEntries {
			b.mu.Unlock()
			return backend.LifecycleResponse{}, status.Error(codes.ResourceExhausted, "pending delete budget exhausted")
		}
		op = &operation{assignment: proto.Clone(v.Record.Assignment).(*pb.Assignment)}
		b.deletes[key] = op
	}
	b.mu.Unlock()
	op.mu.Lock()
	defer op.mu.Unlock()
	if !proto.Equal(op.assignment, v.Record.Assignment) {
		return backend.LifecycleResponse{}, status.Error(codes.FailedPrecondition, "ownership changed; previous delete target is not replayable")
	}
	if op.result != nil {
		return backend.LifecycleResponse{}, nil
	}
	// Retry once against the same assignment. Refresh may change the address, never the target generation.
	for attempt := 0; attempt < 2; attempt++ {
		rpcCtx, cancel := context.WithTimeout(r.Context, b.config.RPCTimeout)
		node, e := b.dial(rpcCtx, v.NodeAddress)
		var result *pb.InstanceResult
		if e == nil {
			result, e = node.DeleteInstance(rpcCtx, &pb.DeleteInstanceRequest{Assignment: op.assignment, Caller: c})
		}
		cancel()
		if e == nil {
			if result == nil || result.Record == nil || !proto.Equal(result.Record.Assignment, op.assignment) || result.Record.State != pb.InstanceState_INSTANCE_STATE_DELETED || result.Record.ResourcesHeld || result.Durability != pb.Durability_DURABILITY_PUBLISHED {
				return backend.LifecycleResponse{}, status.Error(codes.Unavailable, "delete result is not durably confirmed")
			}
			if err = authorize(c, result.Record); err != nil {
				return backend.LifecycleResponse{}, err
			}
			b.put(&pb.GetInstanceResponse{Record: result.Record, NodeAddress: v.NodeAddress})
			op.result = result
			b.mu.Lock()
			delete(b.deletes, key)
			b.mu.Unlock()
			return backend.LifecycleResponse{}, nil
		}
		err = e
		if status.Code(e) != codes.Unavailable && status.Code(e) != codes.DeadlineExceeded && status.Code(e) != codes.FailedPrecondition && status.Code(e) != codes.NotFound {
			return backend.LifecycleResponse{}, e
		}
		b.forget(r.InstanceID)
		if attempt == 1 {
			break
		}
		fresh, e := b.owner(r.Context, r.InstanceID, true)
		if e != nil {
			return backend.LifecycleResponse{}, err
		}
		if !proto.Equal(fresh.Record.Assignment, op.assignment) {
			return backend.LifecycleResponse{}, status.Error(codes.FailedPrecondition, "ownership changed; previous delete target is not replayable")
		}
		v = fresh
	}
	return backend.LifecycleResponse{}, err
}
