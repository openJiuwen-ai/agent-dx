package controlbackend

import (
	"context"
	"errors"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

type masterFake struct {
	pb.MasterServiceClient
	reads int
	owner *pb.GetInstanceResponse
	err   error
}

func (m *masterFake) GetInstance(context.Context, *pb.GetInstanceRequest, ...grpc.CallOption) (*pb.GetInstanceResponse, error) {
	m.reads++
	return m.owner, m.err
}

type nodeFake struct {
	pb.NodeServiceClient
	calls       int
	assignments []*pb.Assignment
	fail        bool
}

func (n *nodeFake) DeleteInstance(_ context.Context, r *pb.DeleteInstanceRequest, _ ...grpc.CallOption) (*pb.InstanceResult, error) {
	n.calls++
	n.assignments = append(n.assignments, proto.Clone(r.Assignment).(*pb.Assignment))
	if n.fail {
		return nil, status.Error(codes.Unavailable, "reply lost")
	}
	return &pb.InstanceResult{Record: &pb.InstanceRecord{Spec: &pb.InstanceSpec{Id: "i", TenantId: "t"}, Assignment: r.Assignment, State: pb.InstanceState_INSTANCE_STATE_DELETED, Revision: 4}, Durability: pb.Durability_DURABILITY_PUBLISHED}, nil
}
func owner(generation uint64) *pb.GetInstanceResponse {
	return &pb.GetInstanceResponse{NodeAddress: "node:1234", Record: &pb.InstanceRecord{Spec: &pb.InstanceSpec{Id: "i", TenantId: "t"}, Assignment: &pb.Assignment{InstanceId: "i", NodeId: "n", Generation: generation}, State: pb.InstanceState_INSTANCE_STATE_RUNNING, Revision: 2}}
}
func request() backend.LifecycleRequest {
	return backend.LifecycleRequest{Context: backend.WithIdentity(context.Background(), backend.Identity{TenantID: "t", Role: backend.RoleTenant}), InstanceID: "i", Signal: 1, RequestID: "delete-a"}
}
func TestCacheHitDeletesWithoutMasterAndKeepsTenantBoundary(t *testing.T) {
	m := &masterFake{owner: owner(1)}
	n := &nodeFake{}
	b, err := New(m, func(context.Context, string) (pb.NodeServiceClient, error) { return n, nil }, Config{CacheTTL: time.Minute, CacheEntries: 10, RPCTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Read(request().Context, "i"); err != nil {
		t.Fatal(err)
	}
	m.err = errors.New("Master down")
	if _, err = b.Lifecycle(request()); err != nil {
		t.Fatal(err)
	}
	if m.reads != 1 || n.calls != 1 {
		t.Fatalf("reads=%d calls=%d", m.reads, n.calls)
	}
	r := request()
	r.Context = backend.WithIdentity(context.Background(), backend.Identity{TenantID: "other", Role: backend.RoleTenant})
	if _, err = b.Lifecycle(r); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross tenant: %v", err)
	}
	if n.calls != 1 {
		t.Fatal("unauthorized call reached Node")
	}
}
func TestAmbiguousDeleteNeverReplaysAgainstNewGeneration(t *testing.T) {
	m := &masterFake{owner: owner(1)}
	n := &nodeFake{fail: true}
	b, _ := New(m, func(context.Context, string) (pb.NodeServiceClient, error) { return n, nil }, Config{CacheTTL: time.Minute, CacheEntries: 10, RPCTimeout: time.Second})
	if _, err := b.Lifecycle(request()); err == nil {
		t.Fatal("expected failure")
	}
	m.owner = owner(2)
	if _, err := b.Lifecycle(request()); err == nil {
		t.Fatal("expected conflict")
	}
	for _, a := range n.assignments {
		if a.Generation != 1 {
			t.Fatal("replayed delete against new owner")
		}
	}
}
