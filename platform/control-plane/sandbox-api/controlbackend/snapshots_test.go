package controlbackend

import (
	"context"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
	"time"
)

type snapshotFake struct {
	pb.SnapshotServiceClient
	seen   *pb.CallerContext
	tenant string
	state  pb.SnapshotState
}

func (f *snapshotFake) GetSnapshot(ctx context.Context, r *pb.GetSnapshotRequest, _ ...grpc.CallOption) (*pb.ReusableSnapshot, error) {
	f.seen = r.Caller
	if _, ok := ctx.Deadline(); !ok {
		return nil, status.Error(codes.Internal, "RPC must be bounded")
	}
	return &pb.ReusableSnapshot{Id: r.Id, Template: &pb.InstanceSpec{TenantId: f.tenant}, State: f.state}, nil
}
func (f *snapshotFake) DeleteSnapshot(ctx context.Context, r *pb.DeleteSnapshotRequest, _ ...grpc.CallOption) (*pb.ReusableSnapshot, error) {
	return f.GetSnapshot(ctx, &pb.GetSnapshotRequest{Id: r.Id, Caller: r.Caller})
}
func TestSnapshotCatalogRequiresVerifiedIdentityAndChecksResponses(t *testing.T) {
	f := &snapshotFake{tenant: "tenant", state: pb.SnapshotState_SNAPSHOT_STATE_READY}
	catalog := NewSnapshotCatalog(f, time.Second)
	if _, err := catalog.Get(context.Background(), "s"); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing identity: %v", err)
	}
	ctx := backend.WithIdentity(context.Background(), backend.Identity{TenantID: "tenant", Role: backend.RoleTenant})
	value, err := catalog.Get(ctx, "s")
	if err != nil || value.SnapshotID != "s" || f.seen.TenantId != "tenant" {
		t.Fatalf("get: %v %v", value, err)
	}
	f.tenant = "other"
	if _, err := catalog.Get(ctx, "s"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("foreign response: %v", err)
	}
	f.tenant = "tenant"
	if err := catalog.Delete(ctx, "s"); status.Code(err) != codes.DataLoss {
		t.Fatalf("ready is not deletion: %v", err)
	}
	f.state = pb.SnapshotState_SNAPSHOT_STATE_DELETING
	if err := catalog.Delete(ctx, "s"); err != nil {
		t.Fatal(err)
	}
}
