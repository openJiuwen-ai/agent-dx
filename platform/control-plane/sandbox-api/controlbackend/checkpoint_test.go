package controlbackend

import (
	"context"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

type checkpointNode struct {
	pb.NodeServiceClient
	calls     int
	revisions []uint64
	m         *masterFake
}

func (n *checkpointNode) PauseInstance(_ context.Context, r *pb.PauseInstanceRequest, _ ...grpc.CallOption) (*pb.InstanceResult, error) {
	n.calls++
	n.revisions = append(n.revisions, r.ExpectedRevision)
	if r.OperationId != "pause-a" || r.TtlSeconds != 600 || r.TimeoutSeconds != 60 {
		panic("lost public options")
	}
	record := proto.Clone(n.m.owner.Record).(*pb.InstanceRecord)
	record.State = pb.InstanceState_INSTANCE_STATE_PAUSED
	record.Revision = 4
	record.ResourcesHeld = false
	record.RuntimeIp = ""
	record.Checkpoint = &pb.RestorePoint{Id: r.OperationId, ExpiresAtUnixSeconds: 123456, SourceRuntimeId: "i-1", Artifact: &pb.CheckpointArtifact{Storage: "local", Location: "/cp/a", SizeBytes: 123}}
	record.LastOperation = &pb.CompletedOperation{Id: r.OperationId, Kind: pb.LifecycleKind_LIFECYCLE_KIND_PAUSE, ExpectedRevision: 2}
	n.m.owner.Record = record
	if n.calls == 1 {
		return nil, status.Error(codes.Unavailable, "committed but reply lost")
	}
	if r.ExpectedRevision != 2 {
		return nil, status.Error(codes.FailedPrecondition, "retry changed revision")
	}
	return &pb.InstanceResult{Record: record, Durability: pb.Durability_DURABILITY_PUBLISHED}, nil
}
func (n *checkpointNode) ResumeInstance(_ context.Context, r *pb.ResumeInstanceRequest, _ ...grpc.CallOption) (*pb.InstanceResult, error) {
	record := proto.Clone(n.m.owner.Record).(*pb.InstanceRecord)
	if r.ExpectedRevision != 4 {
		panic("resume revision")
	}
	record.State = pb.InstanceState_INSTANCE_STATE_RUNNING
	record.Revision = 6
	record.ResourcesHeld = true
	record.RuntimeIp = "10.0.0.3"
	record.RuntimeId = "i-1-r5"
	record.LastOperation = &pb.CompletedOperation{Id: r.OperationId, Kind: pb.LifecycleKind_LIFECYCLE_KIND_RESUME, ExpectedRevision: 4}
	n.m.owner.Record = record
	return &pb.InstanceResult{Record: record, Durability: pb.Durability_DURABILITY_PUBLISHED}, nil
}
func TestCheckpointRetryKeepsOriginalRevisionAndUsesOwnerCache(t *testing.T) {
	m := &masterFake{owner: owner(1)}
	n := &checkpointNode{m: m}
	b, _ := New(m, func(context.Context, string) (pb.NodeServiceClient, error) { return n, nil }, Config{CacheTTL: time.Minute, CacheEntries: 10, RPCTimeout: time.Second})
	r := request()
	r.Signal = 18
	r.RequestID = "pause-a"
	r.TimeoutSeconds = 60
	r.Payload, _ = proto.Marshal(&core.SnapOptions{Ttl: 600, CheckpointTimeoutMs: 60000})
	response, err := b.Lifecycle(r)
	if err != nil {
		t.Fatal(err)
	}
	var cp core.SnapInfo
	if proto.Unmarshal(response.Payload, &cp) != nil || cp.SnapshotID != "pause-a" || cp.Size != 123 {
		t.Fatalf("bad pause payload %v", &cp)
	}
	if n.calls != 2 || n.revisions[0] != 2 || n.revisions[1] != 2 {
		t.Fatal(n.revisions)
	}
	reads := m.reads
	r.Signal = 19
	r.RequestID = "resume-b"
	r.Payload = nil
	response, err = b.Lifecycle(r)
	if err != nil {
		t.Fatal(err)
	}
	var resumed core.SnapStartedInfo
	if proto.Unmarshal(response.Payload, &resumed) != nil || resumed.InstanceID != "i" || resumed.RouteAddress == "" {
		t.Fatalf("bad resume %v", &resumed)
	}
	if m.reads != reads {
		t.Fatal("cache-hit resume queried Master")
	}
	r.Context = backend.WithIdentity(r.Context, backend.Identity{TenantID: "foreign", Role: backend.RoleTenant})
	if _, err = b.Lifecycle(r); status.Code(err) != codes.PermissionDenied {
		t.Fatal("tenant boundary", err)
	}
}

type snapshotNode struct {
	pb.NodeServiceClient
	m     *masterFake
	calls int
}

func (n *snapshotNode) CreateSnapshot(_ context.Context, r *pb.CreateSnapshotRequest, _ ...grpc.CallOption) (*pb.CreateSnapshotResponse, error) {
	n.calls++
	if r.ExpectedRevision != 2 || r.TimeoutSeconds != 60 || len(r.Names) != 1 || r.Names[0] != "base" {
		panic("snapshot arguments changed")
	}
	record := proto.Clone(n.m.owner.Record).(*pb.InstanceRecord)
	record.Revision = 7
	record.LastOperation = &pb.CompletedOperation{Id: r.OperationId, ExpectedRevision: 2, Kind: pb.LifecycleKind_LIFECYCLE_KIND_SNAPSHOT}
	n.m.owner.Record = record
	if n.calls == 1 {
		return nil, status.Error(codes.Unavailable, "reply lost")
	}
	return &pb.CreateSnapshotResponse{
		Snapshot: &pb.ReusableSnapshot{Id: "snapshot-a", Names: r.Names, Template: record.Spec, State: pb.SnapshotState_SNAPSHOT_STATE_READY},
		Instance: &pb.InstanceResult{Record: record, Durability: pb.Durability_DURABILITY_PUBLISHED},
	}, nil
}
func TestReusableSnapshotUsesNodeRPCAndPreservesRetryRevision(t *testing.T) {
	m := &masterFake{owner: owner(1)}
	n := &snapshotNode{m: m}
	b, _ := New(m, func(context.Context, string) (pb.NodeServiceClient, error) { return n, nil }, Config{CacheTTL: time.Minute, CacheEntries: 10, RPCTimeout: time.Second})
	r := request()
	r.Signal = 18
	r.RequestID = "snapshot-op"
	r.Payload, _ = proto.Marshal(&core.SnapOptions{Type: 1, LeaveRunning: true, Name: "base", CheckpointTimeoutMs: 60000})
	result, err := b.Lifecycle(r)
	if err != nil {
		t.Fatal(err)
	}
	var saved core.SnapshotInfo
	if proto.Unmarshal(result.Payload, &saved) != nil || saved.SnapshotID != "snapshot-a" || len(saved.Names) != 1 {
		t.Fatalf("bad snapshot response %v", &saved)
	}
	if n.calls != 2 {
		t.Fatalf("calls=%d", n.calls)
	}
	if m.owner.Record.State != pb.InstanceState_INSTANCE_STATE_RUNNING {
		t.Fatal("source not running")
	}
}
