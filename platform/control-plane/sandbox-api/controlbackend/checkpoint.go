package controlbackend

import (
	"context"
	"fmt"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/common"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/core"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"sync"
	"time"
)

type checkpointOperation struct {
	mu         sync.Mutex
	assignment *pb.Assignment
	expected   uint64
	signal     int
	payload    string
	result     *pb.InstanceResult
	snapshot   *pb.ReusableSnapshot
}

func (b *Backend) checkpoint(r backend.LifecycleRequest) (backend.LifecycleResponse, error) {
	empty := backend.LifecycleResponse{}
	c, err := caller(r.Context)
	if err != nil {
		return empty, err
	}
	if r.RequestID == "" {
		return empty, status.Error(codes.InvalidArgument, "operation ID required")
	}
	v, err := b.owner(r.Context, r.InstanceID, false)
	if err != nil {
		return empty, err
	}
	var options core.SnapOptions
	if r.Signal == 18 {
		if proto.Unmarshal(r.Payload, &options) != nil || options.CheckpointTimeoutMs == 0 || options.CheckpointTimeoutMs > 3600000 {
			return empty, status.Error(codes.InvalidArgument, "invalid pause options")
		}
	}
	reusable := r.Signal == 18 && options.Type == common.SnapType_SNAPSHOT
	if r.Signal == 18 && ((reusable && (!options.LeaveRunning || options.Ttl != 0)) || (!reusable && options.Ttl <= 0)) {
		return empty, status.Error(codes.InvalidArgument, "invalid checkpoint mode")
	}
	key := c.TenantId + "\x00" + r.InstanceID + "\x00" + r.RequestID
	b.mu.Lock()
	op := b.checkpoints[key]
	if op == nil {
		if len(b.checkpoints) >= b.config.CacheEntries {
			b.mu.Unlock()
			return empty, status.Error(codes.ResourceExhausted, "pending operation budget exhausted")
		}
		expected := v.Record.Revision
		kind := pb.LifecycleKind_LIFECYCLE_KIND_PAUSE
		if reusable {
			kind = pb.LifecycleKind_LIFECYCLE_KIND_SNAPSHOT
		} else if r.Signal == 19 {
			kind = pb.LifecycleKind_LIFECYCLE_KIND_RESUME
		}
		if last := v.Record.LastOperation; last != nil && last.Id == r.RequestID && last.Kind == kind {
			expected = last.ExpectedRevision
		}
		op = &checkpointOperation{assignment: proto.Clone(v.Record.Assignment).(*pb.Assignment), expected: expected, signal: r.Signal, payload: string(r.Payload)}
		b.checkpoints[key] = op
	}
	b.mu.Unlock()
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.signal != r.Signal || op.payload != string(r.Payload) || !proto.Equal(op.assignment, v.Record.Assignment) {
		return empty, status.Error(codes.FailedPrecondition, "operation target or arguments changed")
	}
	forget := func() { b.mu.Lock(); delete(b.checkpoints, key); b.mu.Unlock() }
	// Captured assignment and expected revision are immutable across both internal
	// retries and a repeated HTTP request while its result remains uncertain.
	for attempt := 0; op.result == nil && attempt < 2; attempt++ {
		duration := b.config.RPCTimeout
		if r.Signal == 18 {
			duration = time.Duration(options.CheckpointTimeoutMs)*time.Millisecond + b.config.RPCTimeout
		}
		ctx, cancel := context.WithTimeout(r.Context, duration)
		node, e := b.dial(ctx, v.NodeAddress)
		var result *pb.InstanceResult
		if e == nil {
			if reusable {
				names := []string{}
				if options.Name != "" {
					names = append(names, options.Name)
				}
				var saved *pb.CreateSnapshotResponse
				saved, e = node.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{Assignment: op.assignment, Caller: c,
					OperationId: r.RequestID, ExpectedRevision: op.expected, Names: names,
					TimeoutSeconds: (options.CheckpointTimeoutMs + 999) / 1000})
				if e == nil && saved != nil {
					result = saved.Instance
					op.snapshot = saved.Snapshot
				}
			} else if r.Signal == 18 {
				result, e = node.PauseInstance(ctx, &pb.PauseInstanceRequest{Assignment: op.assignment, Caller: c, OperationId: r.RequestID, ExpectedRevision: op.expected, TtlSeconds: uint64(options.Ttl), TimeoutSeconds: (options.CheckpointTimeoutMs + 999) / 1000})
			} else {
				result, e = node.ResumeInstance(ctx, &pb.ResumeInstanceRequest{Assignment: op.assignment, Caller: c, OperationId: r.RequestID, ExpectedRevision: op.expected})
			}
		}
		cancel()
		if e == nil {
			wanted := pb.InstanceState_INSTANCE_STATE_PAUSED
			kind := pb.LifecycleKind_LIFECYCLE_KIND_PAUSE
			if reusable {
				wanted = pb.InstanceState_INSTANCE_STATE_RUNNING
				kind = pb.LifecycleKind_LIFECYCLE_KIND_SNAPSHOT
			} else if r.Signal == 19 {
				wanted = pb.InstanceState_INSTANCE_STATE_RUNNING
				kind = pb.LifecycleKind_LIFECYCLE_KIND_RESUME
			}
			if result != nil && result.Record != nil && result.Durability == pb.Durability_DURABILITY_JOURNALED {
				return empty, status.Error(codes.Unavailable, "operation completed locally; cluster publication is pending")
			}
			if result == nil || result.Record == nil || result.Durability != pb.Durability_DURABILITY_PUBLISHED || !proto.Equal(result.Record.Assignment, op.assignment) || result.Record.State != wanted || result.Record.LastOperation == nil || result.Record.LastOperation.Id != r.RequestID || result.Record.LastOperation.Kind != kind || result.Record.LastOperation.ExpectedRevision != op.expected {
				return empty, status.Error(codes.Unavailable, "operation result is not durably confirmed")
			}
			if e = authorize(c, result.Record); e != nil {
				return empty, e
			}
			op.result = result
			b.put(&pb.GetInstanceResponse{Record: result.Record, NodeAddress: v.NodeAddress, NodeProxyAddress: v.NodeProxyAddress})
			break
		}
		err = e
		if status.Code(e) != codes.Unavailable && status.Code(e) != codes.DeadlineExceeded {
			forget()
			return empty, e
		}
		b.forget(r.InstanceID)
		if attempt == 0 {
			fresh, e := b.owner(r.Context, r.InstanceID, true)
			if e != nil {
				return empty, err
			}
			if !proto.Equal(op.assignment, fresh.Record.Assignment) {
				forget()
				return empty, status.Error(codes.FailedPrecondition, "ownership changed")
			}
			v = fresh
		}
	}
	if op.result == nil {
		return empty, err
	}
	record := op.result.Record
	var payload []byte
	if reusable {
		value, e := snapshotValue(op.snapshot, c)
		if e != nil {
			return empty, e
		}
		if op.snapshot.State != pb.SnapshotState_SNAPSHOT_STATE_READY || !proto.Equal(op.snapshot.Template, record.Spec) {
			return empty, status.Error(codes.DataLoss, "invalid snapshot source")
		}
		payload, err = proto.Marshal(&core.SnapshotInfo{SnapshotID: value.SnapshotID, Names: value.Names})
	} else if r.Signal == 18 {
		cp := record.Checkpoint
		if cp == nil || cp.Artifact == nil || cp.Artifact.SizeBytes == 0 || cp.Artifact.SizeBytes > math.MaxInt64 || cp.ExpiresAtUnixSeconds == 0 {
			return empty, status.Error(codes.DataLoss, "invalid recovery point")
		}
		payload, err = proto.Marshal(&core.SnapInfo{SnapshotID: cp.Id, Size: int64(cp.Artifact.SizeBytes), ExpiresAtUnixSeconds: cp.ExpiresAtUnixSeconds})
	} else {
		if v.NodeProxyAddress == "" {
			return empty, status.Error(codes.DataLoss, "node proxy address missing")
		}
		payload, err = proto.Marshal(&core.SnapStartedInfo{InstanceID: record.Spec.Id, RouteAddress: v.NodeProxyAddress, FunctionProxyID: record.Assignment.NodeId, NodeID: record.Assignment.NodeId, PortMappings: "{}"})
	}
	if err != nil {
		return empty, fmt.Errorf("encode lifecycle response: %w", err)
	}
	forget()
	return backend.LifecycleResponse{Payload: payload}, nil
}
