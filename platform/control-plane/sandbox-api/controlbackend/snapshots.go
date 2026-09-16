package controlbackend

import (
	"context"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type snapshotCatalog struct {
	client  pb.SnapshotServiceClient
	timeout time.Duration
}

func NewSnapshotCatalog(client pb.SnapshotServiceClient, timeout time.Duration) backend.SnapshotCatalog {
	return &snapshotCatalog{client: client, timeout: timeout}
}
func snapshotValue(s *pb.ReusableSnapshot, c *pb.CallerContext) (backend.Snapshot, error) {
	if s == nil || s.Id == "" || s.Template == nil {
		return backend.Snapshot{}, status.Error(codes.DataLoss, "invalid snapshot response")
	}
	if !c.Administrator && s.Template.TenantId != c.TenantId {
		return backend.Snapshot{}, status.Error(codes.PermissionDenied, "snapshot belongs to another tenant")
	}
	return backend.Snapshot{SnapshotID: s.Id, Names: append([]string{}, s.Names...)}, nil
}
func (s *snapshotCatalog) Get(ctx context.Context, id string) (backend.Snapshot, error) {
	c, err := caller(ctx)
	if err != nil {
		return backend.Snapshot{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	value, err := s.client.GetSnapshot(ctx, &pb.GetSnapshotRequest{Id: id, Caller: c})
	if err != nil {
		return backend.Snapshot{}, err
	}
	if value.GetId() != id {
		return backend.Snapshot{}, status.Error(codes.DataLoss, "snapshot identity mismatch")
	}
	return snapshotValue(value, c)
}
func (s *snapshotCatalog) List(ctx context.Context, name, token string, size uint32) (backend.SnapshotPage, error) {
	result := backend.SnapshotPage{Items: []backend.Snapshot{}}
	c, err := caller(ctx)
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	page, err := s.client.ListSnapshots(ctx, &pb.ListSnapshotsRequest{Caller: c, Name: name, PageToken: token, PageSize: size})
	if err != nil {
		return result, err
	}
	if page == nil {
		return result, status.Error(codes.DataLoss, "missing snapshot page")
	}
	for _, item := range page.Snapshots {
		value, e := snapshotValue(item, c)
		if e != nil {
			return backend.SnapshotPage{}, e
		}
		result.Items = append(result.Items, value)
	}
	result.NextPageToken = page.NextPageToken
	return result, nil
}
func (s *snapshotCatalog) Delete(ctx context.Context, id string) error {
	c, err := caller(ctx)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	value, err := s.client.DeleteSnapshot(ctx, &pb.DeleteSnapshotRequest{Id: id, Caller: c})
	if err != nil {
		return err
	}
	if value.GetId() != id || (value.GetState() != pb.SnapshotState_SNAPSHOT_STATE_DELETING && value.GetState() != pb.SnapshotState_SNAPSHOT_STATE_DELETED) {
		return status.Error(codes.DataLoss, "snapshot deletion not accepted")
	}
	_, err = snapshotValue(value, c)
	return err
}
