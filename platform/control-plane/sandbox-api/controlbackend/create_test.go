package controlbackend

import (
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/core"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestCreateTranslationPreservesResourcesEnvironmentAndWholeCards(t *testing.T) {
	raw := &core.CreateRequest{DesignatedInstanceID: "i", SchedulingOps: &core.SchedulingOptions{Resources: map[string]float64{"CPU": 1500, "Memory": 2048, "storage": 1048576, "NPU/Ascend\\+A/count": 2}, Extension: map[string]string{"rootfs": `{"type":"image","runtime":"runc","imageurl":"image:tag"}`}}, CreateOptions: map[string]string{"tenantId": "spoofed", "DELEGATE_ENV_VAR": `{"USER_VALUE":"a"}`}}
	spec, err := createSpec(raw, &pb.CallerContext{TenantId: "actual"})
	if err != nil {
		t.Fatal(err)
	}
	if spec.TenantId != "actual" || spec.Resources.CpuMillis != 1500 || spec.Resources.MemoryBytes != 2*1024*1024*1024 || spec.Resources.DiskBytes != 1048576 || spec.Env["USER_VALUE"] != "a" || spec.Scheduling.Devices[0].GetModel() != "Ascend+A" || spec.Scheduling.Devices[0].Count != 2 {
		t.Fatalf("lost request fields: %v", spec)
	}
	raw.SchedulingOps.Extension["CPU_LIMIT"] = "2000"
	if _, err = createSpec(raw, &pb.CallerContext{TenantId: "actual"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("independent limit silently ignored: %v", err)
	}
	delete(raw.SchedulingOps.Extension, "CPU_LIMIT")
	raw.SnapshotID = "snapshot"
	if _, err = createSpec(raw, &pb.CallerContext{TenantId: "actual"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("snapshot silently ignored: %v", err)
	}
}
