package controlbackend

import (
	"context"
	"encoding/json"
	"fmt"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/core"
	oldruntime "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/runtime"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"sort"
	"strconv"
	"strings"
)

func resource(v float64, scale float64) (uint64, error) {
	n := v * scale
	if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > 1<<53 || math.Trunc(n) != n {
		return 0, status.Error(codes.InvalidArgument, "invalid resource amount")
	}
	return uint64(n), nil
}

// Legacy protobuf is decoded only inside the Go compatibility adapter. The wire
// call to Master contains an InstanceSpec and never a function or POSIX request.
func createSpec(raw *core.CreateRequest, c *pb.CallerContext) (*pb.InstanceSpec, error) {
	if raw.DesignatedInstanceID == "" || raw.SchedulingOps == nil {
		return nil, status.Error(codes.InvalidArgument, "instance ID and resources required")
	}
	if raw.SnapshotID != "" || raw.Failover {
		return nil, status.Error(codes.Unimplemented, "snapshot creation is not connected yet")
	}
	o := raw.SchedulingOps
	e := o.Extension
	for _, key := range []string{"mounts", "extra_config", "inherit_entrypoint", "network_policy", "idle_timeout", "data_plane_tunnel_security_mode", "data_plane_port_forward_security_mode"} {
		if e[key] != "" {
			return nil, status.Errorf(codes.Unimplemented, "%s is not connected yet", key)
		}
	}
	cpu, err := resource(o.Resources["CPU"], 1)
	if err != nil {
		return nil, err
	}
	memory, err := resource(o.Resources["Memory"], 1048576)
	if err != nil {
		return nil, err
	}
	disk, err := resource(o.Resources["storage"], 1)
	if err != nil {
		return nil, err
	}
	for key, wanted := range map[string]uint64{"CPU_LIMIT": cpu, "MEMORY_LIMIT": memory / 1048576, "Memory_LIMIT": memory / 1048576, "STORAGE_LIMIT": disk} {
		if value := e[key]; value != "" && value != strconv.FormatUint(wanted, 10) {
			return nil, status.Errorf(codes.Unimplemented, "independent %s is not connected yet", key)
		}
	}
	if network := raw.CreateOptions["network"]; network != "" {
		var ports struct {
			Forwardings []struct {
				Port      int    `json:"port"`
				RouteKind string `json:"routeKind"`
			} `json:"portForwardings"`
		}
		if json.Unmarshal([]byte(network), &ports) != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid port bindings")
		}
		for _, port := range ports.Forwardings {
			if port.RouteKind == "public" {
				return nil, status.Error(codes.Unimplemented, "public port publication is not connected yet")
			}
		}
	}
	if cpu == 0 || memory == 0 {
		return nil, status.Error(codes.InvalidArgument, "CPU and memory must be positive")
	}
	image := strings.TrimSpace(e["rootfs"])
	runtime := "runsc"
	if strings.HasPrefix(image, "{") {
		var root struct {
			Type    string `json:"type"`
			Runtime string `json:"runtime"`
			Image   string `json:"imageurl"`
		}
		if err = json.Unmarshal([]byte(image), &root); err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid rootfs")
		}
		if root.Type != "image" {
			return nil, status.Error(codes.Unimplemented, "only image rootfs is connected")
		}
		image = root.Image
		if root.Runtime != "" {
			runtime = root.Runtime
		}
	}
	if image == "" {
		return nil, status.Error(codes.InvalidArgument, "image required")
	}
	p := &pb.SchedulingPolicy{Labels: map[string]string{}}
	for key, v := range o.Resources {
		if key == "CPU" || key == "Memory" || key == "storage" {
			continue
		}
		parts := strings.Split(key, "/")
		if len(parts) != 3 || parts[2] != "count" {
			return nil, status.Errorf(codes.Unimplemented, "resource %s is not supported", key)
		}
		kind := pb.DeviceKind_DEVICE_KIND_UNSPECIFIED
		if parts[0] == "GPU" {
			kind = pb.DeviceKind_DEVICE_KIND_GPU
		}
		if parts[0] == "NPU" {
			kind = pb.DeviceKind_DEVICE_KIND_NPU
		}
		count, err := resource(v, 1)
		if err != nil || count == 0 || count > math.MaxUint32 || kind == 0 {
			return nil, status.Error(codes.InvalidArgument, "invalid whole-card request")
		}
		model := strings.ReplaceAll(parts[1], `\`, "")
		var modelPtr *string
		if parts[1] != ".+" {
			modelPtr = &model
		}
		p.Devices = append(p.Devices, &pb.DeviceRequest{Kind: kind, Model: modelPtr, Count: uint32(count)})
	}
	sort.Slice(p.Devices, func(i, j int) bool {
		a, b := p.Devices[i], p.Devices[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.GetModel() < b.GetModel()
	})
	for _, label := range raw.Labels {
		key, value, ok := strings.Cut(label, ":")
		if !ok {
			key, value, ok = strings.Cut(label, "=")
		}
		if !ok {
			key, value = label, ""
		}
		p.Labels[key] = value
	}
	if o.ScheduleAffinity != nil {
		return nil, status.Error(codes.Unimplemented, "HTTP affinity translation is not connected yet")
	}
	env := map[string]string{}
	if text := raw.CreateOptions[httpx.DelegateEnvVar]; text != "" {
		if err = json.Unmarshal([]byte(text), &env); err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid environment")
		}
	}
	for key := range env {
		if strings.HasPrefix(key, "ADX_") || key == "RRT_HTTP_TOKEN" {
			return nil, status.Errorf(codes.InvalidArgument, "environment %s is reserved", key)
		}
	}
	return &pb.InstanceSpec{Id: raw.DesignatedInstanceID, TenantId: c.TenantId, Image: image, Runtime: runtime, Resources: &pb.Resources{CpuMillis: cpu, MemoryBytes: memory, DiskBytes: disk}, Priority: o.Priority, Scheduling: p, Env: env}, nil
}
func (b *Backend) Create(r backend.Request) ([]byte, error) {
	c, err := caller(r.Context)
	if err != nil {
		return nil, err
	}
	var raw core.CreateRequest
	if err = proto.Unmarshal(r.Payload, &raw); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid compatibility payload")
	}
	spec, err := createSpec(&raw, c)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(r.Context, b.config.RPCTimeout)
	defer cancel()
	result, err := b.master.CreateInstance(ctx, &pb.CreateInstanceRequest{Spec: spec, Caller: c})
	if err != nil {
		return nil, err
	}
	if result == nil || result.Record == nil || !proto.Equal(result.Record.Spec, spec) || result.Record.State != pb.InstanceState_INSTANCE_STATE_RUNNING || result.Durability != pb.Durability_DURABILITY_PUBLISHED {
		return nil, status.Error(codes.Unavailable, "create result is not durably confirmed")
	}
	// The first lifecycle request resolves the address once; subsequent requests use the cache.
	return proto.Marshal(&oldruntime.NotifyRequest{Message: fmt.Sprintf("instance %s running", spec.Id)})
}
