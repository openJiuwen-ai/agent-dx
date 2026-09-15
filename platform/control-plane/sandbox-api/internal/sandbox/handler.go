/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package sandbox provides HTTP handlers for sandbox lifecycle management.
package sandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/netip"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ugorji/go/codec"
	"golang.org/x/net/idna"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/affinity"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/common"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/core"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/runtime"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/httpx"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/instancecache"
)

const (
	// Sandbox v1 always executes through the dedicated Rust slot (rrt).
	// The public runtime field selects only the sandbox isolation runtime.
	defaultSandboxRuntime            = "rrt"
	defaultSandboxFunctionID         = "default/0-defaultservice-rrt/$latest"
	sandboxDefaultScheduleSeconds    = 30
	sandboxScheduleBufferSeconds     = 30
	sandboxDefaultInitTimeoutSeconds = 30
	sandboxCreateTimeoutSeconds      = sandboxDefaultScheduleSeconds + sandboxDefaultInitTimeoutSeconds +
		sandboxScheduleBufferSeconds
	sandboxInvokeTimeoutSeconds     = 60
	sandboxDefaultCPU               = 1000
	sandboxDefaultMemory            = 2048
	sandboxGracefulShutdownSeconds  = 5
	sandboxDirectoryQuotaMB         = 512
	sandboxInstanceType             = "reserved"
	sandboxDelegateDirectory        = "/tmp"
	sandboxConcurrency              = "1"
	sandboxModuleName               = "yr.sandbox.sandbox"
	sandboxClassName                = "SandboxInstance"
	sandboxTemporarySchedulerNote   = "-temporary"
	sandboxKillInstanceSignal       = httpx.KillSignalVal
	sandboxPauseInstanceSignal      = 18
	sandboxResumeInstanceSignal     = 19
	sandboxReloadInstanceSignal     = 25
	sandboxUpdateNetworkSignal      = 26
	sandboxPauseDefaultTTLSeconds   = 90_000
	sandboxLifecycleRequestIDHeader = "X-ADX-Request-ID"
	sandboxRunningPollTimeout       = 5 * time.Second
	sandboxRunningPollInterval      = 200 * time.Millisecond
	sandboxDefaultRRTHTTPPort       = 50090
	sandboxDefaultTunnelWSPort      = 8765
	sandboxDefaultTunnelHTTPPort    = 8766
	inlineValueLengthOffset         = 8
	maxSandboxPort                  = 65535
	portForwardingFormatParts       = 2
	createTimeoutSuccessCode        = 3002
	sandboxInstanceDuplicatedCode   = 1004
	millisecondsPerSecond           = 1000
	sandboxCreateHeartbeatInterval  = 2 * time.Second
	sandboxCreateStatusCreating     = "creating"
	sandboxCreateStatusRunning      = "running"
	sandboxCreateStatusTimeout      = "timeout"
	sandboxCreateStatusFailed       = "failed"
	sandboxCreateReplayTTL          = 10 * time.Minute
	sandboxCreateReplayMaxEntries   = 10000
	sandboxCreateReplayCleanupEvery = time.Second
	sandboxCreateReplayCleanupBatch = 64
	sandboxCreateReplayCompactAt    = 1024
	sandboxCreateReplayCompactRatio = 2
	sandboxCreateRequestBodyLimit   = 1 << 20
	sandboxRawRequestIDLength       = 18
	sandboxRawRequestSequence       = "00"
	sandboxXPUFieldCount            = 3
	sandboxStorageResourceName      = "storage"
	sandboxStorageLimitExtension    = "STORAGE_LIMIT"
	bytesPerMiB                     = 1024 * 1024
	decimalRadix                    = 10
	maxSandboxTrafficRules          = 256
	defaultSandboxRulePriority      = 100
	maxSandboxUserRulePriority      = math.MaxUint32 - 1
)

var waitForSandboxInstanceRunning = func(ctx context.Context, instanceID, functionID, resourceSpecNote string) bool {
	deadline := time.Now().Add(sandboxRunningPollTimeout)
	for time.Now().Before(deadline) {
		if isSandboxInstanceRunning(ctx, instanceID, functionID, resourceSpecNote) {
			return true
		}
		time.Sleep(sandboxRunningPollInterval)
	}
	return isSandboxInstanceRunning(ctx, instanceID, functionID, resourceSpecNote)
}

var readAuthoritativeSandboxInstance = func(ctx context.Context, id string) (*backend.Instance, error) {
	d := backend.Current(ctx)
	if d.Instances == nil {
		return nil, backend.ErrUnavailable
	}
	return d.Instances.Read(ctx, id)
}

var confirmSandboxInstanceDeleted = func(ctx context.Context, id string) (bool, error) {
	d := backend.Current(ctx)
	if d.Instances == nil {
		return false, backend.ErrUnavailable
	}
	return d.Instances.ConfirmDeleted(ctx, id)
}

var (
	sandboxXPUTypePattern           = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
	sandboxXPUCountPattern          = regexp.MustCompile(`^[0-9]+$`)
	sandboxPauseRequestIDPattern    = regexp.MustCompile(`^pause-[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	sandboxResumeRequestIDPattern   = regexp.MustCompile(`^resume-[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	sandboxNetworkRequestIDPattern  = regexp.MustCompile(`^network-[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	sandboxReloadRequestIDPattern   = regexp.MustCompile(`^reload-[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	sandboxSnapshotRequestIDPattern = regexp.MustCompile(`^snapshot-[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	sandboxDNSLabelPattern          = regexp.MustCompile(`^[a-z0-9_-]+$`)
)

type pauseV1Request struct {
	TTLSeconds     int32 `json:"ttlSeconds"`
	TimeoutSeconds int   `json:"timeoutSeconds"`
}

type pauseV1Response struct {
	SandboxID  string `json:"sandboxId"`
	SnapshotID string `json:"snapshotId"`
	Size       int64  `json:"size"`
	State      string `json:"state"`
	ExpiresAt  int64  `json:"expiresAt"`
}

type resumeV1Response struct {
	SandboxID       string         `json:"sandboxId"`
	State           string         `json:"state"`
	RouteAddress    string         `json:"routeAddress"`
	FunctionProxyID string         `json:"functionProxyId"`
	NodeID          string         `json:"nodeId"`
	PortMappings    map[string]int `json:"portMappings"`
}

type reloadV1Response struct {
	Success bool `json:"success"`
}

type updateNetworkV1Response struct {
	Success bool `json:"success"`
}

type sandboxLifecycleTransportError struct {
	cause error
}

func (e *sandboxLifecycleTransportError) Error() string { return e.cause.Error() }
func (e *sandboxLifecycleTransportError) Unwrap() error { return e.cause }

type sandboxLifecycleHTTPError struct {
	status int
	cause  error
}

func (e *sandboxLifecycleHTTPError) Error() string { return e.cause.Error() }
func (e *sandboxLifecycleHTTPError) Unwrap() error { return e.cause }

type sandboxLifecycleBusinessError struct {
	operation string
	code      common.ErrorCode
	message   string
}

func (e *sandboxLifecycleBusinessError) Error() string {
	return fmt.Sprintf("%s failed: code=%d message=%s", e.operation, e.code, e.message)
}

type reusableSnapshotInfo struct {
	SnapshotID string   `json:"snapshotId"`
	Names      []string `json:"names"`
}

// CreateRequest holds the parameters for sandbox creation.
type CreateRequest struct {
	Name      string   `json:"name"`
	Namespace string   `json:"namespace"`
	Tenant    string   `json:"tenant"`
	Runtime   string   `json:"runtime"`
	Rootfs    string   `json:"rootfs"`
	Image     string   `json:"image"`
	Ports     []string `json:"ports"`
	// portRouteKinds is populated only by the frontend for runtime-owned ports.
	// It is deliberately not part of the public SDK request contract.
	portRouteKinds map[int]string
	// Resource and runtime extras (honored by v1 create; 0/nil = use default).
	Cpu               int                      `json:"cpu"`
	Memory            int                      `json:"memory"`
	CpuLimit          int                      `json:"cpu_limit"`
	MemLimit          int                      `json:"mem_limit"`
	Env               map[string]string        `json:"env"`
	Mounts            []map[string]interface{} `json:"mounts"`
	ExtraConfig       map[string]interface{}   `json:"extra_config"`
	XPU               string                   `json:"xpu"`
	StorageMb         *int64                   `json:"storageMb,omitempty"`
	StorageLimitMb    int64                    `json:"storage_limit_mb"`
	Network           *SandboxNetworkPolicy    `json:"network,omitempty"`
	SnapshotID        string                   `json:"snapshotId,omitempty"`
	Failover          bool                     `json:"failover"`
	InheritEntrypoint bool                     `json:"inheritEntrypoint,omitempty"`
	DataPlane         *DataPlanePolicy         `json:"dataPlane,omitempty"`
	// ScheduleAffinities exposes the native scheduler semantics instead of
	// adding resource-specific shortcut fields such as nodeId.
	ScheduleAffinities []Affinity `json:"scheduleAffinities,omitempty"`
	// Optional logical create budget. A positive request value overrides the
	// environment and default without changing the legacy request shape.
	CreateTimeoutSeconds int `json:"createTimeoutSeconds"`
	// Optional scheduling budget. The logical create budget must also cover the
	// runtime initialization budget and the frontend response buffer.
	ScheduleTimeoutSeconds int `json:"scheduleTimeoutSeconds"`
	// InitCallTimeoutSeconds is an SDK-owned internal budget. Zero keeps
	// compatibility with clients that predate this request field.
	InitCallTimeoutSeconds int `json:"initCallTimeoutSeconds,omitempty"`
	// nameGenerated marks an anonymous request whose instance name was assigned
	// by this frontend. It is excluded from JSON and request digests.
	nameGenerated bool
}

// RootfsSpec describes a structured sandbox rootfs request for the v1 API.
type RootfsSpec struct {
	Runtime     string                 `json:"runtime,omitempty"`
	Type        string                 `json:"type,omitempty"`
	ImageURL    string                 `json:"imageurl,omitempty"`
	Path        string                 `json:"path,omitempty"`
	ReadOnly    *bool                  `json:"readonly,omitempty"`
	StorageInfo map[string]interface{} `json:"storageInfo,omitempty"`
}

// TunnelSpec asks the frontend to prepare the sandbox-side reverse tunnel.
//
// The client intentionally does not expose RRT_TUNNEL_* envs or the tunnel
// control port. Frontend owns those runtime details and returns the stable
// /tunnel/{safeID} gateway URL path after create.
type TunnelSpec struct {
	Enabled   bool `json:"enabled,omitempty"`
	ProxyPort int  `json:"proxyPort,omitempty"`
}

// SandboxPortRange is an inclusive TCP or UDP port interval.
type SandboxPortRange struct {
	First uint32 `json:"first"`
	Last  uint32 `json:"last"`
}

// SandboxNetworkEndpoint selects a remote CIDR/domain and optional port range.
type SandboxNetworkEndpoint struct {
	CIDR      string            `json:"cidr,omitempty"`
	Domain    string            `json:"domain,omitempty"`
	PortRange *SandboxPortRange `json:"portRange,omitempty"`
}

// SandboxTrafficRule is expressed from the sandbox's point of view.
type SandboxTrafficRule struct {
	Action           string                  `json:"action"`
	Direction        string                  `json:"direction"`
	Protocol         string                  `json:"protocol"`
	Peer             *SandboxNetworkEndpoint `json:"peer,omitempty"`
	SandboxPortRange *SandboxPortRange       `json:"sandboxPortRange,omitempty"`
	Priority         uint32                  `json:"priority"`
}

// SandboxTrafficPolicy has independent ingress and egress defaults.
type SandboxTrafficPolicy struct {
	IngressDefaultAction string               `json:"ingressDefaultAction"`
	EgressDefaultAction  string               `json:"egressDefaultAction"`
	Mode                 string               `json:"mode"`
	Rules                []SandboxTrafficRule `json:"rules"`
}

// SandboxDNSRule matches an exact name or one leading-wildcard suffix.
type SandboxDNSRule struct {
	Action  string `json:"action"`
	Pattern string `json:"pattern"`
}

// SandboxDNSPolicy controls conventional DNS queries.
type SandboxDNSPolicy struct {
	DefaultAction string           `json:"defaultAction"`
	Rules         []SandboxDNSRule `json:"rules"`
}

// SandboxNetworkPolicy is the public creation-time network policy. Legacy
// fields are retained for old clients and cannot be combined with schema v2.
type SandboxNetworkPolicy struct {
	BlockNetwork  bool                  `json:"blockNetwork,omitempty"`
	DNSBlacklist  []string              `json:"dnsBlacklist,omitempty"`
	SchemaVersion uint32                `json:"schemaVersion,omitempty"`
	Traffic       *SandboxTrafficPolicy `json:"traffic,omitempty"`
	DNS           *SandboxDNSPolicy     `json:"dns,omitempty"`
}

// UnmarshalJSON rejects misspelled ACL fields instead of silently weakening a
// default-deny request. Unknown fields elsewhere in create requests retain the
// frontend's legacy behavior.
func (policy *SandboxNetworkPolicy) UnmarshalJSON(data []byte) error {
	type alias SandboxNetworkPolicy
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode((*alias)(policy)); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("invalid trailing network policy data")
	}
	return nil
}

// DataPlaneSecurityMode identifies the minimum transport security required by
// a sandbox data-plane endpoint.
type DataPlaneSecurityMode string

const (
	// DataPlaneSecurityTLS requires an authenticated TLS transport.
	DataPlaneSecurityTLS DataPlaneSecurityMode = "tls"
	// DataPlaneSecurityTLSToken additionally requires a validated user token.
	DataPlaneSecurityTLSToken DataPlaneSecurityMode = "tls-token"
)

// DataPlanePolicy selects the minimum client-side security for this
// sandbox. Both modes require TLS; tls-token additionally requires a validated
// user token. Direct access is fixed to tls-token and has no override.
type DataPlanePolicy struct {
	TunnelSecurityMode      DataPlaneSecurityMode `json:"tunnelSecurityMode,omitempty"`
	PortForwardSecurityMode DataPlaneSecurityMode `json:"portForwardSecurityMode,omitempty"`
}

// CreateV1Request holds POST /api/sandbox/v1/sandboxes parameters.
type CreateV1Request struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Tenant    string `json:"tenant"`
	// LegacyRuntime accepts the deprecated top-level runtime field from old
	// clients. New clients encode the isolation runtime as rootfs.runtime.
	LegacyRuntime          string                   `json:"runtime,omitempty"`
	Image                  string                   `json:"image"`
	Rootfs                 RootfsSpec               `json:"rootfs"`
	Ports                  []string                 `json:"ports"`
	IdleTimeoutSeconds     int                      `json:"idleTimeoutSeconds"`
	Cpu                    int                      `json:"cpu"`
	Memory                 int                      `json:"memory"`
	CpuLimit               int                      `json:"cpu_limit"`
	MemLimit               int                      `json:"mem_limit"`
	Env                    map[string]string        `json:"env"`
	Mounts                 []map[string]interface{} `json:"mounts"`
	ExtraConfig            map[string]interface{}   `json:"extra_config"`
	XPU                    string                   `json:"xpu"`
	StorageMb              *int64                   `json:"storageMb,omitempty"`
	StorageLimitMb         int64                    `json:"storage_limit_mb"`
	ScheduleAffinities     []Affinity               `json:"scheduleAffinities,omitempty"`
	Tunnel                 TunnelSpec               `json:"tunnel,omitempty"`
	Network                *SandboxNetworkPolicy    `json:"network,omitempty"`
	SnapshotID             string                   `json:"snapshotId,omitempty"`
	Failover               bool                     `json:"failover"`
	InheritEntrypoint      bool                     `json:"inheritEntrypoint,omitempty"`
	DataPlane              *DataPlanePolicy         `json:"dataPlane,omitempty"`
	CreateTimeoutSeconds   int                      `json:"createTimeoutSeconds"`
	ScheduleTimeoutSeconds int                      `json:"scheduleTimeoutSeconds"`
	InitCallTimeoutSeconds int                      `json:"initCallTimeoutSeconds,omitempty"`
	portRouteKinds         map[int]string
	nameGenerated          bool
}

// TunnelInfo describes the frontend-owned reverse tunnel endpoint for a sandbox.
type TunnelInfo struct {
	URL       string `json:"url"`
	Path      string `json:"path"`
	ProxyURL  string `json:"proxyUrl"`
	WSPath    string `json:"wsPath"`
	ProxyPort int    `json:"proxyPort"`
}

type sandboxCreateResult struct {
	instanceID string
	status     string
}

type sandboxCreateContext struct {
	context    context.Context
	req        CreateRequest
	funcID     string
	invokeOpts createOptions
	traceID    string
	requestID  string
}

type sandboxCreateCall struct {
	done            chan struct{}
	leaderRequestID string
	result          sandboxCreateResult
	err             error
}

type sandboxCreateSingleflight struct {
	mu    sync.Mutex
	calls map[string]*sandboxCreateCall
}

type sandboxCreateReplayEntry struct {
	done        chan struct{}
	digest      [sha256.Size]byte
	result      sandboxCreateResult
	err         error
	completedAt time.Time
}

type sandboxCreateReplayCompletion struct {
	key   string
	entry *sandboxCreateReplayEntry
}

type sandboxCreateReplayStore struct {
	mu              sync.Mutex
	entries         map[string]*sandboxCreateReplayEntry
	completionQueue []sandboxCreateReplayCompletion
	completionHead  int
	ttl             time.Duration
	maxEntries      int
	now             func() time.Time
}

type sandboxCreateReuse int

const (
	sandboxCreateReuseNone sandboxCreateReuse = iota
	sandboxCreateReuseInflight
	sandboxCreateReuseCompleted
)

type sandboxXPURequest struct {
	resourceName string
	count        int64
	normalized   string
}

type sandboxCreateRequestConflictError struct {
	requestID string
}

func (err *sandboxCreateRequestConflictError) Error() string {
	return fmt.Sprintf(
		"requestId '%s' has already been used to create a sandbox with different parameters",
		err.requestID,
	)
}

type sandboxCreateInFlightConflictError struct {
	leaderRequestID string
}

func (err *sandboxCreateInFlightConflictError) Error() string {
	return fmt.Sprintf("sandbox is already being created by request %s", err.leaderRequestID)
}

type sandboxAlreadyExistsError struct {
	requestID       string
	namespace       string
	name            string
	instanceID      string
	leaderRequestID string
}

func (err *sandboxAlreadyExistsError) Error() string {
	if err.leaderRequestID != "" {
		return fmt.Sprintf(
			"sandbox '%s/%s' is already being created by request %s; "+
				"request %s cannot create the same sandbox (sandboxId=%s)",
			err.namespace, err.name, err.leaderRequestID, err.requestID, err.instanceID,
		)
	}
	return fmt.Sprintf(
		"sandbox '%s/%s' already exists (sandboxId=%s, requestId=%s)",
		err.namespace, err.name, err.instanceID, err.requestID,
	)
}

var createSingleflight = sandboxCreateSingleflight{
	calls: make(map[string]*sandboxCreateCall),
}

var createReplayStore = newSandboxCreateReplayStore(
	sandboxCreateReplayTTL,
	sandboxCreateReplayMaxEntries,
)

func init() {
	createReplayStore.startCleanup(sandboxCreateReplayCleanupEvery)
}

func newSandboxCreateReplayStore(ttl time.Duration, maxEntries int) *sandboxCreateReplayStore {
	return &sandboxCreateReplayStore{
		entries:    make(map[string]*sandboxCreateReplayEntry),
		ttl:        ttl,
		maxEntries: maxEntries,
		now:        time.Now,
	}
}

func (store *sandboxCreateReplayStore) startCleanup(interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			store.cleanup(store.now(), sandboxCreateReplayCleanupBatch)
		}
	}()
}

func (group *sandboxCreateSingleflight) do(
	key string,
	requestID string,
	create func() (sandboxCreateResult, error),
) (sandboxCreateResult, error, string, bool) {
	group.mu.Lock()
	if call, ok := group.calls[key]; ok {
		if call.leaderRequestID != requestID {
			group.mu.Unlock()
			return sandboxCreateResult{status: sandboxCreateStatusFailed},
				&sandboxCreateInFlightConflictError{leaderRequestID: call.leaderRequestID},
				call.leaderRequestID, false
		}
		group.mu.Unlock()
		<-call.done
		return call.result, call.err, call.leaderRequestID, true
	}
	call := &sandboxCreateCall{
		done:            make(chan struct{}),
		leaderRequestID: requestID,
	}
	group.calls[key] = call
	group.mu.Unlock()

	call.result, call.err = create()

	group.mu.Lock()
	delete(group.calls, key)
	close(call.done)
	group.mu.Unlock()
	return call.result, call.err, requestID, false
}

func (store *sandboxCreateReplayStore) do(
	key string,
	requestID string,
	digest [sha256.Size]byte,
	create func() (sandboxCreateResult, error),
) (sandboxCreateResult, error, sandboxCreateReuse) {
	store.mu.Lock()
	now := store.now()
	store.cleanupLocked(now, sandboxCreateReplayCleanupBatch)
	if entry, ok := store.entries[key]; ok {
		if entry.digest != digest {
			store.mu.Unlock()
			return sandboxCreateResult{status: sandboxCreateStatusFailed},
				&sandboxCreateRequestConflictError{requestID: requestID},
				sandboxCreateReuseNone
		}
		if entry.completedAt.IsZero() {
			store.mu.Unlock()
			<-entry.done
			return entry.result, entry.err, sandboxCreateReuseInflight
		}
		result, err := entry.result, entry.err
		store.mu.Unlock()
		return result, err, sandboxCreateReuseCompleted
	}

	entry := &sandboxCreateReplayEntry{
		done:   make(chan struct{}),
		digest: digest,
	}
	store.entries[key] = entry
	store.mu.Unlock()

	entry.result, entry.err = create()

	store.mu.Lock()
	entry.completedAt = store.now()
	close(entry.done)
	store.completionQueue = append(store.completionQueue, sandboxCreateReplayCompletion{
		key:   key,
		entry: entry,
	})
	store.cleanupLocked(entry.completedAt, sandboxCreateReplayCleanupBatch)
	store.mu.Unlock()
	return entry.result, entry.err, sandboxCreateReuseNone
}

func (store *sandboxCreateReplayStore) cleanup(now time.Time, expiredLimit int) {
	store.mu.Lock()
	store.cleanupLocked(now, expiredLimit)
	store.mu.Unlock()
}

// cleanupLocked removes completed entries in completion order. Each queue
// record is appended and removed once, so expiry and capacity eviction are
// amortized O(1) instead of scanning the entire replay map per request.
func (store *sandboxCreateReplayStore) cleanupLocked(now time.Time, expiredLimit int) {
	expiredRemoved := 0
	for store.completionHead < len(store.completionQueue) {
		record := store.completionQueue[store.completionHead]
		entry, exists := store.entries[record.key]
		if !exists || entry != record.entry {
			store.popCompletionLocked()
			continue
		}

		overCapacity := store.maxEntries > 0 && len(store.entries) > store.maxEntries
		expired := store.ttl <= 0 || !now.Before(entry.completedAt.Add(store.ttl))
		if !overCapacity && (!expired || expiredRemoved >= expiredLimit) {
			break
		}

		delete(store.entries, record.key)
		store.popCompletionLocked()
		if expired {
			expiredRemoved++
		}
	}
	store.compactCompletionQueueLocked()
}

func (store *sandboxCreateReplayStore) popCompletionLocked() {
	store.completionQueue[store.completionHead] = sandboxCreateReplayCompletion{}
	store.completionHead++
}

func (store *sandboxCreateReplayStore) compactCompletionQueueLocked() {
	if store.completionHead == len(store.completionQueue) {
		store.completionQueue = nil
		store.completionHead = 0
		return
	}
	if store.completionHead < sandboxCreateReplayCompactAt ||
		store.completionHead*sandboxCreateReplayCompactRatio < len(store.completionQueue) {
		return
	}
	remaining := copy(store.completionQueue, store.completionQueue[store.completionHead:])
	clear(store.completionQueue[remaining:])
	store.completionQueue = store.completionQueue[:remaining]
	store.completionHead = 0
}

// InvokeV1Request is the single data-plane envelope for file/process/shell actions.
type InvokeV1Request struct {
	Action string                 `json:"action"`
	Args   map[string]interface{} `json:"args"`
}

// CreateHandler handles POST /api/sandbox/create.
// It calls CreateInstanceRaw with the native core CreateRequest to create the
// built-in sandbox. The worker loads yr.sandbox.sandbox.SandboxInstance from
// its local Python environment, without actor metadata or serialized arguments.
func CreateHandler(ctx *gin.Context) {
	traceID := ensureSandboxTrace(ctx)
	requestID := ensureSandboxRequestID(ctx, traceID)
	var req CreateRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("invalid request body: %v", err))
		return
	}
	result, err := createSandbox(ctx, req, 0, traceID, requestID, true)
	if err != nil {
		return
	}
	httpx.SetCtxResponse(ctx, map[string]string{"instance_id": result.instanceID}, http.StatusOK, nil)
}

// CreateV1Handler handles POST /api/sandbox/v1/sandboxes.
func CreateV1Handler(ctx *gin.Context) {
	traceID := ensureSandboxTrace(ctx)
	requestID := ensureSandboxRequestID(ctx, traceID)
	var req CreateV1Request
	if status, err := readCreateV1Request(ctx, &req); err != nil {
		httpx.SetCtxResponse(ctx, nil, status, err)
		return
	}
	rootfs, tunnelInfo, err := prepareCreateV1Request(&req)
	if err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, err)
		return
	}
	createReq := createRequestFromV1(req, rootfs)
	if acceptsSandboxCreateEventStream(ctx) {
		streamSandboxCreate(ctx, createReq, req.IdleTimeoutSeconds, traceID, requestID, tunnelInfo)
		return
	}

	result, err := createSandbox(ctx, createReq, req.IdleTimeoutSeconds, traceID, requestID, true)
	if err != nil {
		return
	}
	httpx.SetCtxResponse(
		ctx, createV1Response(result.instanceID, sandboxCreateStatusRunning, requestID, tunnelInfo), http.StatusOK, nil,
	)
}

func readCreateV1Request(ctx *gin.Context, req *CreateV1Request) (int, error) {
	ctx.Request.Body = http.MaxBytesReader(ctx.Writer, ctx.Request.Body, sandboxCreateRequestBodyLimit)
	body, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return http.StatusRequestEntityTooLarge, fmt.Errorf(
				"request body exceeds %d bytes", sandboxCreateRequestBodyLimit,
			)
		}
		return http.StatusBadRequest, fmt.Errorf("failed to read request body: %v", err)
	}
	if err := json.Unmarshal(body, req); err != nil {
		return http.StatusBadRequest, fmt.Errorf("invalid request body: %v", err)
	}
	return 0, nil
}

func prepareCreateV1Request(req *CreateV1Request) (string, *TunnelInfo, error) {
	if req.Name == "" {
		req.Name = newSandboxName()
		req.nameGenerated = true
	}
	if req.Namespace == "" {
		req.Namespace = "default"
	}
	req.LegacyRuntime = strings.TrimSpace(req.LegacyRuntime)
	req.Rootfs.Runtime = strings.TrimSpace(req.Rootfs.Runtime)
	if req.LegacyRuntime != "" && req.Rootfs.Runtime != "" && req.LegacyRuntime != req.Rootfs.Runtime {
		return "", nil, fmt.Errorf(
			"deprecated top-level runtime %q conflicts with rootfs.runtime %q",
			req.LegacyRuntime,
			req.Rootfs.Runtime,
		)
	}
	if req.Rootfs.Runtime == "" {
		req.Rootfs.Runtime = req.LegacyRuntime
	}
	if req.Rootfs.Runtime == "" {
		req.Rootfs.Runtime = "runsc"
	}
	if err := validateScheduleAffinities(req.ScheduleAffinities); err != nil {
		return "", nil, err
	}
	xpu, err := parseSandboxXPU(req.XPU)
	if err != nil {
		return "", nil, err
	}
	if xpu != nil {
		req.XPU = xpu.normalized
	}
	if err := validateSandboxStorage(req.StorageMb, req.StorageLimitMb); err != nil {
		return "", nil, err
	}
	if err := validateSandboxDataPlanePolicy(req.DataPlane); err != nil {
		return "", nil, err
	}
	rootfs, err := buildRootfsOption(req.Rootfs, req.Image)
	if err != nil {
		return "", nil, err
	}
	if req.InheritEntrypoint {
		resolvedRootfs, image := normalizeRootfsSpec(req.Rootfs, req.Image)
		if strings.TrimSpace(req.SnapshotID) != "" {
			return "", nil, fmt.Errorf("inheritEntrypoint cannot be combined with snapshotId")
		}
		if resolvedRootfs.Type != "image" || image == "" {
			return "", nil, fmt.Errorf("inheritEntrypoint requires an image rootfs")
		}
	}
	network, err := normalizeSandboxNetworkPolicy(req.Network)
	if err != nil {
		return "", nil, err
	}
	req.Network = network
	prepareSandboxRRTHTTP(req)
	return rootfs, prepareSandboxTunnel(req), nil
}

func validateSandboxDataPlanePolicy(policy *DataPlanePolicy) error {
	if policy == nil {
		return nil
	}
	for name, mode := range map[string]DataPlaneSecurityMode{
		"tunnelSecurityMode":      policy.TunnelSecurityMode,
		"portForwardSecurityMode": policy.PortForwardSecurityMode,
	} {
		if mode != "" && mode != DataPlaneSecurityTLS && mode != DataPlaneSecurityTLSToken {
			return fmt.Errorf("dataPlane.%s must be tls or tls-token", name)
		}
	}
	return nil
}

func parseSandboxXPU(value string) (*sandboxXPURequest, error) {
	if value == "" {
		return nil, nil
	}
	fields := strings.Split(value, ":")
	if len(fields) != sandboxXPUFieldCount || fields[0] == "" || fields[2] == "" {
		return nil, fmt.Errorf("xpu must have exactly three fields: type:model:count")
	}
	for _, field := range fields {
		if strings.TrimSpace(field) != field {
			return nil, fmt.Errorf("xpu fields must not contain surrounding whitespace")
		}
	}

	xpuType := strings.ToLower(fields[0])
	model := fields[1]
	if !sandboxXPUTypePattern.MatchString(xpuType) {
		return nil, fmt.Errorf("xpu type must be an identifier")
	}
	if !sandboxXPUCountPattern.MatchString(fields[2]) {
		return nil, fmt.Errorf("xpu count must be a positive integer")
	}
	count, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || count <= 0 {
		return nil, fmt.Errorf("xpu count must be a positive integer")
	}

	modelPattern := ".+"
	if model != "" {
		modelPattern = regexp.QuoteMeta(model)
	}
	return &sandboxXPURequest{
		resourceName: strings.ToUpper(xpuType) + "/" + modelPattern + "/count",
		count:        count,
		normalized:   fmt.Sprintf("%s:%s:%d", xpuType, model, count),
	}, nil
}

func validateSandboxStorage(storageMb *int64, storageLimitMb int64) error {
	if storageLimitMb < 0 {
		return fmt.Errorf("storage_limit_mb must be 0 or a positive integer")
	}
	if storageLimitMb > math.MaxInt64/bytesPerMiB {
		return fmt.Errorf("storage_limit_mb is too large")
	}
	if storageMb == nil {
		return nil
	}
	if *storageMb <= 0 {
		return fmt.Errorf("storageMb must be a positive integer")
	}
	if *storageMb > math.MaxInt64/bytesPerMiB {
		return fmt.Errorf("storageMb is too large")
	}
	if storageLimitMb > 0 && storageLimitMb < *storageMb {
		return fmt.Errorf("storage_limit_mb must be greater than or equal to storageMb")
	}
	return nil
}

func newSandboxName() string {
	return "sandbox-" + uuid.NewString()
}

func normalizeSandboxNetworkPolicy(policy *SandboxNetworkPolicy) (*SandboxNetworkPolicy, error) {
	if policy == nil {
		return nil, nil
	}
	if policy.SchemaVersion != 0 && policy.SchemaVersion != 2 {
		return nil, fmt.Errorf("network schemaVersion must be 2 or omitted for legacy policies")
	}
	if policy.SchemaVersion == 2 {
		if policy.BlockNetwork || len(policy.DNSBlacklist) > 0 {
			return nil, fmt.Errorf("legacy and schema v2 network policy fields cannot be combined")
		}
		traffic, err := normalizeSandboxTrafficPolicy(policy.Traffic)
		if err != nil {
			return nil, err
		}
		dns, err := normalizeSandboxDNSPolicy(policy.DNS)
		if err != nil {
			return nil, err
		}
		if traffic == nil && dns == nil {
			return nil, nil
		}
		return &SandboxNetworkPolicy{
			SchemaVersion: 2,
			Traffic:       traffic,
			DNS:           dns,
		}, nil
	}
	if policy.Traffic != nil || policy.DNS != nil {
		return nil, fmt.Errorf("schema v2 network fields require schemaVersion 2")
	}
	if policy.BlockNetwork && len(policy.DNSBlacklist) > 0 {
		return nil, fmt.Errorf("blockNetwork and dnsBlacklist cannot be combined")
	}
	normalized := make([]string, 0, len(policy.DNSBlacklist))
	seen := make(map[string]struct{}, len(policy.DNSBlacklist))
	for _, pattern := range policy.DNSBlacklist {
		value, err := normalizeSandboxDNSPattern(pattern)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	if !policy.BlockNetwork && len(normalized) == 0 {
		return nil, nil
	}
	return &SandboxNetworkPolicy{
		BlockNetwork: policy.BlockNetwork,
		DNSBlacklist: normalized,
	}, nil
}

func normalizeSandboxTrafficPolicy(policy *SandboxTrafficPolicy) (*SandboxTrafficPolicy, error) {
	if policy == nil {
		return nil, nil
	}
	ingress, err := normalizeSandboxNetworkChoice(
		policy.IngressDefaultAction, "traffic.ingressDefaultAction", "allow", "deny",
	)
	if err != nil {
		return nil, err
	}
	egress, err := normalizeSandboxNetworkChoice(
		policy.EgressDefaultAction, "traffic.egressDefaultAction", "allow", "deny",
	)
	if err != nil {
		return nil, err
	}
	mode := policy.Mode
	if strings.TrimSpace(mode) == "" {
		mode = "stateful"
	}
	mode, err = normalizeSandboxNetworkChoice(mode, "traffic.mode", "stateful", "stateless")
	if err != nil {
		return nil, err
	}
	if len(policy.Rules) > maxSandboxTrafficRules {
		return nil, fmt.Errorf("traffic.rules supports at most %d entries", maxSandboxTrafficRules)
	}
	rules := make([]SandboxTrafficRule, 0, len(policy.Rules))
	for index, rule := range policy.Rules {
		normalized, err := normalizeSandboxTrafficRule(rule, index)
		if err != nil {
			return nil, err
		}
		rules = append(rules, normalized)
	}
	return &SandboxTrafficPolicy{
		IngressDefaultAction: ingress,
		EgressDefaultAction:  egress,
		Mode:                 mode,
		Rules:                rules,
	}, nil
}

func normalizeSandboxTrafficRule(rule SandboxTrafficRule, index int) (SandboxTrafficRule, error) {
	prefix := fmt.Sprintf("traffic.rules[%d]", index)
	action, err := normalizeSandboxNetworkChoice(rule.Action, prefix+".action", "allow", "deny")
	if err != nil {
		return SandboxTrafficRule{}, err
	}
	direction, err := normalizeSandboxNetworkChoice(
		rule.Direction, prefix+".direction", "ingress", "egress", "both",
	)
	if err != nil {
		return SandboxTrafficRule{}, err
	}
	protocol, err := normalizeSandboxNetworkChoice(
		rule.Protocol, prefix+".protocol", "any", "tcp", "udp", "icmp",
	)
	if err != nil {
		return SandboxTrafficRule{}, err
	}
	priority := rule.Priority
	if priority == 0 {
		priority = defaultSandboxRulePriority
	}
	if priority > maxSandboxUserRulePriority {
		return SandboxTrafficRule{}, fmt.Errorf(
			"%s.priority must be in 1..%d", prefix, maxSandboxUserRulePriority,
		)
	}
	peer, err := normalizeSandboxNetworkEndpoint(rule.Peer, direction, protocol, prefix+".peer")
	if err != nil {
		return SandboxTrafficRule{}, err
	}
	sandboxPorts, err := normalizeSandboxPortRange(rule.SandboxPortRange, prefix+".sandboxPortRange")
	if err != nil {
		return SandboxTrafficRule{}, err
	}
	if sandboxPorts != nil && protocol != "tcp" && protocol != "udp" {
		return SandboxTrafficRule{}, fmt.Errorf("%s requires protocol tcp or udp", prefix+".sandboxPortRange")
	}
	return SandboxTrafficRule{
		Action:           action,
		Direction:        direction,
		Protocol:         protocol,
		Peer:             peer,
		SandboxPortRange: sandboxPorts,
		Priority:         priority,
	}, nil
}

func normalizeSandboxNetworkEndpoint(
	peer *SandboxNetworkEndpoint, direction, protocol, prefix string,
) (*SandboxNetworkEndpoint, error) {
	if peer == nil {
		return nil, nil
	}
	if strings.TrimSpace(peer.CIDR) != "" && strings.TrimSpace(peer.Domain) != "" {
		return nil, fmt.Errorf("%s cannot contain both cidr and domain", prefix)
	}
	cidr := ""
	if strings.TrimSpace(peer.CIDR) != "" {
		var err error
		cidr, err = normalizeSandboxCIDR(peer.CIDR)
		if err != nil {
			return nil, fmt.Errorf("%s.cidr: %w", prefix, err)
		}
	}
	domain := ""
	if strings.TrimSpace(peer.Domain) != "" {
		if direction != "egress" {
			return nil, fmt.Errorf("%s.domain is valid only for egress", prefix)
		}
		var err error
		domain, err = normalizeSandboxDomainPattern(peer.Domain, "domain")
		if err != nil {
			return nil, fmt.Errorf("%s.domain: %w", prefix, err)
		}
	}
	ports, err := normalizeSandboxPortRange(peer.PortRange, prefix+".portRange")
	if err != nil {
		return nil, err
	}
	if ports != nil && protocol != "tcp" && protocol != "udp" {
		return nil, fmt.Errorf("%s.portRange requires protocol tcp or udp", prefix)
	}
	if cidr == "" && domain == "" && ports == nil {
		return nil, nil
	}
	return &SandboxNetworkEndpoint{CIDR: cidr, Domain: domain, PortRange: ports}, nil
}

func normalizeSandboxPortRange(portRange *SandboxPortRange, prefix string) (*SandboxPortRange, error) {
	if portRange == nil {
		return nil, nil
	}
	if portRange.First == 0 || portRange.Last == 0 || portRange.First > portRange.Last ||
		portRange.Last > maxSandboxPort {
		return nil, fmt.Errorf("%s must satisfy 1 <= first <= last <= %d", prefix, maxSandboxPort)
	}
	return &SandboxPortRange{First: portRange.First, Last: portRange.Last}, nil
}

func normalizeSandboxCIDR(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if !strings.Contains(trimmed, "/") {
		address, err := netip.ParseAddr(trimmed)
		if err != nil || !address.Is4() {
			return "", fmt.Errorf("%q is not an IPv4 address or CIDR", value)
		}
		return netip.PrefixFrom(address, 32).String(), nil
	}
	prefix, err := netip.ParsePrefix(trimmed)
	if err != nil || !prefix.Addr().Is4() {
		return "", fmt.Errorf("%q is not an IPv4 address or CIDR", value)
	}
	return prefix.Masked().String(), nil
}

func normalizeSandboxDNSPolicy(policy *SandboxDNSPolicy) (*SandboxDNSPolicy, error) {
	if policy == nil {
		return nil, nil
	}
	defaultAction, err := normalizeSandboxNetworkChoice(
		policy.DefaultAction, "dns.defaultAction", "allow", "deny",
	)
	if err != nil {
		return nil, err
	}
	rules := make([]SandboxDNSRule, 0, len(policy.Rules))
	for index, rule := range policy.Rules {
		action, err := normalizeSandboxNetworkChoice(
			rule.Action, fmt.Sprintf("dns.rules[%d].action", index), "allow", "deny",
		)
		if err != nil {
			return nil, err
		}
		pattern, err := normalizeSandboxDomainPattern(rule.Pattern, "DNS")
		if err != nil {
			return nil, fmt.Errorf("dns.rules[%d].pattern: %w", index, err)
		}
		rules = append(rules, SandboxDNSRule{Action: action, Pattern: pattern})
	}
	return &SandboxDNSPolicy{DefaultAction: defaultAction, Rules: rules}, nil
}

func normalizeSandboxNetworkChoice(value, name string, allowed ...string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	for _, candidate := range allowed {
		if normalized == candidate {
			return normalized, nil
		}
	}
	return "", fmt.Errorf("%s must be one of %s", name, strings.Join(allowed, ", "))
}

func normalizeSandboxDNSPattern(pattern string) (string, error) {
	return normalizeSandboxDomainPattern(pattern, "DNS blacklist")
}

func normalizeSandboxDomainPattern(pattern, description string) (string, error) {
	value := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(pattern), "."))
	wildcard := strings.HasPrefix(value, "*.")
	if wildcard {
		value = strings.TrimPrefix(value, "*.")
	}
	if value == "" || strings.ContainsAny(value, "*?") {
		return "", fmt.Errorf("invalid %s pattern %q", description, pattern)
	}
	ascii, err := idna.Lookup.ToASCII(value)
	if err != nil {
		ascii = value
		for _, char := range ascii {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') &&
				char != '-' && char != '_' && char != '.' {
				return "", fmt.Errorf("invalid %s pattern %q", description, pattern)
			}
		}
	}
	value = strings.ToLower(ascii)
	if len(value) > 253 {
		return "", fmt.Errorf("invalid %s pattern %q", description, pattern)
	}
	for _, label := range strings.Split(value, ".") {
		if len(label) == 0 || len(label) > 63 || strings.HasPrefix(label, "-") ||
			strings.HasSuffix(label, "-") || !sandboxDNSLabelPattern.MatchString(label) {
			return "", fmt.Errorf("invalid %s pattern %q", description, pattern)
		}
	}
	if wildcard {
		return "*." + value, nil
	}
	return value, nil
}

func validateScheduleAffinities(affinities []Affinity) error {
	for affinityIndex, affinity := range affinities {
		if affinity.Kind != AffinityKindResource &&
			affinity.Kind != AffinityKindInstance {
			return fmt.Errorf(
				"scheduleAffinities[%d].kind must be resource(0) or instance(1)",
				affinityIndex,
			)
		}
		if affinity.Affinity < PreferredAffinity ||
			affinity.Affinity > RequiredAntiAffinity {
			return fmt.Errorf(
				"scheduleAffinities[%d].affinity must be between 0 and 3",
				affinityIndex,
			)
		}
		if len(affinity.LabelOps) == 0 {
			return fmt.Errorf(
				"scheduleAffinities[%d].labelOps must not be empty",
				affinityIndex,
			)
		}
		for operatorIndex, operator := range affinity.LabelOps {
			if operator.Type < LabelOpIn ||
				operator.Type > LabelOpNotExists {
				return fmt.Errorf(
					"scheduleAffinities[%d].labelOps[%d].type must be between 0 and 3",
					affinityIndex,
					operatorIndex,
				)
			}
			if strings.TrimSpace(operator.LabelKey) == "" {
				return fmt.Errorf(
					"scheduleAffinities[%d].labelOps[%d].labelKey must not be empty",
					affinityIndex,
					operatorIndex,
				)
			}
			if (operator.Type == LabelOpIn ||
				operator.Type == LabelOpNotIn) &&
				len(operator.LabelValues) == 0 {
				return fmt.Errorf(
					"scheduleAffinities[%d].labelOps[%d].labelValues must not be empty",
					affinityIndex,
					operatorIndex,
				)
			}
		}
	}
	return nil
}

func createRequestFromV1(req CreateV1Request, rootfs string) CreateRequest {
	return CreateRequest{
		Name:                   req.Name,
		Namespace:              req.Namespace,
		Tenant:                 req.Tenant,
		Runtime:                defaultSandboxRuntime,
		Rootfs:                 rootfs,
		Ports:                  req.Ports,
		portRouteKinds:         req.portRouteKinds,
		Cpu:                    req.Cpu,
		Memory:                 req.Memory,
		CpuLimit:               req.CpuLimit,
		MemLimit:               req.MemLimit,
		Env:                    req.Env,
		Mounts:                 req.Mounts,
		ExtraConfig:            req.ExtraConfig,
		XPU:                    req.XPU,
		StorageMb:              req.StorageMb,
		StorageLimitMb:         req.StorageLimitMb,
		Network:                req.Network,
		DataPlane:              req.DataPlane,
		ScheduleAffinities:     req.ScheduleAffinities,
		SnapshotID:             strings.TrimSpace(req.SnapshotID),
		Failover:               req.Failover,
		InheritEntrypoint:      req.InheritEntrypoint,
		CreateTimeoutSeconds:   req.CreateTimeoutSeconds,
		ScheduleTimeoutSeconds: req.ScheduleTimeoutSeconds,
		InitCallTimeoutSeconds: req.InitCallTimeoutSeconds,
		nameGenerated:          req.nameGenerated,
	}
}

func streamSandboxCreate(
	ctx *gin.Context,
	createReq CreateRequest,
	idleTimeoutSeconds int,
	traceID string,
	requestID string,
	tunnelInfo *TunnelInfo,
) {
	writeSandboxCreateSSEHeader(ctx)
	if err := writeSandboxCreateSSEEvent(ctx, "accepted", map[string]interface{}{
		"status":    sandboxCreateStatusCreating,
		"requestId": requestID,
	}); err != nil {
		httpx.Logger().Errorf("failed to write sandbox accepted event: %v", err)
		return
	}

	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go serveSandboxCreateHeartbeats(ctx, stopHeartbeat, heartbeatDone)
	result, createErr := createSandbox(ctx, createReq, idleTimeoutSeconds, traceID, requestID, false)
	close(stopHeartbeat)
	<-heartbeatDone

	data := createV1Response(result.instanceID, result.status, requestID, tunnelInfo)
	if createErr != nil {
		data["errorCode"] = createErrorCode(createErr)
		data["message"] = createErr.Error()
	}
	if err := writeSandboxCreateSSEEvent(ctx, "final", data); err != nil {
		httpx.Logger().Errorf("failed to write sandbox final event: %v", err)
	}
}

func createV1Response(instanceID, status, requestID string, tunnelInfo *TunnelInfo) map[string]interface{} {
	resp := map[string]interface{}{
		"sandboxId":  instanceID,
		"instanceId": instanceID,
		"status":     status,
		"requestId":  requestID,
	}
	if tunnelInfo != nil && status == sandboxCreateStatusRunning {
		tunnelInfo.URL = fmt.Sprintf("/tunnel/%s", sanitizeInstanceID(instanceID))
		tunnelInfo.Path = tunnelInfo.URL
		tunnelInfo.WSPath = tunnelInfo.URL
		resp["tunnel"] = tunnelInfo
	}
	return resp
}

func prepareSandboxRRTHTTP(req *CreateV1Request) {
	req.Ports = appendUniquePorts(req.Ports, strconv.Itoa(sandboxDefaultRRTHTTPPort))
	setSandboxPortRouteKind(req, sandboxDefaultRRTHTTPPort, sandboxRouteDirect)
	if req.Env == nil {
		req.Env = map[string]string{}
	}
	// Frontend owns the direct-invoke RRT HTTP server port. SDK callers use
	// /direct/{safeID}/invoke and never need to know or set RRT_HTTP_PORT.
	req.Env["RRT_HTTP_PORT"] = strconv.Itoa(sandboxDefaultRRTHTTPPort)
	// Command recovery budgets are platform-owned. Copy the resolved Frontend
	// deployment values into every RRT sandbox so users cannot silently disable
	// retention, output bounds, watch activity leasing, or registry limits.
	commandEnvDefaults := map[string]string{
		"RRT_COMMAND_RESULT_TTL_SECS":                      "3600",
		"RRT_COMMAND_STDOUT_LIMIT_BYTES":                   "4194304",
		"RRT_COMMAND_STDERR_LIMIT_BYTES":                   "4194304",
		"RRT_COMMAND_REGISTRY_MAX_RECORDS":                 "4096",
		"RRT_COMMAND_REGISTRY_MAX_BYTES":                   "268435456",
		"RRT_COMMAND_REGISTRY_MEMORY_HIGH_WATERMARK_BYTES": "201326592",
		"RRT_COMMAND_ACTIVITY_HEARTBEAT_SECS":              "10",
		"RRT_COMMAND_WATCH_MAX_SUBSCRIPTIONS":              "4096",
		"RRT_COMMAND_WATCH_MAX_FRAME_BYTES":                "1048576",
	}
	for key, fallback := range commandEnvDefaults {
		deploymentKey := "ADX_" + key
		if key == "RRT_COMMAND_WATCH_MAX_SUBSCRIPTIONS" {
			deploymentKey = "ADX_COMMAND_WATCH_MAX_SUBSCRIPTIONS_PER_CONNECTION"
		} else if key == "RRT_COMMAND_WATCH_MAX_FRAME_BYTES" {
			deploymentKey = "ADX_COMMAND_WATCH_MAX_FRAME_BYTES"
		}
		value := strings.TrimSpace(os.Getenv(deploymentKey))
		if value == "" {
			value = fallback
		}
		req.Env[key] = value
	}
}

func usesSandboxRRTRuntime(runtime string) bool {
	selectedRuntime := strings.ToLower(strings.TrimSpace(runtime))
	if selectedRuntime == "" {
		selectedRuntime = defaultSandboxRuntime
	}
	return selectedRuntime == "rust" || selectedRuntime == "rrt" || selectedRuntime == "rrt-runtime"
}

func prepareSandboxTunnel(req *CreateV1Request) *TunnelInfo {
	if !req.Tunnel.Enabled {
		return nil
	}
	proxyPort := req.Tunnel.ProxyPort
	if proxyPort <= 0 {
		proxyPort = sandboxDefaultTunnelHTTPPort
	}
	wsPort := proxyPort - 1
	if wsPort <= 0 {
		wsPort = sandboxDefaultTunnelWSPort
		proxyPort = sandboxDefaultTunnelHTTPPort
	}
	req.Ports = appendUniquePorts(req.Ports, strconv.Itoa(wsPort), strconv.Itoa(proxyPort))
	setSandboxPortRouteKind(req, wsPort, sandboxRouteTunnel)
	setSandboxPortRouteKind(req, proxyPort, sandboxRouteDirect)
	if req.Env == nil {
		req.Env = map[string]string{}
	}
	// Frontend deliberately owns these RRT runtime envs. SDK callers request a
	// tunnel declaratively and never need to know or override the control ports.
	req.Env["RRT_TUNNEL_WS_PORT"] = strconv.Itoa(wsPort)
	req.Env["RRT_TUNNEL_HTTP_PORT"] = strconv.Itoa(proxyPort)
	return &TunnelInfo{
		ProxyURL:  fmt.Sprintf("http://127.0.0.1:%d", proxyPort),
		ProxyPort: proxyPort,
	}
}

func setSandboxPortRouteKind(req *CreateV1Request, port int, routeKind string) {
	if req.portRouteKinds == nil {
		req.portRouteKinds = make(map[int]string)
	}
	req.portRouteKinds[port] = routeKind
}

func appendUniquePorts(ports []string, values ...string) []string {
	seen := make(map[string]bool, len(ports)+len(values))
	for _, port := range ports {
		seen[port] = true
	}
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		ports = append(ports, value)
		seen[value] = true
	}
	return ports
}

func createSandbox(
	ctx *gin.Context,
	req CreateRequest,
	idleTimeoutSeconds int,
	traceID string,
	requestID string,
	respondOnError bool,
) (sandboxCreateResult, error) {
	invocation, err := prepareSandboxInvocation(ctx, req, idleTimeoutSeconds, traceID)
	if err != nil {
		if respondOnError {
			httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, err)
		}
		return sandboxCreateResult{status: sandboxCreateStatusFailed}, err
	}
	digest, err := sandboxCreateRequestDigest(req, idleTimeoutSeconds)
	if err != nil {
		if respondOnError {
			httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, err)
		}
		return sandboxCreateResult{status: sandboxCreateStatusFailed}, err
	}
	requestKey := sandboxCreateRequestIDKey(ctx.Request, req, requestID)
	result, createErr, reuse := createReplayStore.do(requestKey, requestID, digest, func() (
		sandboxCreateResult, error,
	) {
		instanceID := req.Namespace + "-" + req.Name
		if !req.nameGenerated && sandboxInstanceExists(instanceID) {
			return sandboxCreateResult{
					instanceID: instanceID,
					status:     sandboxCreateStatusFailed,
				}, &sandboxAlreadyExistsError{
					requestID:  requestID,
					namespace:  req.Namespace,
					name:       req.Name,
					instanceID: instanceID,
				}
		}
		identityKey := sandboxCreateSingleflightKey(req)
		identityResult, identityErr, leaderRequestID, shared := createSingleflight.do(
			identityKey, requestID, func() (sandboxCreateResult, error) {
				createdInstanceID, callErr := createSandboxInstanceRaw(
					ctx, invocation, req.Name, req.Namespace,
				)
				if isSandboxInstanceDuplicated(callErr) {
					return sandboxCreateResult{
							instanceID: instanceID,
							status:     sandboxCreateStatusFailed,
						}, &sandboxAlreadyExistsError{
							requestID:  requestID,
							namespace:  req.Namespace,
							name:       req.Name,
							instanceID: instanceID,
						}
				}
				return finishSandboxCreate(sandboxCreateContext{
					context:    ctx.Request.Context(),
					req:        req,
					funcID:     invocation.funcID,
					invokeOpts: invocation.invokeOpts,
					traceID:    traceID,
					requestID:  requestID,
				}, createdInstanceID, callErr)
			},
		)
		var inflightErr *sandboxCreateInFlightConflictError
		if errors.As(identityErr, &inflightErr) {
			identityErr = &sandboxAlreadyExistsError{
				requestID:       requestID,
				namespace:       req.Namespace,
				name:            req.Name,
				instanceID:      instanceID,
				leaderRequestID: inflightErr.leaderRequestID,
			}
		}
		if shared {
			httpx.Logger().Infof(
				"coalesced duplicate sandbox create identity requestID=%s "+
					"leaderRequestID=%s sandboxID=%s name=%s ns=%s tenant=%s",
				requestID, leaderRequestID, identityResult.instanceID,
				req.Name, req.Namespace, sandboxCreateTenant(ctx.Request, req),
			)
		}
		return identityResult, identityErr
	})
	switch reuse {
	case sandboxCreateReuseInflight:
		httpx.Logger().Infof(
			"coalesced duplicate sandbox create requestID=%s sandboxID=%s "+
				"name=%s ns=%s tenant=%s",
			requestID, result.instanceID, req.Name, req.Namespace,
			sandboxCreateTenant(ctx.Request, req),
		)
	case sandboxCreateReuseCompleted:
		httpx.Logger().Infof(
			"replayed completed sandbox create requestID=%s sandboxID=%s "+
				"name=%s ns=%s tenant=%s",
			requestID, result.instanceID, req.Name, req.Namespace,
			sandboxCreateTenant(ctx.Request, req),
		)
	}
	if createErr != nil && respondOnError {
		var conflictErr *sandboxCreateRequestConflictError
		var alreadyExistsErr *sandboxAlreadyExistsError
		if errors.As(createErr, &conflictErr) || errors.As(createErr, &alreadyExistsErr) {
			httpx.SetCtxResponse(ctx, nil, http.StatusConflict, createErr)
		} else if result.status == sandboxCreateStatusTimeout {
			httpx.SetCtxResponse(
				ctx, nil, http.StatusInternalServerError,
				fmt.Errorf("sandbox create timeout before running: %w", createErr),
			)
		} else {
			httpx.SetCtxResponse(
				ctx, nil, http.StatusInternalServerError, fmt.Errorf("failed to create sandbox: %w", createErr),
			)
		}
	}
	return result, createErr
}

func sandboxCreateTenant(req *http.Request, createReq CreateRequest) string {
	if tenant := tenantClaim(req); tenant != "" {
		return tenant
	}
	return createReq.Tenant
}

func sandboxCreateSingleflightKey(createReq CreateRequest) string {
	return strings.Join([]string{
		createReq.Namespace,
		createReq.Name,
	}, "\x00")
}

func sandboxCreateRequestIDKey(req *http.Request, createReq CreateRequest, requestID string) string {
	return strings.Join([]string{
		sandboxCreateTenant(req, createReq),
		requestID,
	}, "\x00")
}

func sandboxCreateRequestDigest(
	req CreateRequest,
	idleTimeoutSeconds int,
) ([sha256.Size]byte, error) {
	if req.nameGenerated {
		req.Name = ""
	}
	payload, err := json.Marshal(struct {
		Request            CreateRequest  `json:"request"`
		PortRouteKinds     map[int]string `json:"portRouteKinds,omitempty"`
		IdleTimeoutSeconds int            `json:"idleTimeoutSeconds"`
	}{
		Request:            req,
		PortRouteKinds:     req.portRouteKinds,
		IdleTimeoutSeconds: idleTimeoutSeconds,
	})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("failed to hash sandbox create request: %w", err)
	}
	return sha256.Sum256(payload), nil
}

func sandboxInstanceExists(instanceID string) bool {
	summary, ok := instancecache.Default().GetSummary(instanceID)
	// Terminal summaries can outlive their instance records for diagnostics.
	// Let the authoritative create reject any remaining name conflict.
	return ok && summary.StatusCode != instancecache.StatusFatal &&
		summary.StatusCode != instancecache.StatusScheduleFailed
}

func isSandboxInstanceDuplicated(err error) bool {
	if err == nil {
		return false
	}
	var errInfo operationError
	return errors.As(err, &errInfo) && errInfo.Code == sandboxInstanceDuplicatedCode
}

type sandboxInvocation struct {
	funcID     string
	invokeOpts createOptions
	snapshotID string
	failover   bool
}

func prepareSandboxInvocation(
	ctx *gin.Context,
	req CreateRequest,
	idleTimeoutSeconds int,
	traceID string,
) (sandboxInvocation, error) {
	if req.Name == "" || req.Namespace == "" {
		return sandboxInvocation{}, fmt.Errorf("name and namespace are required")
	}
	rootfs := req.Rootfs
	if rootfs == "" {
		rootfs = req.Image
	}
	funcID, err := sandboxFunctionIDForRuntime(req.Runtime)
	if err != nil {
		return sandboxInvocation{}, err
	}

	traceParent := ctx.Request.Header.Get(httpx.HeaderTraceParent)
	invokeOpts, err := newSandboxInvokeOptions(sandboxInvokeOptionRequest{
		ctx:                ctx,
		createReq:          req,
		rootfs:             rootfs,
		idleTimeoutSeconds: idleTimeoutSeconds,
		traceID:            traceID,
		traceParent:        traceParent,
		funcID:             funcID,
	})
	if err != nil {
		return sandboxInvocation{}, err
	}
	return sandboxInvocation{
		funcID:     funcID,
		invokeOpts: invokeOpts,
		snapshotID: req.SnapshotID,
		failover:   req.Failover,
	}, nil
}

func createSandboxInstanceRaw(
	ctx *gin.Context,
	invocation sandboxInvocation,
	name string,
	namespace string,
) (string, error) {
	createReq, err := buildSandboxRawCreateRequest(invocation, name, namespace)
	if err != nil {
		return "", err
	}
	createReqRaw, err := proto.Marshal(createReq)
	if err != nil {
		return "", fmt.Errorf("failed to marshal sandbox create request: %w", err)
	}
	createCtx := context.WithoutCancel(ctx.Request.Context())
	if invocation.invokeOpts.Timeout > 0 {
		var cancel context.CancelFunc
		createCtx, cancel = context.WithTimeout(
			createCtx,
			time.Duration(invocation.invokeOpts.Timeout)*time.Second,
		)
		defer cancel()
	}
	respRaw, err := createOnBackend(backend.Request{Context: createCtx, Payload: createReqRaw,
		TraceParent: ctx.Request.Header.Get(httpx.HeaderTraceParent), TimeoutSeconds: invocation.invokeOpts.Timeout})
	if err != nil {
		return "", err
	}
	return parseSandboxRawCreateResponse(respRaw, createReq.GetDesignatedInstanceID())
}

func buildSandboxRawCreateRequest(
	invocation sandboxInvocation,
	name string,
	namespace string,
) (*core.CreateRequest, error) {
	invokeOpts := invocation.invokeOpts
	resources := map[string]float64{}
	if invokeOpts.Cpu >= 0 {
		resources["CPU"] = float64(invokeOpts.Cpu)
	}
	if invokeOpts.Memory >= 0 {
		resources["Memory"] = float64(invokeOpts.Memory)
	}
	for key, value := range invokeOpts.CustomResources {
		resources[key] = value
	}

	extensions := cloneStringMap(invokeOpts.CustomExtensions)
	if invokeOpts.CpuLimit != 0 {
		setStringMapDefault(extensions, "CPU_LIMIT", strconv.Itoa(invokeOpts.CpuLimit))
	}
	if invokeOpts.MemoryLimit != 0 {
		setStringMapDefault(extensions, "MEMORY_LIMIT", strconv.Itoa(invokeOpts.MemoryLimit))
	}

	createOptions := cloneStringMap(invokeOpts.CreateOpt)
	setStringMapDefault(createOptions, "DATA_AFFINITY_ENABLED", "false")
	setStringMapDefault(createOptions, "RecoverRetryTimes", strconv.Itoa(invokeOpts.RecoverRetryTimes))
	for key, value := range invokeOpts.CustomExtensions {
		if key == "Concurrency" {
			key = "ConcurrentNum"
		}
		setStringMapDefault(createOptions, key, value)
	}

	scheduleAffinity, err := buildSandboxRawScheduleAffinity(invokeOpts.ScheduleAffinities)
	if err != nil {
		return nil, fmt.Errorf("failed to build sandbox schedule affinity: %w", err)
	}

	return &core.CreateRequest{
		Function: invocation.funcID,
		SchedulingOps: &core.SchedulingOptions{
			Priority:          int32(invokeOpts.Priority),
			Resources:         resources,
			Extension:         extensions,
			ScheduleAffinity:  scheduleAffinity,
			ScheduleTimeoutMs: invokeOpts.ScheduleTimeoutMs,
		},
		RequestID:            newSandboxRawRequestID(),
		TraceID:              invokeOpts.TraceID,
		Labels:               append([]string(nil), invokeOpts.Labels...),
		DesignatedInstanceID: namespace + "-" + name,
		CreateOptions:        createOptions,
		SnapshotID:           strings.TrimSpace(invocation.snapshotID),
		Failover:             invocation.failover,
	}, nil
}

func buildSandboxRawScheduleAffinity(
	scheduleAffinities []Affinity,
) (*affinity.Affinity, error) {
	if len(scheduleAffinities) == 0 {
		return nil, nil
	}
	rawAffinity := &affinity.Affinity{}
	for _, scheduleAffinity := range scheduleAffinities {
		selector, err := rawAffinitySelector(rawAffinity, scheduleAffinity)
		if err != nil {
			return nil, err
		}
		expressions := make([]*affinity.LabelExpression, 0, len(scheduleAffinity.LabelOps))
		for _, labelOp := range scheduleAffinity.LabelOps {
			operator, err := rawLabelOperator(labelOp)
			if err != nil {
				return nil, err
			}
			expressions = append(expressions, &affinity.LabelExpression{
				Key: labelOp.LabelKey,
				Op:  operator,
			})
		}
		if selector.Condition == nil {
			selector.Condition = &affinity.Condition{}
		}
		selector.Condition.OrderPriority = scheduleAffinity.PreferredPriority
		selector.Condition.SubConditions = append(
			selector.Condition.SubConditions,
			&affinity.SubCondition{Expressions: expressions},
		)
	}
	return rawAffinity, nil
}

func rawAffinitySelector(
	rawAffinity *affinity.Affinity,
	scheduleAffinity Affinity,
) (*affinity.Selector, error) {
	switch scheduleAffinity.Kind {
	case AffinityKindResource:
		if rawAffinity.Resource == nil {
			rawAffinity.Resource = &affinity.ResourceAffinity{}
		}
		return resourceAffinitySelector(rawAffinity.Resource, scheduleAffinity)
	case AffinityKindInstance:
		if rawAffinity.Instance == nil {
			rawAffinity.Instance = &affinity.InstanceAffinity{}
		}
		return instanceAffinitySelector(rawAffinity.Instance, scheduleAffinity)
	default:
		return nil, fmt.Errorf("unsupported affinity kind %d", scheduleAffinity.Kind)
	}
}

func resourceAffinitySelector(
	resource *affinity.ResourceAffinity,
	scheduleAffinity Affinity,
) (*affinity.Selector, error) {
	preferRequired := scheduleAffinity.PreferredPriority &&
		scheduleAffinity.PreferredAntiOtherLabels
	switch scheduleAffinity.Affinity {
	case PreferredAffinity:
		if preferRequired {
			return initializeAffinitySelector(&resource.RequiredAffinity), nil
		}
		return initializeAffinitySelector(&resource.PreferredAffinity), nil
	case PreferredAntiAffinity:
		if preferRequired {
			return initializeAffinitySelector(&resource.RequiredAntiAffinity), nil
		}
		return initializeAffinitySelector(&resource.PreferredAntiAffinity), nil
	case RequiredAffinity:
		return initializeAffinitySelector(&resource.RequiredAffinity), nil
	case RequiredAntiAffinity:
		return initializeAffinitySelector(&resource.RequiredAntiAffinity), nil
	default:
		return nil, fmt.Errorf("unsupported resource affinity type %d", scheduleAffinity.Affinity)
	}
}

func instanceAffinitySelector(
	instance *affinity.InstanceAffinity,
	scheduleAffinity Affinity,
) (*affinity.Selector, error) {
	switch scheduleAffinity.Affinity {
	case PreferredAffinity:
		return initializeAffinitySelector(&instance.PreferredAffinity), nil
	case PreferredAntiAffinity:
		return initializeAffinitySelector(&instance.PreferredAntiAffinity), nil
	case RequiredAffinity:
		return initializeAffinitySelector(&instance.RequiredAffinity), nil
	case RequiredAntiAffinity:
		return initializeAffinitySelector(&instance.RequiredAntiAffinity), nil
	default:
		return nil, fmt.Errorf("unsupported instance affinity type %d", scheduleAffinity.Affinity)
	}
}

func initializeAffinitySelector(selector **affinity.Selector) *affinity.Selector {
	if *selector == nil {
		*selector = &affinity.Selector{}
	}
	return *selector
}

func rawLabelOperator(labelOp LabelOperator) (*affinity.LabelOperator, error) {
	operator := &affinity.LabelOperator{}
	switch labelOp.Type {
	case LabelOpIn:
		operator.LabelOperator = &affinity.LabelOperator_In{
			In: &affinity.LabelIn{Values: append([]string(nil), labelOp.LabelValues...)},
		}
	case LabelOpNotIn:
		operator.LabelOperator = &affinity.LabelOperator_NotIn{
			NotIn: &affinity.LabelNotIn{Values: append([]string(nil), labelOp.LabelValues...)},
		}
	case LabelOpExists:
		operator.LabelOperator = &affinity.LabelOperator_Exists{Exists: &affinity.LabelExists{}}
	case LabelOpNotExists:
		operator.LabelOperator = &affinity.LabelOperator_NotExist{
			NotExist: &affinity.LabelDoesNotExist{},
		}
	default:
		return nil, fmt.Errorf("unsupported label operator type %d", labelOp.Type)
	}
	return operator, nil
}

func newSandboxRawRequestID() string {
	random := strings.ReplaceAll(uuid.NewString(), "-", "")
	return random[:sandboxRawRequestIDLength-len(sandboxRawRequestSequence)] + sandboxRawRequestSequence
}

func cloneStringMap(source map[string]string) map[string]string {
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func setStringMapDefault(values map[string]string, key string, value string) {
	if _, exists := values[key]; !exists {
		values[key] = value
	}
}

func parseSandboxRawCreateResponse(raw []byte, designatedInstanceID string) (string, error) {
	var notify runtime.NotifyRequest
	if err := proto.Unmarshal(raw, &notify); err != nil {
		return "", fmt.Errorf("failed to unmarshal sandbox create response: %w", err)
	}
	code := int(notify.GetCode())
	message := notify.GetMessage()
	if code != 0 {
		if message == "" {
			message = fmt.Sprintf("sandbox raw create failed with code %d", code)
		}
		return designatedInstanceID, operationError{Code: code, Err: errors.New(message)}
	}
	return designatedInstanceID, nil
}

func finishSandboxCreate(createCtx sandboxCreateContext, instanceID string, err error) (sandboxCreateResult, error) {
	if err != nil {
		if shouldTreatCreateTimeoutAsSuccess(instanceID, err) {
			if waitForSandboxInstanceRunning(createCtx.context,
				instanceID, createCtx.funcID, createCtx.invokeOpts.CreateOpt[httpx.ResourceSpecNote],
			) {
				httpx.Logger().Infof(
					"sandbox instance reached running state after create timeout "+
						"traceID=%s requestID=%s instanceID=%s name=%s ns=%s",
					createCtx.traceID, createCtx.requestID, instanceID, createCtx.req.Name, createCtx.req.Namespace,
				)
				return sandboxCreateResult{instanceID: instanceID, status: sandboxCreateStatusRunning}, nil
			}
			httpx.Logger().Warnf(
				"sandbox create timed out before running traceID=%s requestID=%s "+
					"instanceID=%s name=%s ns=%s: %v",
				createCtx.traceID, createCtx.requestID, instanceID, createCtx.req.Name, createCtx.req.Namespace, err,
			)
			return sandboxCreateResult{instanceID: instanceID, status: sandboxCreateStatusTimeout}, err
		}
		httpx.Logger().Errorf(
			"failed to create sandbox instance traceID=%s requestID=%s "+
				"instanceID=%s name=%s ns=%s: %v",
			createCtx.traceID, createCtx.requestID, instanceID, createCtx.req.Name, createCtx.req.Namespace, err,
		)
		return sandboxCreateResult{instanceID: instanceID, status: sandboxCreateStatusFailed}, err
	}
	httpx.Logger().Infof(
		"sandbox created traceID=%s requestID=%s instanceID=%s name=%s ns=%s",
		createCtx.traceID, createCtx.requestID, instanceID, createCtx.req.Name, createCtx.req.Namespace,
	)
	return sandboxCreateResult{instanceID: instanceID, status: sandboxCreateStatusRunning}, nil
}

type sandboxInvokeOptionRequest struct {
	ctx                *gin.Context
	createReq          CreateRequest
	rootfs             string
	idleTimeoutSeconds int
	traceID            string
	traceParent        string
	funcID             string
}

func newSandboxInvokeOptions(req sandboxInvokeOptionRequest) (createOptions, error) {
	cpu, memory := createResources(req.createReq)
	xpu, err := parseSandboxXPU(req.createReq.XPU)
	if err != nil {
		return createOptions{}, err
	}
	if err := validateSandboxStorage(req.createReq.StorageMb, req.createReq.StorageLimitMb); err != nil {
		return createOptions{}, err
	}
	createTimeoutSeconds, scheduleTimeoutSeconds, err := resolveSandboxCreateTimeouts(
		req.createReq.CreateTimeoutSeconds,
		req.createReq.ScheduleTimeoutSeconds,
		req.createReq.InitCallTimeoutSeconds,
	)
	if err != nil {
		return createOptions{}, err
	}
	invokeOpts := createOptions{
		TraceID:            req.traceID,
		Cpu:                cpu,
		Memory:             memory,
		CpuLimit:           req.createReq.CpuLimit,
		MemoryLimit:        req.createReq.MemLimit,
		Timeout:            createTimeoutSeconds,
		ScheduleTimeoutMs:  int64(scheduleTimeoutSeconds) * millisecondsPerSecond,
		ScheduleAffinities: req.createReq.ScheduleAffinities,
		CreateOpt:          map[string]string{},
		CustomExtensions: map[string]string{
			"lifecycle":   "detached",
			"Concurrency": sandboxConcurrency,
		},
	}
	if xpu != nil || req.createReq.StorageMb != nil || req.createReq.StorageLimitMb > 0 {
		invokeOpts.CustomResources = make(map[string]float64, 2)
	}
	if xpu != nil {
		invokeOpts.CustomResources[xpu.resourceName] = float64(xpu.count)
	}
	if req.createReq.StorageMb != nil {
		invokeOpts.CustomResources[sandboxStorageResourceName] =
			float64(*req.createReq.StorageMb) * bytesPerMiB
	} else if req.createReq.StorageLimitMb > 0 {
		// A standalone limit also reserves that capacity from the scheduler.
		invokeOpts.CustomResources[sandboxStorageResourceName] =
			float64(req.createReq.StorageLimitMb) * bytesPerMiB
	}
	if err := fillSandboxCustomExtensions(
		&invokeOpts,
		req.createReq,
		req.rootfs,
		req.idleTimeoutSeconds,
		req.traceParent,
	); err != nil {
		return createOptions{}, err
	}
	if err := fillSandboxCreateOptions(req.ctx, &invokeOpts, req.createReq, req.funcID); err != nil {
		return createOptions{}, err
	}
	return invokeOpts, nil
}

func resourceDefaults(cpu, memory int) (int, int) {
	if cpu <= 0 {
		cpu = sandboxDefaultCPU
	}
	if memory <= 0 {
		memory = sandboxDefaultMemory
	}
	return cpu, memory
}

func createResources(req CreateRequest) (int, int) {
	cpu, memory := resourceDefaults(req.Cpu, req.Memory)
	if strings.TrimSpace(req.SnapshotID) == "" {
		return cpu, memory
	}
	// A Create-from-Snapshot request inherits resources that the caller did
	// not specify. Keep the negative sentinel expected by
	// buildSandboxRawCreateRequest instead of turning omission into ordinary
	// sandbox defaults before FunctionSystem can merge the Snapshot template.
	if req.Cpu <= 0 {
		cpu = -1
	}
	if req.Memory <= 0 {
		memory = -1
	}
	return cpu, memory
}

func fillSandboxCustomExtensions(
	invokeOpts *createOptions,
	req CreateRequest,
	rootfs string,
	idleTimeoutSeconds int,
	traceParent string,
) error {
	if traceParent != "" {
		invokeOpts.CustomExtensions["traceparent"] = traceParent
	}
	if idleTimeoutSeconds > 0 {
		invokeOpts.CustomExtensions["idle_timeout"] = strconv.Itoa(idleTimeoutSeconds)
	}
	if req.CpuLimit > 0 {
		invokeOpts.CustomExtensions["CPU_LIMIT"] = strconv.Itoa(req.CpuLimit)
	}
	if req.MemLimit > 0 {
		invokeOpts.CustomExtensions["Memory_LIMIT"] = strconv.Itoa(req.MemLimit)
	}
	if req.StorageLimitMb > 0 {
		invokeOpts.CustomExtensions[sandboxStorageLimitExtension] = strconv.FormatInt(
			req.StorageLimitMb*bytesPerMiB,
			decimalRadix,
		)
	}
	if rootfs != "" {
		invokeOpts.CustomExtensions["rootfs"] = rootfs
	}
	if req.InheritEntrypoint {
		invokeOpts.CustomExtensions["inherit_entrypoint"] = "true"
	}
	if len(req.Mounts) > 0 {
		if mountsJSON, err := json.Marshal(req.Mounts); err == nil {
			invokeOpts.CustomExtensions["mounts"] = string(mountsJSON)
		} else {
			httpx.Logger().Warnf("failed to marshal sandbox mounts: %v", err)
		}
	}
	if len(req.ExtraConfig) > 0 {
		if ecJSON, err := json.Marshal(req.ExtraConfig); err == nil {
			invokeOpts.CustomExtensions["extra_config"] = string(ecJSON)
		} else {
			httpx.Logger().Warnf("failed to marshal sandbox extra_config: %v", err)
		}
	}
	if req.Network != nil {
		networkJSON, err := json.Marshal(req.Network)
		if err != nil {
			return fmt.Errorf("failed to marshal sandbox network policy: %w", err)
		}
		invokeOpts.CustomExtensions["network_policy"] = string(networkJSON)
	}
	if req.DataPlane != nil {
		if req.DataPlane.TunnelSecurityMode != "" {
			invokeOpts.CustomExtensions["data_plane_tunnel_security_mode"] =
				string(req.DataPlane.TunnelSecurityMode)
		}
		if req.DataPlane.PortForwardSecurityMode != "" {
			invokeOpts.CustomExtensions["data_plane_port_forward_security_mode"] =
				string(req.DataPlane.PortForwardSecurityMode)
		}
	}
	return nil
}

func fillSandboxCreateOptions(
	ctx *gin.Context,
	invokeOpts *createOptions,
	req CreateRequest,
	funcID string,
) error {
	if len(req.Env) > 0 {
		if envJSON, err := json.Marshal(req.Env); err == nil {
			invokeOpts.CreateOpt[httpx.DelegateEnvVar] = string(envJSON)
		} else {
			httpx.Logger().Warnf("failed to marshal sandbox env: %v", err)
		}
	}
	if len(req.Ports) > 0 {
		networkConfig, err := buildSandboxNetworkConfig(req.Ports, req.portRouteKinds)
		if err != nil {
			return err
		}
		invokeOpts.CreateOpt["network"] = networkConfig
	}
	tenantID := httpx.GetCompatibleGinHeader(ctx.Request, httpx.HeaderTenantID, "tenantId")
	if tenantID == "" {
		tenantID = req.Tenant
	}
	if tenantID == "" {
		tenantID = tenantClaim(ctx.Request)
	}
	if tenantID != "" {
		invokeOpts.CreateOpt["tenantId"] = tenantID
	}
	invokeOpts.CreateOpt[httpx.FunctionKeyNote] = funcID
	invokeOpts.CreateOpt[httpx.InstanceTypeNote] = sandboxInstanceType
	invokeOpts.CreateOpt[httpx.SchedulerManagedNote] = strconv.FormatBool(false)
	invokeOpts.CreateOpt["call_timeout"] = fmt.Sprintf("%d", invokeOpts.Timeout)
	invokeOpts.CreateOpt["init_call_timeout"] = fmt.Sprintf(
		"%d", effectiveSandboxInitCallTimeoutSeconds(req.InitCallTimeoutSeconds),
	)
	invokeOpts.CreateOpt["GRACEFUL_SHUTDOWN_TIME"] = fmt.Sprintf("%d", sandboxGracefulShutdownSeconds)
	invokeOpts.CreateOpt["DELEGATE_DIRECTORY_INFO"] = sandboxDelegateDirectory
	invokeOpts.CreateOpt["DELEGATE_DIRECTORY_QUOTA"] = fmt.Sprintf("%d", sandboxDirectoryQuotaMB)
	invokeOpts.CreateOpt["ConcurrentNum"] = sandboxConcurrency
	invokeOpts.CreateOpt["moduleName"] = sandboxModuleName
	invokeOpts.CreateOpt["className"] = sandboxClassName
	if resSpecJSON, err := buildSandboxResourceSpecJSON(
		invokeOpts.Cpu, invokeOpts.Memory, invokeOpts.CustomResources,
	); err == nil {
		invokeOpts.CreateOpt[httpx.ResourceSpecNote] = resSpecJSON
	} else {
		httpx.Logger().Warnf("failed to marshal sandbox resource spec: %v", err)
	}
	return nil
}

func getSandboxCreateTimeoutSeconds(requested int) int {
	if requested > 0 {
		return requested
	}
	raw := strings.TrimSpace(os.Getenv("ADX_SANDBOX_CREATE_TIMEOUT"))
	if raw == "" {
		return sandboxCreateTimeoutSeconds
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		httpx.Logger().Warnf(
			"invalid ADX_SANDBOX_CREATE_TIMEOUT=%q, using default %d",
			raw, sandboxCreateTimeoutSeconds,
		)
		return sandboxCreateTimeoutSeconds
	}
	return value
}

func effectiveSandboxInitCallTimeoutSeconds(requested int) int {
	if requested > 0 {
		return requested
	}
	return sandboxDefaultInitTimeoutSeconds
}

func validateSandboxCreateTimeoutInputs(requestedCreate, requestedSchedule, requestedInit int) error {
	if requestedCreate < 0 {
		return fmt.Errorf("createTimeoutSeconds must be a positive integer")
	}
	if requestedSchedule < 0 {
		return fmt.Errorf("scheduleTimeoutSeconds must be a positive integer")
	}
	if requestedInit < 0 {
		return fmt.Errorf("initCallTimeoutSeconds must be a positive integer")
	}
	return nil
}

func resolveSandboxCreateTimeouts(requestedCreate, requestedSchedule, requestedInit int) (int, int, error) {
	if err := validateSandboxCreateTimeoutInputs(requestedCreate, requestedSchedule, requestedInit); err != nil {
		return 0, 0, err
	}
	createTimeout := requestedCreate
	scheduleTimeout := requestedSchedule
	createReserve := effectiveSandboxInitCallTimeoutSeconds(requestedInit) + sandboxScheduleBufferSeconds
	if createTimeout == 0 && scheduleTimeout == 0 {
		scheduleTimeout = sandboxDefaultScheduleSeconds
		createTimeout = getSandboxCreateTimeoutSeconds(0)
		minimumCreateTimeout := scheduleTimeout + createReserve
		if createTimeout < minimumCreateTimeout {
			httpx.Logger().Warnf(
				"ADX_SANDBOX_CREATE_TIMEOUT=%d uses the legacy create budget; expanding it to %d",
				createTimeout, minimumCreateTimeout,
			)
			createTimeout = minimumCreateTimeout
		}
		return createTimeout, scheduleTimeout, nil
	}
	if createTimeout == 0 {
		return scheduleTimeout + createReserve, scheduleTimeout, nil
	}
	if scheduleTimeout == 0 {
		if createTimeout <= sandboxScheduleBufferSeconds {
			return 0, 0, fmt.Errorf(
				"createTimeoutSeconds must be greater than %d", sandboxScheduleBufferSeconds,
			)
		}
		legacyScheduleTimeout := createTimeout - sandboxScheduleBufferSeconds
		return legacyScheduleTimeout + createReserve, legacyScheduleTimeout, nil
	}
	if scheduleTimeout > createTimeout {
		return 0, 0, fmt.Errorf(
			"scheduleTimeoutSeconds must be less than or equal to createTimeoutSeconds",
		)
	}
	remainingCreateBudget := createTimeout - scheduleTimeout
	if remainingCreateBudget < sandboxScheduleBufferSeconds {
		return 0, 0, fmt.Errorf(
			"createTimeoutSeconds - scheduleTimeoutSeconds must be at least %d",
			sandboxScheduleBufferSeconds,
		)
	}
	if remainingCreateBudget < createReserve {
		return scheduleTimeout + createReserve, scheduleTimeout, nil
	}
	return createTimeout, scheduleTimeout, nil
}

func acceptsSandboxCreateEventStream(ctx *gin.Context) bool {
	for _, part := range strings.Split(ctx.GetHeader("Accept"), ",") {
		mediaType := strings.TrimSpace(strings.SplitN(strings.TrimSpace(part), ";", 2)[0])
		if strings.EqualFold(mediaType, httpx.AcceptEventStream) {
			return true
		}
	}
	return false
}

func writeSandboxCreateSSEHeader(ctx *gin.Context) {
	ctx.Header(httpx.ContentTypeHeaderKey, httpx.AcceptEventStream)
	ctx.Header("Cache-Control", "no-cache")
	ctx.Header("Connection", "keep-alive")
	ctx.Header("X-Accel-Buffering", "no")
	ctx.Status(http.StatusOK)
}

func writeSandboxCreateSSEHeartbeat(ctx *gin.Context) error {
	if _, err := ctx.Writer.WriteString(": heartbeat\n\n"); err != nil {
		return err
	}
	ctx.Writer.Flush()
	return nil
}

func serveSandboxCreateHeartbeats(ctx *gin.Context, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(sandboxCreateHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Request.Context().Done():
			return
		case <-ticker.C:
			if err := writeSandboxCreateSSEHeartbeat(ctx); err != nil {
				return
			}
		}
	}
}

func writeSandboxCreateSSEEvent(ctx *gin.Context, event string, data map[string]interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if _, err = ctx.Writer.WriteString(fmt.Sprintf("event: %s\n", event)); err != nil {
		return err
	}
	if _, err = ctx.Writer.WriteString(fmt.Sprintf("data: %s\n\n", payload)); err != nil {
		return err
	}
	ctx.Writer.Flush()
	return nil
}

// tenantClaim recovers the tenant claim for instance ownership metadata
// when frontend JWT middleware is disabled (for example in local/CI smoke).
// This is attribution, not authentication: /direct still sends the original
// token to sandboxRouter, which validates it according to router policy before
// comparing the claim with this stored owner.
func tenantClaim(req *http.Request) string {
	// The registered API middleware replaces this header with the verified tenant.
	// Unverified JWT payloads are never used as an ownership source.
	if req == nil {
		return ""
	}
	return req.Header.Get(httpx.HeaderTenantID)
}

func createErrorCode(err error) int {
	var conflictErr *sandboxCreateRequestConflictError
	if errors.As(err, &conflictErr) {
		return http.StatusConflict
	}
	var alreadyExistsErr *sandboxAlreadyExistsError
	if errors.As(err, &alreadyExistsErr) {
		return sandboxInstanceDuplicatedCode
	}
	var errInfo operationError
	if errors.As(err, &errInfo) {
		return errInfo.Code
	}
	return 0
}

func ensureSandboxTrace(ctx *gin.Context) string {
	traceID := httpx.InitTraceID(ctx)
	ctx.Header(httpx.HeaderTraceID, traceID)
	return traceID
}

func ensureSandboxRequestID(ctx *gin.Context, traceID string) string {
	requestID := strings.TrimSpace(ctx.GetHeader(httpx.HeaderRequestID))
	if requestID == "" {
		requestID = traceID
		ctx.Request.Header.Set(httpx.HeaderRequestID, requestID)
	}
	ctx.Header(httpx.HeaderRequestID, requestID)
	return requestID
}

func normalizeRootfsSpec(spec RootfsSpec, fallbackImage string) (RootfsSpec, string) {
	spec.Runtime = strings.TrimSpace(spec.Runtime)
	spec.Type = strings.TrimSpace(spec.Type)
	spec.Path = strings.TrimSpace(spec.Path)
	spec.ImageURL = strings.TrimSpace(spec.ImageURL)
	fallbackImage = strings.TrimSpace(fallbackImage)
	image := spec.ImageURL
	if image == "" && spec.Type != "local" && spec.Type != "s3" {
		image = fallbackImage
	}
	if spec.Type == "" && image != "" {
		spec.Type = "image"
	}
	if spec.Runtime == "" {
		spec.Runtime = "runsc"
	}
	return spec, image
}

func validateRootfsSpec(spec *RootfsSpec, image string) error {
	switch spec.Type {
	case "":
		if spec.Path != "" || len(spec.StorageInfo) != 0 {
			return fmt.Errorf("rootfs source fields require type")
		}
	case "image":
		return validateImageRootfs(spec, image)
	case "local":
		return validateLocalRootfs(spec)
	case "s3":
		return validateS3Rootfs(spec)
	default:
		return fmt.Errorf("unsupported rootfs type %q", spec.Type)
	}
	return nil
}

func validateImageRootfs(spec *RootfsSpec, image string) error {
	if image == "" {
		return fmt.Errorf("image rootfs requires imageurl")
	}
	if spec.Path != "" || len(spec.StorageInfo) != 0 {
		return fmt.Errorf("image rootfs cannot contain path or storageInfo")
	}
	spec.ImageURL = image
	return nil
}

func validateLocalRootfs(spec *RootfsSpec) error {
	if spec.Path == "" {
		return fmt.Errorf("local rootfs requires path")
	}
	if spec.ImageURL != "" || len(spec.StorageInfo) != 0 {
		return fmt.Errorf("local rootfs cannot contain imageurl or storageInfo")
	}
	return nil
}

func validateS3Rootfs(spec *RootfsSpec) error {
	if spec.Path != "" || spec.ImageURL != "" {
		return fmt.Errorf("s3 rootfs cannot contain path or imageurl")
	}
	for _, key := range []string{"endpoint", "bucket", "object"} {
		value, ok := spec.StorageInfo[key].(string)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("s3 rootfs requires non-empty storageInfo.%s", key)
		}
		spec.StorageInfo[key] = strings.TrimSpace(value)
	}
	for _, key := range []string{"accessKey", "secretKey"} {
		value, exists := spec.StorageInfo[key]
		if exists {
			if _, ok := value.(string); !ok {
				return fmt.Errorf("storageInfo.%s must be a string", key)
			}
		}
	}
	return nil
}

func buildRootfsOption(spec RootfsSpec, fallbackImage string) (string, error) {
	spec, image := normalizeRootfsSpec(spec, fallbackImage)
	if err := validateRootfsSpec(&spec, image); err != nil {
		return "", err
	}

	data, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// InvokeV1Handler handles POST /api/sandbox/v1/sandboxes/{sandboxID}/invoke.
func InvokeV1Handler(ctx *gin.Context) {
	traceID := ensureSandboxTrace(ctx)
	instanceID := ctx.Param("sandboxID")
	if instanceID == "" {
		instanceID = ctx.Param("sandboxId")
	}
	if instanceID == "" {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("sandboxID is required"))
		return
	}
	var req InvokeV1Request
	if err := ctx.ShouldBindJSON(&req); err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("invalid request body: %v", err))
		return
	}
	req.Action = strings.TrimSpace(req.Action)
	if req.Action == "" {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("action is required"))
		return
	}
	if req.Args == nil {
		req.Args = map[string]interface{}{}
	}
	if needsAuth, status, err := ensureDeleteJWTContext(ctx, traceID); err != nil {
		httpx.SetCtxResponse(ctx, nil, status, err)
		return
	} else if needsAuth {
		if status, err := authorizeSandboxDelete(ctx, instanceID); err != nil {
			httpx.SetCtxResponse(ctx, nil, status, err)
			return
		}
	}
	result, err := invokeSandboxAction(invokeActionRequest{
		ctx:         ctx.Request.Context(),
		instanceID:  instanceID,
		action:      req.Action,
		args:        req.Args,
		timeout:     sandboxInvokeTimeoutSeconds,
		traceID:     traceID,
		traceParent: ctx.Request.Header.Get(httpx.HeaderTraceParent),
	})
	if err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError, err)
		return
	}
	httpx.SetCtxResponse(ctx, result, http.StatusOK, responseHasError(result))
}

type invokeActionRequest struct {
	ctx         context.Context
	instanceID  string
	action      string
	args        map[string]interface{}
	timeout     int
	traceID     string
	traceParent string
}

func invokeSandboxAction(req invokeActionRequest) (interface{}, error) {
	envelope := map[string]interface{}{
		"sandbox_method": "sandbox_invoke",
		"action":         req.action,
		"args":           normalizeJSONValue(req.args),
	}
	packedArg, err := encodeInlineArg(envelope)
	if err != nil {
		return nil, err
	}
	invokeReq := &core.InvokeRequest{
		Function: defaultSandboxFunctionID,
		Args: []*common.Arg{{
			Type:       common.Arg_ArgType(packedArg.Type),
			Value:      packedArg.Data,
			NestedRefs: packedArg.NestedObjectIDs,
		}},
		InstanceID: req.instanceID,
		RequestID:  newSandboxRawRequestID(),
		TraceID:    req.traceID,
	}
	invokeReqRaw, err := proto.Marshal(invokeReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal sandbox invoke request: %w", err)
	}
	parent := req.ctx
	if parent == nil {
		parent = context.Background()
	}
	if req.timeout > 0 {
		var cancel context.CancelFunc
		parent, cancel = context.WithTimeout(parent, time.Duration(req.timeout)*time.Second)
		defer cancel()
	}
	respRaw, err := invokeOnBackend(backend.Request{Context: parent, Payload: invokeReqRaw, TraceParent: req.traceParent})
	if err != nil {
		return nil, err
	}
	return parseSandboxRawInvokeResponse(respRaw)
}

func parseSandboxRawInvokeResponse(raw []byte) (interface{}, error) {
	var notify runtime.NotifyRequest
	if err := proto.Unmarshal(raw, &notify); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sandbox invoke response: %w", err)
	}
	code := int(notify.GetCode())
	if code != 0 {
		message := notify.GetMessage()
		if message == "" {
			message = fmt.Sprintf("sandbox raw invoke failed with code %d", code)
		}
		return nil, operationError{Code: code, Err: errors.New(message)}
	}
	if len(notify.GetSmallObjects()) != 1 {
		return nil, fmt.Errorf(
			"sandbox direct invoke requires exactly one inline result, got %d; ObjectRef and multiple results are not supported",
			len(notify.GetSmallObjects()))
	}
	return decodeYRValue(notify.GetSmallObjects()[0].GetValue())
}

func normalizeJSONValue(value interface{}) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		m := make(map[string]interface{}, len(v))
		for key, val := range v {
			m[key] = normalizeJSONValue(val)
		}
		return m
	case []interface{}:
		for i := range v {
			v[i] = normalizeJSONValue(v[i])
		}
		return v
	case float64:
		if math.Trunc(v) == v && v >= math.MinInt64 && v <= math.MaxInt64 {
			return int64(v)
		}
		return v
	default:
		return v
	}
}

var msgpackHandle codec.MsgpackHandle

func encodeMsgpack(value interface{}) ([]byte, error) {
	var out []byte
	enc := codec.NewEncoderBytes(&out, &msgpackHandle)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return out, nil
}

func encodeInlineArg(value interface{}) (inlineArg, error) {
	msg, err := encodeMsgpack(value)
	if err != nil {
		return inlineArg{}, err
	}
	// The inline-value header carries the msgpack payload length at buf[8:16]
	// as a FIXED-WIDTH little-endian uint64 (the libruntime DataObject
	// convention), NOT a variable-length msgpack int. Encoding it as msgpack
	// happens to work only while len(msg) < 128 (the fixint byte equals the
	// raw LE byte); at len(msg) >= 128 msgpack prepends a 0xcc marker, so a
	// fixed-width reader gets a corrupt size and the payload is dropped (this
	// is the "data is required" failure on WS uploads of chunks > ~125 bytes).
	buf := make([]byte, inlineHeaderSize+len(msg))
	binary.LittleEndian.PutUint64(
		buf[inlineValueLengthOffset:inlineHeaderSize],
		uint64(len(msg)),
	)
	copy(buf[inlineHeaderSize:], msg)
	return inlineArg{Type: inlineValue, Data: buf}, nil
}

func decodeYRValue(data []byte) (interface{}, error) {
	candidates := make([][]byte, 0, portForwardingFormatParts)
	if len(data) >= inlineHeaderSize && isZeroHeader(data[:inlineHeaderSize]) {
		candidates = append(candidates, data[inlineHeaderSize:])
	} else {
		candidates = append(candidates, data)
		if len(data) >= inlineHeaderSize {
			candidates = append(candidates, data[inlineHeaderSize:])
		}
	}

	var lastErr error
	for _, candidate := range candidates {
		var out interface{}
		dec := codec.NewDecoderBytes(candidate, &msgpackHandle)
		if err := dec.Decode(&out); err != nil {
			lastErr = err
			continue
		}
		return normalizeMsgpack(out), nil
	}
	return nil, lastErr
}

func isZeroHeader(header []byte) bool {
	for _, b := range header {
		if b != 0 {
			return false
		}
	}
	return true
}

func normalizeMsgpack(value interface{}) interface{} {
	switch v := value.(type) {
	case map[interface{}]interface{}:
		m := make(map[string]interface{}, len(v))
		for key, val := range v {
			m[fmt.Sprint(key)] = normalizeMsgpack(val)
		}
		return m
	case map[string]interface{}:
		m := make(map[string]interface{}, len(v))
		for key, val := range v {
			m[key] = normalizeMsgpack(val)
		}
		return m
	case []interface{}:
		for i := range v {
			v[i] = normalizeMsgpack(v[i])
		}
		return v
	case []uint8:
		if utf8.Valid(v) {
			return string(v)
		}
		return v
	default:
		return v
	}
}

func responseHasError(value interface{}) error {
	if m, ok := value.(map[string]interface{}); ok {
		if errVal, exists := m["error"]; exists && errVal != nil && fmt.Sprint(errVal) != "" {
			return fmt.Errorf("%v", errVal)
		}
	}
	return nil
}

func sandboxFunctionIDForRuntime(runtime string) (string, error) {
	selectedRuntime := strings.TrimSpace(runtime)
	if selectedRuntime == "" {
		selectedRuntime = defaultSandboxRuntime
	}

	switch strings.ToLower(selectedRuntime) {
	case "python3.10", "py3.10", "py310", "3.10":
		return "default/0-defaultservice-py310/$latest", nil
	case "python3.9", "py3.9", "py39", "3.9":
		return "default/0-defaultservice-py39/$latest", nil
	case "rust", "rrt", "rrt-runtime":
		// Dedicated Rust sandbox backend (native rrt-runtime). Selected
		// explicitly via the create API's runtime field, decoupled from the
		// python-version slots. See the "rrt" function in services.yaml.
		return "default/0-defaultservice-rrt/$latest", nil
	default:
		return "", fmt.Errorf("unsupported sandbox runtime %q", selectedRuntime)
	}
}

func shouldTreatCreateTimeoutAsSuccess(instanceID string, err error) bool {
	if instanceID == "" || err == nil {
		return false
	}

	var errInfo operationError
	if !errors.As(err, &errInfo) {
		return false
	}

	return errInfo.Code == createTimeoutSuccessCode
}

func buildSandboxResourceSpecJSON(
	cpu, memory int,
	customResources map[string]float64,
) (string, error) {
	var resourceCounts map[string]int64
	if len(customResources) != 0 {
		resourceCounts = make(map[string]int64, len(customResources))
		for name, count := range customResources {
			resourceCounts[name] = int64(count)
		}
	}
	resourceSpec := sandboxResourceSpec{
		CPU:             int64(cpu),
		Memory:          int64(memory),
		InvokeLabel:     "",
		CustomResources: resourceCounts,
	}
	data, err := json.Marshal(resourceSpec)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

type sandboxPortForwarding struct {
	Port      int    `json:"port"`
	Protocol  string `json:"protocol"`
	RouteKind string `json:"routeKind"`
}

const (
	sandboxRoutePublic = "public"
	sandboxRouteDirect = "direct"
	sandboxRouteTunnel = "tunnel"
)

func buildSandboxNetworkConfig(ports []string, routeKinds map[int]string) (string, error) {
	// The sandbox data plane is L7-only (sandboxrouter is an HTTP/WS reverse
	// proxy; there is no L4 routing). So a forwarded port declares its L7
	// SCHEME ("http"/"https"), NOT a transport protocol — the scheme flows
	// into portForward so the router knows how to speak to the backend. The
	// L4 host<->container mapping is always TCP (http/https ride TCP); the
	// runtime-launcher maps the scheme to a TCP docker port binding.
	portForwardings := make([]sandboxPortForwarding, 0, len(ports))
	for _, portForward := range ports {
		scheme := "http"
		portString := strings.TrimSpace(portForward)
		parts := strings.Split(portString, ":")
		switch len(parts) {
		case 1:
			portString = strings.TrimSpace(parts[0])
		case portForwardingFormatParts:
			scheme = strings.ToLower(strings.TrimSpace(parts[0]))
			portString = strings.TrimSpace(parts[1])
		default:
			return "", fmt.Errorf("invalid port forwarding format %q, expected PORT or http|https:PORT", portForward)
		}

		port, err := strconv.Atoi(portString)
		if err != nil {
			return "", fmt.Errorf("invalid port number %q", portString)
		}
		if port < 1 || port > maxSandboxPort {
			return "", fmt.Errorf("port must be in [1, %d], got %d", maxSandboxPort, port)
		}
		if scheme != "http" && scheme != "https" {
			return "", fmt.Errorf("port scheme must be http or https, got %s", scheme)
		}
		portForwardings = append(portForwardings, sandboxPortForwarding{
			Port:      port,
			Protocol:  scheme,
			RouteKind: portRouteKind(port, routeKinds),
		})
	}

	networkConfig := map[string][]sandboxPortForwarding{
		"portForwardings": portForwardings,
	}
	data, err := json.Marshal(networkConfig)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func portRouteKind(port int, routeKinds map[int]string) string {
	if routeKind, ok := routeKinds[port]; ok {
		return routeKind
	}
	return sandboxRoutePublic
}

func isSandboxInstanceRunning(ctx context.Context, instanceID, functionID, resourceSpecNote string) bool {
	d := backend.Current(ctx)
	return d.Instances != nil && d.Instances.IsRunning(instanceID)
}

// DeleteHandler handles DELETE /api/sandbox/:instanceId.
// It confirms deletion of failed instances or sends a kill signal to the owner.
func DeleteHandler(ctx *gin.Context) {
	traceID := ensureSandboxTrace(ctx)
	requestID := ensureSandboxRequestID(ctx, traceID)
	instanceID := ctx.Param("instanceId")
	if instanceID == "" {
		instanceID = ctx.Param("sandboxID")
	}
	if instanceID == "" {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("instanceId is required"))
		return
	}
	needsAuth, errCode, authErr := ensureDeleteJWTContext(ctx, ctx.GetHeader(httpx.HeaderTraceID))
	if authErr != nil {
		httpx.Logger().Warnf("reject sandbox delete instanceID=%s: %v", instanceID, authErr)
		httpx.SetCtxResponse(ctx, nil, errCode, authErr)
		return
	}
	if needsAuth {
		if errCode, err := authorizeSandboxDelete(ctx, instanceID); err != nil {
			httpx.Logger().Warnf("reject sandbox delete instanceID=%s: %v", instanceID, err)
			httpx.SetCtxResponse(ctx, nil, errCode, err)
			return
		}
	}

	// Keep the failure history for diagnostics; only authority can establish
	// idempotent success. Authorization above still applies to that history.
	if summary, ok := instancecache.Default().GetSummary(instanceID); ok &&
		(summary.StatusCode == instancecache.StatusFatal || summary.StatusCode == instancecache.StatusFailed) {
		deleted, err := confirmSandboxInstanceDeleted(ctx.Request.Context(), instanceID)
		if err != nil {
			httpx.SetCtxResponse(ctx, nil, http.StatusServiceUnavailable,
				fmt.Errorf("failed to confirm sandbox deletion: %w", err))
			return
		}
		if deleted {
			httpx.SetCtxResponse(ctx, map[string]string{"status": "deleted"}, http.StatusOK, nil)
			return
		}
	}

	tenantID := httpx.GetCompatibleGinHeader(ctx.Request, httpx.HeaderTenantID, "tenantId")
	if tenantID == "" {
		tenantID = "default"
	}
	invokeOpts := createOptions{TraceID: traceID}
	killRequest := newLifecycleRequest(
		ctx.Request.Context(), instanceID, sandboxKillInstanceSignal, []byte("sandbox deleted"), tenantID, invokeOpts,
	)
	killRequest.RequestID = requestID
	if err := deleteOnBackend(killRequest); err != nil {
		httpx.Logger().Errorf("failed to kill sandbox instance %s: %v", instanceID, err)
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError, fmt.Errorf("failed to delete sandbox: %w", err))
		return
	}

	httpx.SetCtxResponse(ctx, map[string]string{"status": "deleted"}, http.StatusOK, nil)
}

// PauseV1Handler synchronously pauses one sandbox through signal 18.
func PauseV1Handler(ctx *gin.Context) {
	instanceID := strings.TrimSpace(ctx.Param("sandboxID"))
	runningSummary, hadRunningSummary := instancecache.Default().GetSummary(instanceID)
	var req pauseV1Request
	if err := ctx.ShouldBindJSON(&req); err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, fmt.Errorf("invalid request body: %v", err))
		return
	}
	if req.TTLSeconds == 0 {
		req.TTLSeconds = sandboxPauseDefaultTTLSeconds
	}
	if req.TTLSeconds < 0 {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, errors.New("ttlSeconds must be positive"))
		return
	}
	timeoutSeconds, err := resolveSandboxCheckpointTimeout(req.TimeoutSeconds)
	if err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusBadRequest, err)
		return
	}
	payload, err := proto.Marshal(&core.SnapOptions{
		Type:                common.SnapType_PAUSE_RESUME,
		Ttl:                 req.TTLSeconds,
		CheckpointTimeoutMs: uint64(timeoutSeconds) * uint64(time.Second/time.Millisecond),
	})
	if err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError, err)
		return
	}
	killResponse, err := executeSandboxLifecycleKillWithTimeout(
		ctx, lifecycleKillOptions{
			signal:           sandboxPauseInstanceSignal,
			payload:          payload,
			requestIDPattern: sandboxPauseRequestIDPattern,
			operation:        "pause",
			timeoutSeconds:   timeoutSeconds,
		},
	)
	if err != nil {
		setSandboxLifecycleError(ctx, err)
		return
	}
	var snapInfo core.SnapInfo
	if err := proto.Unmarshal(killResponse.GetPayload(), &snapInfo); err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError, fmt.Errorf("invalid pause response: %v", err))
		return
	}
	requestID := strings.TrimSpace(ctx.GetHeader(sandboxLifecycleRequestIDHeader))
	if snapInfo.GetSnapshotID() != requestID || snapInfo.GetSize() <= 0 {
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError,
			fmt.Errorf("invalid pause response identity or size"))
		return
	}
	if hadRunningSummary {
		runningSummary.StatusCode = instancecache.StatusPaused
		runningSummary.StatusMsg = "paused"
		runningSummary.NodeID = instancecache.InstanceManagerOwner
		runningSummary.ContainerID = ""
		runningSummary.ContainerIP = ""
		instancecache.Default().PutSummary(runningSummary)
	}
	httpx.SetCtxResponse(ctx, pauseV1Response{
		SandboxID:  instanceID,
		SnapshotID: snapInfo.GetSnapshotID(),
		Size:       snapInfo.GetSize(),
		State:      "paused",
		ExpiresAt:  time.Now().Unix() + int64(req.TTLSeconds),
	}, http.StatusOK, nil)
}

// ResumeV1Handler synchronously resumes one sandbox through signal 19.
func ResumeV1Handler(ctx *gin.Context) {
	instanceID := strings.TrimSpace(ctx.Param("sandboxID"))
	payload, err := proto.Marshal(&core.SnapStartOptions{Type: common.SnapType_PAUSE_RESUME})
	if err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError, err)
		return
	}
	killResponse, err := executeSandboxLifecycleKill(
		ctx,
		sandboxResumeInstanceSignal,
		payload,
		sandboxResumeRequestIDPattern,
		"resume",
	)
	if err != nil {
		setSandboxLifecycleError(ctx, err)
		return
	}
	var started core.SnapStartedInfo
	if err := proto.Unmarshal(killResponse.GetPayload(), &started); err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError, fmt.Errorf("invalid resume response: %v", err))
		return
	}
	if started.GetInstanceID() != instanceID || started.GetRouteAddress() == "" ||
		started.GetFunctionProxyID() == "" {
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError,
			fmt.Errorf("invalid resume response identity or route"))
		return
	}
	portMappings, err := parseResumePortMappings(started.GetPortMappings())
	if err != nil {
		httpx.SetCtxResponse(ctx, nil, http.StatusInternalServerError, fmt.Errorf("invalid resume port mappings: %v", err))
		return
	}
	// SandboxRouter caches converge independently through ETCD watch/read-through;
	// local route publication is not part of the lifecycle success boundary.
	httpx.SetCtxResponse(ctx, resumeV1Response{
		SandboxID:       started.GetInstanceID(),
		State:           "running",
		RouteAddress:    started.GetRouteAddress(),
		FunctionProxyID: started.GetFunctionProxyID(),
		NodeID:          started.GetNodeID(),
		PortMappings:    portMappings,
	}, http.StatusOK, nil)
}

// ReloadV1Handler restores one running sandbox from its latest local anonymous snapshot.
func ReloadV1Handler(ctx *gin.Context) {
	_, err := executeSandboxLifecycleKill(
		ctx,
		sandboxReloadInstanceSignal,
		nil,
		sandboxReloadRequestIDPattern,
		"reload",
	)
	if err != nil {
		httpx.SetCtxResponse(ctx, reloadV1Response{Success: false}, lifecycleHTTPStatus(err), err)
		return
	}
	httpx.SetCtxResponse(ctx, reloadV1Response{Success: true}, http.StatusOK, nil)
}

// UpdateNetworkV1Handler atomically replaces a running sandbox's network policy.
func UpdateNetworkV1Handler(ctx *gin.Context) {
	var requested SandboxNetworkPolicy
	decoder := json.NewDecoder(ctx.Request.Body)
	if err := decoder.Decode(&requested); err != nil {
		httpx.SetCtxResponse(ctx, updateNetworkV1Response{Success: false},
			http.StatusBadRequest, fmt.Errorf("invalid network policy: %w", err))
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		httpx.SetCtxResponse(ctx, updateNetworkV1Response{Success: false},
			http.StatusBadRequest, errors.New("invalid trailing network policy data"))
		return
	}
	normalized, err := normalizeSandboxNetworkPolicy(&requested)
	if err != nil {
		httpx.SetCtxResponse(ctx, updateNetworkV1Response{Success: false},
			http.StatusBadRequest, err)
		return
	}
	payload := []byte("{}")
	if normalized != nil {
		payload, err = json.Marshal(normalized)
		if err != nil {
			httpx.SetCtxResponse(ctx, updateNetworkV1Response{Success: false},
				http.StatusInternalServerError, err)
			return
		}
	}
	_, err = executeSandboxLifecycleKill(
		ctx,
		sandboxUpdateNetworkSignal,
		payload,
		sandboxNetworkRequestIDPattern,
		"network",
	)
	if err != nil {
		httpx.SetCtxResponse(ctx, updateNetworkV1Response{Success: false},
			lifecycleHTTPStatus(err), err)
		return
	}
	httpx.SetCtxResponse(ctx, updateNetworkV1Response{Success: true}, http.StatusOK, nil)
}

func parseResumePortMappings(encoded string) (map[string]int, error) {
	const physicalMappingFieldCount = 3

	result := map[string]int{}
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return result, nil
	}
	if strings.HasPrefix(encoded, "{") {
		if err := json.Unmarshal([]byte(encoded), &result); err != nil {
			return nil, err
		}
		return result, nil
	}
	var physical []string
	if err := json.Unmarshal([]byte(encoded), &physical); err != nil {
		return nil, err
	}
	for _, mapping := range physical {
		parts := strings.Split(mapping, ":")
		if len(parts) != physicalMappingFieldCount {
			return nil, fmt.Errorf("malformed physical mapping %q", mapping)
		}
		hostPort, err := strconv.Atoi(parts[1])
		if err != nil || hostPort <= 0 || hostPort > maxSandboxPort {
			return nil, fmt.Errorf("invalid host port in mapping %q", mapping)
		}
		containerPort, err := strconv.Atoi(parts[2])
		if err != nil || containerPort <= 0 || containerPort > maxSandboxPort {
			return nil, fmt.Errorf("invalid container port in mapping %q", mapping)
		}
		key := strconv.Itoa(containerPort)
		if previous, exists := result[key]; exists && previous != hostPort {
			return nil, fmt.Errorf("conflicting host ports for container port %d", containerPort)
		}
		result[key] = hostPort
	}
	return result, nil
}

func executeSandboxLifecycleKill(
	ctx *gin.Context,
	signal int,
	payload []byte,
	requestIDPattern *regexp.Regexp,
	operation string,
) (*core.KillResponse, error) {
	return executeSandboxLifecycleKillWithTimeout(
		ctx, lifecycleKillOptions{
			signal:           signal,
			payload:          payload,
			requestIDPattern: requestIDPattern,
			operation:        operation,
		},
	)
}

type lifecycleKillOptions struct {
	signal           int
	payload          []byte
	requestIDPattern *regexp.Regexp
	operation        string
	timeoutSeconds   int
}

func executeSandboxLifecycleKillWithTimeout(
	ctx *gin.Context,
	options lifecycleKillOptions,
) (*core.KillResponse, error) {
	instanceID := strings.TrimSpace(ctx.Param("sandboxID"))
	if instanceID == "" {
		return nil, errors.New("sandboxID is required")
	}
	requestID := strings.TrimSpace(ctx.GetHeader(sandboxLifecycleRequestIDHeader))
	if !options.requestIDPattern.MatchString(requestID) {
		return nil, fmt.Errorf("%s must contain a valid SDK %s request ID",
			sandboxLifecycleRequestIDHeader, options.operation)
	}
	needsAuth, statusCode, err := ensureDeleteJWTContext(ctx, ctx.GetHeader(httpx.HeaderTraceID))
	if err != nil {
		return nil, &sandboxLifecycleHTTPError{status: statusCode, cause: err}
	}
	if needsAuth {
		if statusCode, err := authorizeSandboxDelete(ctx, instanceID); err != nil {
			return nil, &sandboxLifecycleHTTPError{status: statusCode, cause: err}
		}
	}
	tenantID := httpx.GetCompatibleGinHeader(ctx.Request, httpx.HeaderTenantID, "tenantId")
	if tenantID == "" {
		tenantID = "default"
	}
	invokeOptions := createOptions{
		TraceID: ctx.GetHeader(httpx.HeaderTraceID),
		Timeout: options.timeoutSeconds,
		CustomExtensions: map[string]string{
			"traceparent": ctx.GetHeader(httpx.HeaderTraceParent),
		},
	}
	killRequest := newLifecycleRequest(
		ctx.Request.Context(), instanceID, options.signal, options.payload, tenantID, invokeOptions)
	killRequest.RequestID = requestID
	response, err := lifecycleOnBackend(killRequest)
	if err != nil {
		return nil, &sandboxLifecycleTransportError{cause: err}
	}
	if response.GetCode() != common.ErrorCode_ERR_NONE {
		return nil, &sandboxLifecycleBusinessError{
			operation: options.operation,
			code:      response.GetCode(),
			message:   response.GetMessage(),
		}
	}
	return response, nil
}

func setSandboxLifecycleError(ctx *gin.Context, err error) {
	httpx.SetCtxResponse(ctx, nil, lifecycleHTTPStatus(err), err)
}

func lifecycleHTTPStatus(err error) int {
	if status.Code(err) == codes.Unimplemented {
		return http.StatusNotImplemented
	}

	statusCode := http.StatusInternalServerError
	var transportError *sandboxLifecycleTransportError
	var businessError *sandboxLifecycleBusinessError
	var httpError *sandboxLifecycleHTTPError
	if errors.As(err, &httpError) {
		statusCode = httpError.status
	} else if errors.As(err, &transportError) {
		statusCode = http.StatusServiceUnavailable
	} else if errors.As(err, &businessError) {
		statusCode = http.StatusConflict
	} else if strings.Contains(err.Error(), sandboxLifecycleRequestIDHeader) ||
		strings.Contains(err.Error(), "required") ||
		strings.Contains(err.Error(), "authorized") ||
		strings.Contains(err.Error(), "tenant") {
		statusCode = http.StatusBadRequest
	}
	return statusCode
}

func needsDeleteAuthorization(ctx *gin.Context) bool {
	_, hasSub := ctx.Get("jwt_sub")
	_, hasRole := ctx.Get("jwt_role")
	return hasSub || hasRole || ctx.GetHeader("X-Auth-Token") != ""
}

func ensureDeleteJWTContext(ctx *gin.Context, traceID string) (bool, int, error) {
	if !needsDeleteAuthorization(ctx) {
		return false, http.StatusOK, nil
	}
	if _, hasSub := ctx.Get("jwt_sub"); hasSub {
		if _, hasRole := ctx.Get("jwt_role"); hasRole {
			return true, http.StatusOK, nil
		}
	}
	token := ctx.GetHeader("X-Auth-Token")
	d := backend.Current(ctx.Request.Context())
	if d.Authenticate == nil {
		return true, http.StatusServiceUnavailable, backend.ErrUnavailable
	}
	identity, err := d.Authenticate(ctx.Request.Context(), token)
	errCode := http.StatusForbidden
	if err != nil {
		return true, errCode, err
	}
	ctx.Set("jwt_sub", identity.TenantID)
	ctx.Set("jwt_role", identity.Role)
	return true, http.StatusOK, nil
}

func authorizeSandboxDelete(ctx *gin.Context, instanceID string) (int, error) {
	callerTenant, _ := ctx.Get("jwt_sub")
	callerRole, _ := ctx.Get("jwt_role")
	callerTenantID, ok := callerTenant.(string)
	if !ok || callerTenantID == "" {
		return http.StatusForbidden, errors.New("missing caller tenant in JWT context")
	}
	callerRoleName, ok := callerRole.(string)
	if !ok || callerRoleName == "" {
		return http.StatusForbidden, errors.New("missing caller role in JWT context")
	}
	if callerRoleName == backend.RoleAdmin {
		return http.StatusOK, nil
	}
	if callerRoleName != backend.RoleTenant {
		return http.StatusForbidden, errors.New("caller role is not authorized to delete instances")
	}
	targetTenantID := ""
	if summary, ok := instancecache.Default().GetSummary(instanceID); ok {
		targetTenantID = summary.TenantID
	} else {
		instance, err := readAuthoritativeSandboxInstance(ctx.Request.Context(), instanceID)
		if err != nil {
			if status.Code(err) == codes.PermissionDenied {
				return http.StatusForbidden, err
			}
			if errors.Is(err, backend.ErrInstanceNotFound) {
				return http.StatusNotFound, errors.New("authoritative instance not found")
			}
			return http.StatusServiceUnavailable,
				fmt.Errorf("failed to read authoritative instance ownership: %w", err)
		}
		if instance == nil {
			return http.StatusServiceUnavailable, errors.New("missing authoritative instance response")
		}
		targetTenantID = instance.TenantID
	}
	if callerTenantID == targetTenantID {
		return http.StatusOK, nil
	}
	return http.StatusForbidden, errors.New("caller tenant is not authorized to delete target instance")
}
