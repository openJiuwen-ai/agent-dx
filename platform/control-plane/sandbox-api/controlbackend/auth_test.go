package controlbackend

import (
	"context"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
	"time"
)

type authFake struct {
	pb.AuthServiceClient
	calls   int
	expires uint64
	fail    bool
}

func (f *authFake) VerifyApiKey(context.Context, *pb.VerifyApiKeyRequest, ...grpc.CallOption) (*pb.VerifyApiKeyResponse, error) {
	f.calls++
	if f.fail {
		return nil, status.Error(codes.Unavailable, "Master offline")
	}
	return &pb.VerifyApiKeyResponse{Caller: &pb.CallerContext{TenantId: "tenant"}, ExpiresAtUnixSeconds: f.expires}, nil
}
func TestAuthCacheContinuesDuringOutageButRejectsExpiredCredentials(t *testing.T) {
	f := &authFake{}
	a, _ := NewAuthenticator(f, time.Minute, time.Second, 2)
	key := strings.Repeat("a", 40)
	if _, err := a.Verify(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	f.fail = true
	if _, err := a.Verify(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 {
		t.Fatal("cache hit contacted Master")
	}
	if _, err := a.Verify(context.Background(), strings.Repeat("b", 40)); status.Code(err) != codes.Unavailable {
		t.Fatalf("missing credential cache: %v", err)
	}
	f.fail = false
	f.expires = uint64(time.Now().Add(-time.Second).Unix())
	if _, err := a.Verify(context.Background(), strings.Repeat("b", 40)); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expired key accepted: %v", err)
	}
}
