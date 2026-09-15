package discovery

import (
	"context"
	"errors"
	"google.golang.org/grpc/resolver"
	"testing"
	"time"
)

func TestDiscoveryContract(t *testing.T) {
	good := []interface{}{`{"schema":1,"epoch":9007199254740993,"address":"https://localhost:9000"}`, `{"epoch":9007199254740993}`}
	address, err := decode(good)
	if err != nil || address != "localhost:9000" {
		t.Fatal(address, err)
	}
	for _, v := range [][]interface{}{{nil, good[1]}, {good[0], `{"epoch":9007199254740992}`}, {`{"schema":1,"epoch":1,"address":"http://localhost:9000"}`, `{"epoch":1}`}} {
		if _, err := decode(v); err == nil {
			t.Fatal("accepted invalid discovery", v)
		}
	}
}

type conn struct {
	resolver.ClientConn
	updates  chan string
	failures chan error
}

func (c *conn) UpdateState(s resolver.State) error { c.updates <- s.Addresses[0].Addr; return nil }
func (c *conn) ReportError(e error)                { c.failures <- e }
func TestResolverMovesToNewMasterAndReportsOutage(t *testing.T) {
	values := make(chan string, 3)
	values <- "localhost:1"
	values <- ""
	values <- "localhost:2"
	b := &Builder{interval: time.Hour, timeLimit: time.Second, lookup: func(ctx context.Context) (string, error) {
		select {
		case s := <-values:
			if s == "" {
				return "", errors.New("down")
			}
			return s, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}}
	c := &conn{updates: make(chan string, 3), failures: make(chan error, 3)}
	r, err := b.Build(resolver.Target{}, c, resolver.BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, want := range []string{"localhost:1", "error", "localhost:2"} {
		if want == "error" {
			select {
			case <-c.failures:
			case <-time.After(time.Second):
				t.Fatal("missing outage")
			}
		} else {
			select {
			case got := <-c.updates:
				if got != want {
					t.Fatal(got)
				}
			case <-time.After(time.Second):
				t.Fatal("missing endpoint")
			}
		}
		r.ResolveNow(resolver.ResolveNowOptions{})
	}
}
