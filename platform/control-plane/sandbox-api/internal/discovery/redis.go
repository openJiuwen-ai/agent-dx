// Package discovery resolves the current Master; it does not own Instance routes.
package discovery

import (
	"context"
	"encoding/json"
	"errors"
	redis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/resolver"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Config struct {
	RedisURL    string `json:"redis_url"`
	Namespace   string `json:"namespace"`
	PollSeconds int    `json:"poll_seconds"`
}
type endpoint struct {
	Schema  uint32 `json:"schema"`
	Epoch   uint64 `json:"epoch"`
	Address string `json:"address"`
}

func decode(values []interface{}) (string, error) {
	if len(values) != 2 {
		return "", errors.New("invalid Master discovery response")
	}
	raw, ok := values[0].(string)
	if !ok {
		return "", errors.New("Master not registered")
	}
	header, ok := values[1].(string)
	if !ok {
		return "", errors.New("Master epoch unavailable")
	}
	var e endpoint
	var h struct {
		Epoch uint64 `json:"epoch"`
	}
	if json.Unmarshal([]byte(raw), &e) != nil || json.Unmarshal([]byte(header), &h) != nil || e.Schema != 1 || e.Epoch == 0 || e.Epoch != h.Epoch {
		return "", errors.New("invalid or superseded Master discovery")
	}
	u, err := url.Parse(e.Address)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", errors.New("HTTPS Master endpoint required")
	}
	if _, _, err = net.SplitHostPort(u.Host); err != nil {
		return "", errors.New("Master host and port required")
	}
	return u.Host, nil
}

// Builder is scoped to one gRPC connection, not registered process-wide.
type Builder struct {
	lookup              func(context.Context) (string, error)
	interval, timeLimit time.Duration
}

func New(c Config, timeout time.Duration) (*Builder, func() error, error) {
	if c.Namespace == "" || len(c.Namespace) > 128 || strings.IndexFunc(c.Namespace, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-')
	}) >= 0 || c.PollSeconds <= 0 || timeout <= 0 {
		return nil, nil, errors.New("discovery namespace and positive intervals required")
	}
	options, err := redis.ParseURL(strings.Replace(c.RedisURL, "redis+unix://", "unix://", 1))
	if err != nil {
		return nil, nil, errors.New("invalid discovery Redis endpoint")
	}
	options.ContextTimeoutEnabled = true
	options.DialTimeout = timeout
	options.ReadTimeout = timeout
	options.WriteTimeout = timeout
	options.MaxRetries = -1
	client := redis.NewClient(options)
	prefix := "adx:{" + c.Namespace + "}"
	b := &Builder{interval: time.Duration(c.PollSeconds) * time.Second, timeLimit: timeout}
	b.lookup = func(ctx context.Context) (string, error) {
		v, err := client.Eval(ctx, "return {redis.call('GET', KEYS[1]), redis.call('HGET', KEYS[2], 'header')}", []string{prefix + ":master:v1", prefix + ":control:v1"}).Slice()
		if err != nil {
			return "", errors.New("Master discovery unavailable")
		}
		return decode(v)
	}
	return b, client.Close, nil
}
func (*Builder) Scheme() string { return "adx-redis" }
func (b *Builder) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &watcher{cancel: cancel, wake: make(chan struct{}, 1), done: make(chan struct{})}
	go func() {
		defer close(r.done)
		tick := time.NewTicker(b.interval)
		defer tick.Stop()
		last := ""
		for {
			call, stop := context.WithTimeout(ctx, b.timeLimit)
			address, err := b.lookup(call)
			stop()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				cc.ReportError(err)
			} else if address != last {
				if err = cc.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: address}}}); err != nil {
					cc.ReportError(err)
				} else {
					last = address
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			case <-r.wake:
			}
		}
	}()
	return r, nil
}

type watcher struct {
	cancel context.CancelFunc
	wake   chan struct{}
	done   chan struct{}
	once   sync.Once
}

func (r *watcher) ResolveNow(resolver.ResolveNowOptions) {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func (r *watcher) Close() { r.once.Do(r.cancel); <-r.done }
