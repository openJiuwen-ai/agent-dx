package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	api "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/backend"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/controlbackend"
	"gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/discovery"
	pb "gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api/internal/gen/controlv1"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type config struct {
	LoopbackHTTP        bool              `json:"loopback_http"`
	Discovery           *discovery.Config `json:"discovery"`
	Listen              string            `json:"listen"`
	MasterAddress       string            `json:"master_address"`
	CA                  string            `json:"ca"`
	Certificate         string            `json:"certificate"`
	PrivateKey          string            `json:"private_key"`
	ServerName          string            `json:"server_name"`
	RPCTimeoutSeconds   int               `json:"rpc_timeout_seconds"`
	CacheTTLSeconds     int               `json:"cache_ttl_seconds"`
	CacheEntries        int               `json:"cache_entries"`
	AuthCacheTTLSeconds int               `json:"auth_cache_ttl_seconds"`
	AgentAddress        string            `json:"agent_address"`
}

// Loopback HTTP is the local Edge ingress; internal RPC always uses mTLS.
func validateIngress(c config) error {
	if !c.LoopbackHTTP {
		return nil
	}
	host, _, err := net.SplitHostPort(c.Listen)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("loopback_http requires a literal loopback listen address")
	}
	return nil
}

type unsupportedSnapshots struct{}

func (unsupportedSnapshots) Do(*http.Request) (*http.Response, error) {
	return nil, errors.New("snapshot API is not connected yet")
}
func run() error {
	file := flag.String("config", "", "service JSON configuration")
	flag.Parse()
	if *file == "" {
		return errors.New("--config required")
	}
	f, err := os.Open(*file)
	if err != nil {
		return err
	}
	defer f.Close()
	var c config
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&c) != nil {
		return errors.New("invalid service configuration")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("trailing service configuration")
	}
	if c.Listen == "" || c.ServerName == "" || ((c.MasterAddress == "") == (c.Discovery == nil)) {
		return errors.New("listen, TLS server name and exactly one of master_address and discovery required")
	}
	if err := validateIngress(c); err != nil {
		return err
	}
	cert, err := tls.LoadX509KeyPair(c.Certificate, c.PrivateKey)
	if err != nil {
		return errors.New("cannot load service TLS identity")
	}
	ca, err := os.ReadFile(c.CA)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("invalid TLS CA")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{cert}, ServerName: c.ServerName}
	timeout := time.Duration(c.RPCTimeoutSeconds) * time.Second
	options := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	target := c.MasterAddress
	if c.Discovery != nil {
		builder, closeRedis, e := discovery.New(*c.Discovery, timeout)
		if e != nil {
			return e
		}
		defer closeRedis()
		options = append(options, grpc.WithResolvers(builder))
		target = "adx-redis:///master"
	}
	master, err := grpc.NewClient(target, options...)
	if err != nil {
		return err
	}
	defer master.Close()
	auth, err := controlbackend.NewAuthenticator(pb.NewAuthServiceClient(master), time.Duration(c.AuthCacheTTLSeconds)*time.Second, timeout, c.CacheEntries)
	if err != nil {
		return err
	}
	var mu sync.Mutex
	nodes := map[string]*grpc.ClientConn{}
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		for _, v := range nodes {
			_ = v.Close()
		}
	}()
	dial := func(_ context.Context, address string) (pb.NodeServiceClient, error) {
		mu.Lock()
		defer mu.Unlock()
		connection := nodes[address]
		if connection == nil {
			if len(nodes) >= c.CacheEntries {
				return nil, errors.New("node connection budget exhausted")
			}
			var e error
			connection, e = grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
			if e != nil {
				return nil, e
			}
			nodes[address] = connection
		}
		return pb.NewNodeServiceClient(connection), nil
	}
	b, err := controlbackend.New(pb.NewMasterServiceClient(master), dial, controlbackend.Config{CacheTTL: time.Duration(c.CacheTTLSeconds) * time.Second, CacheEntries: c.CacheEntries, RPCTimeout: timeout})
	if err != nil {
		return err
	}
	router := gin.New()
	router.Use(gin.Recovery())
	d := backend.Dependencies{Transport: b, Instances: b, Authenticate: auth.Verify, MasterAddress: func() string { return "" }, SnapshotHTTPClient: unsupportedSnapshots{}}
	if err = api.RegisterRoutes(router, d); err != nil {
		return err
	}
	var agent http.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "Agent service unavailable", http.StatusServiceUnavailable)
	})
	if c.AgentAddress != "" {
		u, e := url.Parse(c.AgentAddress)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return errors.New("invalid Agent endpoint")
		}
		agent = httputil.NewSingleHostReverseProxy(u)
	}
	if err = api.RegisterAgentRoutes(router, auth.Verify, agent); err != nil {
		return err
	}
	server := &http.Server{Addr: c.Listen, Handler: router, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stopped := make(chan struct{})
	shutdownResult := make(chan error, 1)
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err := server.Shutdown(shutdown)
			if err != nil {
				_ = server.Close()
			}
			shutdownResult <- err
		case <-stopped:
			shutdownResult <- nil
		}
	}()
	log.Printf("adx-sandbox-api listening on %s", c.Listen)
	if c.LoopbackHTTP {
		err = server.ListenAndServe()
	} else {
		err = server.ListenAndServeTLS(c.Certificate, c.PrivateKey)
	}
	if errors.Is(err, http.ErrServerClosed) {
		return <-shutdownResult
	}
	return err
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
