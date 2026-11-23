// A one-user development gateway and its example sandbox service.
package main

import (
	"crypto/subtle"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("set %s", name)
	}
	return value
}

func sandbox(w http.ResponseWriter, r *http.Request) {
	// Only report whether the expected demonstration credential arrived.
	if r.Header.Get("X-ADX-AUTH") != "" || r.Header.Get("Authorization") != "Bearer example-sandbox" {
		http.Error(w, "unexpected credential headers", http.StatusUnauthorized)
		return
	}
	if r.URL.Path == "/echo" {
		fmt.Fprintln(w, "hello from sandbox")
		return
	}
	if r.URL.Path != "/ws" {
		http.NotFound(w, r)
		return
	}
	upgrader := websocket.Upgrader{}
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	c.SetReadLimit(4096)
	for {
		kind, message, err := c.ReadMessage()
		if err != nil || c.WriteMessage(kind, message) != nil {
			return
		}
	}
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "sandbox" {
		log.Fatal(http.ListenAndServe(":8080", http.HandlerFunc(sandbox)))
	}
	platform, err := url.Parse(required("ADX_SERVER_ADDRESS"))
	if err != nil || platform.Scheme != "https" || platform.Host == "" || platform.User != nil || platform.RawQuery != "" || platform.Fragment != "" || strings.Trim(platform.Path, "/") != "" {
		log.Fatal("ADX_SERVER_ADDRESS must be an HTTPS origin")
	}
	platformKey := required("ADX_TOKEN")
	session := required("DEMO_SESSION")
	target := "urn:adx:template:gateway-echo:" + required("DEMO_TEMPLATE_VERSION")
	proxy := &httputil.ReverseProxy{Rewrite: func(p *httputil.ProxyRequest) {
		path := "/agent/http/echo"
		if p.In.URL.Path == "/ws" {
			path = "/agent/ws/ws"
		}
		p.SetURL(platform)
		p.Out.URL.Path = path
		p.Out.URL.RawPath = ""
		p.Out.URL.RawQuery = url.Values{"target": {target}, "port": {"8080"}}.Encode()
		// Construct the upstream handshake from the supported client headers.
		headers := http.Header{}
		for _, key := range []string{"Connection", "Upgrade", "Sec-WebSocket-Key", "Sec-WebSocket-Version"} {
			if value := p.Out.Header.Get(key); value != "" {
				headers.Set(key, value)
			}
		}
		headers.Set("X-ADX-AUTH", platformKey)
		headers.Set("X-ADX-Binding-Id", "demo-alice")
		headers.Set("Authorization", "Bearer example-sandbox")
		p.Out.Header = headers
	}}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || (r.URL.Path != "/echo" && r.URL.Path != "/ws") {
			http.NotFound(w, r)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+session)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		proxy.ServeHTTP(w, r)
	})
	server := http.Server{Addr: "127.0.0.1:8181", Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(server.ListenAndServe())
}
