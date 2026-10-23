// Streaming diagnostic observer; never log Authorization or query contents.
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

type response struct {
	http.ResponseWriter
	bytes      int64
	status     int
	writeError bool
}

func (r *response) Unwrap() http.ResponseWriter { return r.ResponseWriter }
func (r *response) WriteHeader(status int)      { r.status = status; r.ResponseWriter.WriteHeader(status) }
func (r *response) Write(data []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	count, err := r.ResponseWriter.Write(data)
	r.bytes += int64(count)
	if err != nil {
		r.writeError = true
	}
	return count, err
}
func observer(target *url.URL, output io.Writer) http.Handler {
	var mu sync.Mutex
	proxy := httputil.NewSingleHostReverseProxy(target)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		response := &response{ResponseWriter: writer}
		// Preserve the original signed Host, path and Range. The proxy streams bodies.
		// A failed upstream read can panic with ErrAbortHandler; retain partial evidence.
		completed := false
		defer func() {
			declared, err := strconv.ParseInt(response.Header().Get("Content-Length"), 10, 64)
			complete := completed && !response.writeError && (err != nil || declared == response.bytes)
			row := map[string]any{
				"at_unix_ns": started.UnixNano(), "method": request.Method,
				"path": request.URL.Path, "range": request.Header.Get("Range"),
				"query": request.URL.RawQuery != "", "content_range": response.Header().Get("Content-Range"),
				"complete": complete, "status": response.status, "response_bytes": response.bytes,
				"seconds": time.Since(started).Seconds(),
			}
			mu.Lock()
			defer mu.Unlock()
			if err := json.NewEncoder(output).Encode(row); err != nil {
				log.Printf("observer evidence write failed: %v", err)
			}
		}()
		proxy.ServeHTTP(response, request)
		completed = true
	})
}
func main() {
	listen := flag.String("listen", "127.0.0.1:18334", "")
	upstream := flag.String("upstream", "http://127.0.0.1:18333", "")
	path := flag.String("log", "", "JSONL path")
	flag.Parse()
	target, err := url.Parse(*upstream)
	if err != nil {
		log.Fatal(err)
	}
	output, err := os.OpenFile(*path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer output.Close()
	log.Fatal(http.ListenAndServe(*listen, observer(target, output)))
}
