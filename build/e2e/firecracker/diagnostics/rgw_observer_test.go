package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestStreamingObserverPreservesSignedRequestAndCountsActualRange(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "signed-rgw.example:18334" || r.Header.Get("Authorization") != "private-auth" || r.Header.Get("Range") != "bytes=2-5" || r.URL.RawQuery != "X-Amz-Signature=private-signature" {
			t.Errorf("signed request changed")
		}
		w.Header().Set("Content-Range", "bytes 2-5/10")
		w.Header().Set("Content-Length", "4")
		w.WriteHeader(206)
		_, _ = io.WriteString(w, "data")
	}))
	defer backend.Close()
	target, _ := url.Parse(backend.URL)
	var output bytes.Buffer
	handler := observer(target, &output)
	request := httptest.NewRequest("GET", "http://signed-rgw.example:18334/volume/chunks/1?X-Amz-Signature=private-signature", nil)
	request.Header.Set("Authorization", "private-auth")
	request.Header.Set("Range", "bytes=2-5")
	reply := httptest.NewRecorder()
	handler.ServeHTTP(reply, request)
	if reply.Code != 206 || reply.Body.String() != "data" {
		t.Fatal("response changed")
	}
	var row map[string]any
	if err := json.Unmarshal(output.Bytes(), &row); err != nil {
		t.Fatal(err)
	}
	if row["response_bytes"] != float64(4) || row["complete"] != true || row["content_range"] != "bytes 2-5/10" {
		t.Fatal(row)
	}
	if strings.Contains(output.String(), "private-") || strings.Contains(output.String(), "X-Amz-Signature") {
		t.Fatal("credential leaked to observer log")
	}
}
