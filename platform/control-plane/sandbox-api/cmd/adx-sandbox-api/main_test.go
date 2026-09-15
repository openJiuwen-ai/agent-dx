package main

import "testing"

func TestLoopbackIngress(t *testing.T) {
	for _, address := range []string{"127.0.0.1:8888", "[::1]:8888"} {
		if err := validateIngress(config{Listen: address, LoopbackHTTP: true}); err != nil {
			t.Fatal(err)
		}
	}
	for _, address := range []string{"0.0.0.0:8888", ":8888", "10.0.0.1:8888", "localhost:8888", "invalid"} {
		if err := validateIngress(config{Listen: address, LoopbackHTTP: true}); err == nil {
			t.Fatalf("accepted %s", address)
		}
	}
	if err := validateIngress(config{Listen: "0.0.0.0:8888"}); err != nil {
		t.Fatal(err)
	}
}
