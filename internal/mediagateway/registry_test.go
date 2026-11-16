package mediagateway

import (
	"net/netip"
	"testing"
)

func TestValidateEndpoints(t *testing.T) {
	own := func(netip.Addr) bool { return true }
	cases := []struct {
		name     string
		endpoint Endpoint
		ok       bool
	}{
		{"LAN", Endpoint{"lan", "ws://192.168.39.11:8091/ws/stream"}, true},
		{"Tailscale", Endpoint{"tailscale", "ws://100.82.22.45:8091/ws/stream"}, true},
		{"loopback", Endpoint{"lan", "ws://127.0.0.1:8091/ws/stream"}, false},
		{"wildcard", Endpoint{"lan", "ws://0.0.0.0:8091/ws/stream"}, false},
		{"public", Endpoint{"lan", "ws://8.8.8.8:8091/ws/stream"}, false},
		{"wrong port", Endpoint{"lan", "ws://192.168.39.11:8092/ws/stream"}, false},
		{"wrong network", Endpoint{"lan", "ws://100.82.22.45:8091/ws/stream"}, false},
		{"bad path", Endpoint{"lan", "ws://192.168.39.11:8091/ws/storage"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEndpoints([]Endpoint{tc.endpoint}, 8091, own)
			if (err == nil) != tc.ok {
				t.Fatalf("got error %v; expected OK %v", err, tc.ok)
			}
		})
	}
}
func TestReconnectDoesNotDeleteNewSession(t *testing.T) {
	r := New()
	e := Registration{GatewayID: "homecloud-media-gateway", SessionID: 101,
		Endpoints:    []Endpoint{{Network: "lan", URL: "ws://192.168.39.11:8091/ws/stream"}},
		Capabilities: Capabilities{MediaProtocol: 1, BinaryFrame: "HCM1"}}
	owns := func(netip.Addr) bool { return true }
	if _, err := r.Register(e, 8091, owns); err != nil {
		t.Fatal(err)
	}
	e.SessionID = 102
	if _, err := r.Register(e, 8091, owns); err != nil {
		t.Fatal(err)
	}
	r.Unregister(e.GatewayID, 101)
	v, ok := r.Get()
	if !ok || v.SessionID != 102 {
		t.Fatal("old session removed newer registration")
	}
	ep, err := r.Resolve("lan")
	if err != nil || ep.URL != e.Endpoints[0].URL {
		t.Fatalf("resolve: %v %+v", err, ep)
	}
	r.Unregister(e.GatewayID, 102)
	if _, ok = r.Get(); ok {
		t.Fatal("stale registration after disconnect")
	}
}
