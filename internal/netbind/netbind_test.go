package netbind

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPrivateIPRules(t *testing.T) {
	cases := []struct {
		name, addr string
		want       bool
	}{
		{"LAN", "192.168.39.11", true}, {"private10", "10.1.2.3", true},
		{"loopback", "127.0.0.1", true}, {"tailscale", "100.82.22.45", true},
		{"tailIPv6", "fd7a:115c:a1e0::1", true},
		{"non-tailscale-CGNAT", "100.82.22.45", false},
		{"public", "8.8.8.8", false}, {"linklocal", "169.254.10.1", false},
		{"unspecified", "0.0.0.0", false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			name := "Wi-Fi"
			if tt.name == "tailscale" || tt.name == "tailIPv6" {
				name = "Tailscale"
			}
			got := privateInterfaceIP(name, net.ParseIP(tt.addr))
			if got != tt.want {
				t.Fatalf("got %v want %v", got, tt.want)
			}
		})
	}
}

func TestRejectPublicOrWildcardBinding(t *testing.T) {
	for _, host := range []string{"0.0.0.0", "::", "8.8.8.8", ""} {
		if _, err := Discover(host, 8080); err == nil {
			t.Errorf("%s unexpectedly accepted", host)
		}
	}
}
func TestPeerFilter(t *testing.T) {
	ep := Endpoint{Allowed: mustCIDR("192.168.39.0/24")}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	for _, tt := range []struct {
		remote string
		want   int
	}{
		{"192.168.39.45:123", 200}, {"8.8.8.8:123", 403},
		{"192.168.40.5:123", 403}, {"127.0.0.1:123", 403},
	} {
		req := httptest.NewRequest("GET", "/healthz", nil)
		req.RemoteAddr = tt.remote
		rec := httptest.NewRecorder()
		RestrictPeer(ep, next).ServeHTTP(rec, req)
		if rec.Code != tt.want {
			t.Errorf("%s: got %d want %d", tt.remote, rec.Code, tt.want)
		}
	}
}
