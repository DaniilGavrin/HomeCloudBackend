// Package netbind discovers private host IPs and creates network-scoped handlers.
// It is defense in depth; firewall, router and Tailscale policies are still required.
package netbind

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

type Endpoint struct {
	Interface string
	IP        net.IP
	Address   string
	Allowed   *net.IPNet
}

var tailscaleV4 = mustCIDR("100.64.0.0/10")
var tailscaleV6 = mustCIDR("fd7a:115c:a1e0::/48")

func mustCIDR(s string) *net.IPNet { _, n, _ := net.ParseCIDR(s); return n }

func isTailscale(name string) bool { return strings.Contains(strings.ToLower(name), "tailscale") }

func privateInterfaceIP(name string, ip net.IP) bool {
	if ip == nil || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	if isTailscale(name) {
		return tailscaleV4.Contains(ip) || tailscaleV6.Contains(ip)
	}
	return ip.IsPrivate()
}

func peerCIDR(ifaceName string, ip net.IP, original *net.IPNet) *net.IPNet {
	if ip.IsLoopback() {
		if ip.To4() != nil {
			return mustCIDR("127.0.0.0/8")
		}
		return mustCIDR("::1/128")
	}
	if isTailscale(ifaceName) {
		if ip.To4() != nil {
			return tailscaleV4
		}
		return tailscaleV6
	}
	return original
}

func Discover(host string, port int) ([]Endpoint, error) {
	if port < 1 || port > 65535 {
		return nil, errors.New("port must be 1..65535")
	}
	if host == "0.0.0.0" || host == "::" || host == "[::]" || host == "" {
		return nil, errors.New("wildcard host forbidden; use auto_private")
	}
	auto := host == "auto_private"
	wanted := net.ParseIP(host)
	if !auto && wanted == nil {
		if strings.EqualFold(host, "localhost") {
			wanted = net.ParseIP("127.0.0.1")
		} else {
			return nil, errors.New("host must be auto_private, localhost, or a literal private IP")
		}
	}
	if wanted != nil && !wanted.IsLoopback() && !wanted.IsPrivate() && !tailscaleV4.Contains(wanted) && !tailscaleV6.Contains(wanted) {
		return nil, errors.New("public bind forbidden")
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var endpoints []Endpoint
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		// Filter untrusted virtual interfaces by default. Tailscale is explicit.
		if iface.Flags&net.FlagLoopback == 0 && len(iface.HardwareAddr) == 0 && !isTailscale(iface.Name) {
			continue
		}
		addresses, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addresses {
			network, ok := a.(*net.IPNet)
			if !ok || !privateInterfaceIP(iface.Name, network.IP) {
				continue
			}
			ip := network.IP
			if !auto && !ip.Equal(wanted) {
				continue
			}
			key := ip.String()
			if seen[key] {
				continue
			}
			seen[key] = true
			endpoints = append(endpoints, Endpoint{
				Interface: iface.Name, IP: ip,
				Address: net.JoinHostPort(key, strconv.Itoa(port)),
				Allowed: peerCIDR(iface.Name, ip, network),
			})
		}
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no eligible private interface addresses for %q", host)
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].Address < endpoints[j].Address })
	return endpoints, nil
}

// RestrictPeer protects every HTTP route, including WebSocket Upgrade.
// It checks the actual socket peer address; proxy-forwarded headers are ignored.
func RestrictPeer(ep Endpoint, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remote, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		peer := net.ParseIP(remote)
		if peer == nil || !ep.Allowed.Contains(peer) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
