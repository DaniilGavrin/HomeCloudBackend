// Package mediagateway holds only ephemeral registration of the local companion.
// Rooms and active playback state are owned by a different component.
package mediagateway

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Endpoint struct {
	Network string `json:"network"`
	URL     string `json:"url"`
}
type Capabilities struct {
	MediaProtocol int      `json:"media_protocol"`
	BinaryFrame   string   `json:"binary_frame"`
	DirectWS      bool     `json:"direct_ws"`
	Modes         []string `json:"modes"`
}
type Registration struct {
	GatewayID                string       `json:"gateway_id"`
	OwnerNodeID              string       `json:"owner_node_id,omitempty"`
	Endpoints                []Endpoint   `json:"endpoints"`
	Capabilities             Capabilities `json:"capabilities"`
	RegisteredAt             time.Time    `json:"registered_at"`
	SessionID                int64        `json:"-"`
	HeartbeatIntervalSeconds int          `json:"heartbeat_interval_seconds"`
	HeartbeatGraceSeconds    int          `json:"heartbeat_grace_seconds"`
}

type Registry struct {
	mu      sync.RWMutex
	current *Registration
}

func New() *Registry { return &Registry{} }

// IsLocalIP verifies that a URL uses an address on THIS host, not a private
// address selected by an untrusted peer. It can be injected in unit tests.
func IsLocalIP(ip netip.Addr) bool {
	ifaces, err := net.Interfaces()
	if err != nil {
		return false
	}
	ip = ip.Unmap()
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			subnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			own, ok := netip.AddrFromSlice(subnet.IP)
			if ok && own.Unmap() == ip {
				return true
			}
		}
	}
	return false
}

var tailscale4 = netip.MustParsePrefix("100.64.0.0/10")
var tailscale6 = netip.MustParsePrefix("fd7a:115c:a1e0::/48")

func ValidateEndpoints(items []Endpoint, port int, ownsIP func(netip.Addr) bool) error {
	if len(items) < 1 || len(items) > 16 {
		return errors.New("endpoints: expected 1..16 private non-loopback URLs")
	}
	seen := make(map[string]bool, len(items))
	for _, ep := range items {
		if ep.Network != "lan" && ep.Network != "tailscale" {
			return errors.New("endpoint network must be lan or tailscale")
		}
		u, err := url.Parse(ep.URL)
		if err != nil || u.Scheme != "ws" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/ws/stream" || u.Opaque != "" {
			return fmt.Errorf("invalid Gateway ws endpoint %q", ep.URL)
		}
		if u.Port() != strconv.Itoa(port) {
			return errors.New("endpoint port differs from supervised Gateway health port")
		}
		ip, err := netip.ParseAddr(u.Hostname())
		if err != nil || !ip.IsValid() || ip.IsLoopback() || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() {
			return errors.New("endpoint must be an explicit routable private IP (no 127.0.0.1 or 0.0.0.0)")
		}
		ip = ip.Unmap()
		ts := tailscale4.Contains(ip) || tailscale6.Contains(ip)
		if (ep.Network == "tailscale" && !ts) || (ep.Network == "lan" && (!ip.IsPrivate() || ts)) {
			return errors.New("endpoint IP does not belong to declared network type")
		}
		if ownsIP == nil || !ownsIP(ip) {
			return fmt.Errorf("endpoint %s is not an active local interface", ip)
		}
		key := strings.ToLower(ep.URL)
		if seen[key] {
			return errors.New("duplicate endpoint")
		}
		seen[key] = true
	}
	return nil
}

// Register replaces only the local Gateway entry: no second participant is
// created after reconnect. Never persist ephemeral endpoints in YAML/DB.
func (r *Registry) Register(entry Registration, port int, ownsIP func(netip.Addr) bool) (Registration, error) {
	// Earlier registration clients lacked an explicit heartbeat policy.
	// The companion's startup YAML is the single source of the new policy.
	if entry.HeartbeatIntervalSeconds == 0 {
		entry.HeartbeatIntervalSeconds = 20
	}
	if entry.HeartbeatGraceSeconds == 0 {
		entry.HeartbeatGraceSeconds = 10
	}
	if entry.HeartbeatIntervalSeconds < 20 || entry.HeartbeatIntervalSeconds > 30 || entry.HeartbeatGraceSeconds != 10 {
		return Registration{}, errors.New("invalid heartbeat interval/grace")
	}
	if entry.GatewayID != "homecloud-media-gateway" {
		return Registration{}, errors.New("unknown Gateway identity")
	}
	if entry.SessionID == 0 {
		return Registration{}, errors.New("missing control session identity")
	}
	if entry.Capabilities.MediaProtocol != 1 || entry.Capabilities.BinaryFrame != "HCM1" || len(entry.Capabilities.Modes) > 8 {
		return Registration{}, errors.New("incompatible media capabilities")
	}
	if err := ValidateEndpoints(entry.Endpoints, port, ownsIP); err != nil {
		return Registration{}, err
	}
	entry.RegisteredAt = time.Now().UTC()
	entry.Endpoints = append([]Endpoint(nil), entry.Endpoints...)
	entry.Capabilities.Modes = append([]string(nil), entry.Capabilities.Modes...)
	sort.Slice(entry.Endpoints, func(i, j int) bool { return entry.Endpoints[i].URL < entry.Endpoints[j].URL })
	r.mu.Lock()
	r.current = &entry
	r.mu.Unlock()
	return clone(entry), nil
}
func clone(e Registration) Registration {
	e.Endpoints = append([]Endpoint(nil), e.Endpoints...)
	e.Capabilities.Modes = append([]string(nil), e.Capabilities.Modes...)
	return e
}
func (r *Registry) Unregister(gatewayID string, sessionID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current != nil && r.current.GatewayID == gatewayID && r.current.SessionID == sessionID {
		r.current = nil
	}
}
func (r *Registry) Get() (Registration, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current == nil {
		return Registration{}, false
	}
	return clone(*r.current), true
}
func (r *Registry) Resolve(network string) (Endpoint, error) {
	if network != "lan" && network != "tailscale" {
		return Endpoint{}, errors.New("network must be lan or tailscale")
	}
	e, ok := r.Get()
	if !ok {
		return Endpoint{}, errors.New("Gateway is offline")
	}
	for _, ep := range e.Endpoints {
		if ep.Network == network {
			return ep, nil
		}
	}
	return Endpoint{}, fmt.Errorf("Gateway has no reachable %s endpoint", network)
}
