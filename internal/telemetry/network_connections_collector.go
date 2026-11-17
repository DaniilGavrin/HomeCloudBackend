package telemetry

import (
	"context"
	"time"

	gnet "github.com/shirou/gopsutil/v4/net"
)

// NetworkConnectionsCollector stores aggregate connection counts only. Remote
// addresses and ports are intentionally excluded to keep cardinality and privacy bounded.
type NetworkConnectionsCollector struct{ interval time.Duration }

func NewNetworkConnectionsCollector(interval time.Duration) *NetworkConnectionsCollector {
	if interval < 10*time.Second { interval = 10*time.Second }
	return &NetworkConnectionsCollector{interval: interval}
}
func (c *NetworkConnectionsCollector) Name() string { return "network_connections" }
func (c *NetworkConnectionsCollector) Interval() time.Duration { return c.interval }
func (c *NetworkConnectionsCollector) Collect(ctx context.Context) ([]Sample, error) {
	connections, err := gnet.ConnectionsWithContext(ctx, "all")
	if err != nil { return nil, err }
	type key struct{ protocol, state string }
	counts := make(map[key]float64)
	for _, conn := range connections {
		protocol := "other"
		switch conn.Type {
		case 1: protocol = "tcp"
		case 2: protocol = "udp"
		}
		state := conn.Status
		if state == "" { state = "NONE" }
		counts[key{protocol, state}]++
	}
	now := time.Now().UTC()
	out := make([]Sample, 0, len(counts)+2)
	out = append(out,
		Sample{Name:"network.connections.total", Value:float64(len(connections)), Unit:"connections", Timestamp:now},
	)
	for k, count := range counts {
		out = append(out, Sample{Name:"network.connections.by_state", Labels:map[string]string{"protocol":k.protocol,"state":k.state}, Value:count, Unit:"connections", Timestamp:now})
	}
	return out, nil
}
