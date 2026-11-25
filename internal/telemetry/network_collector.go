package telemetry

import (
	"context"
	"sync"
	"time"

	gnet "github.com/shirou/gopsutil/v4/net"
)

type networkPrevious struct {
	at time.Time
	rxBytes, txBytes uint64
	rxPackets, txPackets uint64
	rxErrors, txErrors uint64
	rxDrops, txDrops uint64
}

type NetworkCollector struct {
	interval time.Duration
	mu sync.Mutex
	previous map[string]networkPrevious
}

func NewNetworkCollector(interval time.Duration) *NetworkCollector {
	if interval < time.Second { interval = time.Second }
	return &NetworkCollector{interval:interval, previous:make(map[string]networkPrevious)}
}
func (c *NetworkCollector) Name() string { return "network" }
func (c *NetworkCollector) Interval() time.Duration { return c.interval }
func (c *NetworkCollector) Collect(ctx context.Context) ([]Sample, error) {
	stats, err := gnet.IOCountersWithContext(ctx, true)
	if err != nil { return nil, err }
	now := time.Now().UTC()
	out := make([]Sample, 0, len(stats)*8)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, current := range stats {
		if current.Name == "" { continue }
		prev, ok := c.previous[current.Name]
		if ok && current.BytesRecv >= prev.rxBytes && current.BytesSent >= prev.txBytes &&
			current.PacketsRecv >= prev.rxPackets && current.PacketsSent >= prev.txPackets &&
			current.Errin >= prev.rxErrors && current.Errout >= prev.txErrors &&
			current.Dropin >= prev.rxDrops && current.Dropout >= prev.txDrops {
			seconds := now.Sub(prev.at).Seconds()
			if seconds > 0 {
				labels := map[string]string{"interface":current.Name}
				add := func(name string, value float64, unit string) {
					out = append(out, Sample{Name:name, Labels:labels, Value:value, Unit:unit, Timestamp:now})
				}
				add("network.receive_bytes_per_second", float64(current.BytesRecv-prev.rxBytes)/seconds, "bytes_per_second")
				add("network.transmit_bytes_per_second", float64(current.BytesSent-prev.txBytes)/seconds, "bytes_per_second")
				add("network.receive_packets_per_second", float64(current.PacketsRecv-prev.rxPackets)/seconds, "packets_per_second")
				add("network.transmit_packets_per_second", float64(current.PacketsSent-prev.txPackets)/seconds, "packets_per_second")
				add("network.receive_errors_per_second", float64(current.Errin-prev.rxErrors)/seconds, "errors_per_second")
				add("network.transmit_errors_per_second", float64(current.Errout-prev.txErrors)/seconds, "errors_per_second")
				add("network.receive_drops_per_second", float64(current.Dropin-prev.rxDrops)/seconds, "drops_per_second")
				add("network.transmit_drops_per_second", float64(current.Dropout-prev.txDrops)/seconds, "drops_per_second")
			}
		}
		c.previous[current.Name] = networkPrevious{at:now, rxBytes:current.BytesRecv, txBytes:current.BytesSent,
			rxPackets:current.PacketsRecv, txPackets:current.PacketsSent, rxErrors:current.Errin,
			txErrors:current.Errout, rxDrops:current.Dropin, txDrops:current.Dropout}
	}
	return out, nil
}
