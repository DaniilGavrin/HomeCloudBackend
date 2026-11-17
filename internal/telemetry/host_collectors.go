package telemetry

import (
	"fmt"
	"time"
)

// RegisterHostCollectors attaches the portable host collectors to the core.
// CPU, memory and network use the configured cadence. Disk, process, sensor and
// Backend collectors enforce slower minimum intervals on resource-constrained nodes.
func RegisterHostCollectors(core *Core, interval time.Duration) error {
	if core == nil {
		return fmt.Errorf("telemetry core is nil")
	}
	if interval < time.Second {
		interval = time.Second
	}
	collectors := []Collector{
		NewCPUCollector(interval),
		NewMemoryCollector(interval),
		NewNetworkCollector(interval),
		NewDiskCollector(interval),
		NewDiskInventoryCollector(interval),
		NewProcessCollector(interval),
		NewSensorCollector(interval),
		NewBackendCollector(interval),
		NewHostCollector(interval),
		NewFanCollector(interval),
		NewGPUCollector(interval),
		NewPressureCollector(interval),
		NewLinuxKernelCollector(interval),
		NewNetworkConnectionsCollector(interval),
		NewSMARTCollector(interval),
	}
	for _, collector := range collectors {
		if err := core.Register(collector); err != nil {
			return fmt.Errorf("register %s collector: %w", collector.Name(), err)
		}
	}
	return nil
}
