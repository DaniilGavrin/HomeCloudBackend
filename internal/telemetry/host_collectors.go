package telemetry

import (
	"fmt"
	"time"
)

// RegisterHostCollectors attaches the portable host collectors to the core.
// CPU, memory and network use the configured cadence; disk capacity scans are
// internally throttled to avoid repeatedly walking mounted filesystems.
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
		NewProcessCollector(interval),
		NewSensorCollector(interval),
		NewBackendCollector(interval),
	}
	for _, collector := range collectors {
		if err := core.Register(collector); err != nil {
			return fmt.Errorf("register %s collector: %w", collector.Name(), err)
		}
	}
	return nil
}
