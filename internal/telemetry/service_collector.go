package telemetry

import (
	"context"
	"time"
)

// ServiceCollector records availability for services known to the local
// Backend. The provider returns stable service identifiers and online state.
type ServiceCollector struct {
	interval time.Duration
	provider func() map[string]bool
}

func NewServiceCollector(provider func() map[string]bool) *ServiceCollector {
	return &ServiceCollector{interval:5*time.Second, provider:provider}
}
func (c *ServiceCollector) Name() string { return "services" }
func (c *ServiceCollector) Interval() time.Duration { return c.interval }
func (c *ServiceCollector) Collect(context.Context) ([]Sample, error) {
	if c.provider == nil { return nil, nil }
	states := c.provider()
	now := time.Now().UTC()
	out := make([]Sample, 0, len(states))
	for name, online := range states {
		value := 0.0
		if online { value = 1 }
		out = append(out, Sample{
			Name:"services.available", Labels:map[string]string{"service":name},
			Value:value, Unit:"boolean", Timestamp:now,
		})
	}
	return out, nil
}
