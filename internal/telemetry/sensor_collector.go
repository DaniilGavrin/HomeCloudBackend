package telemetry

import (
	"context"
	"time"

	"github.com/shirou/gopsutil/v4/sensors"
)

type SensorCollector struct{ interval time.Duration }

func NewSensorCollector(interval time.Duration) *SensorCollector {
	if interval < 5*time.Second { interval = 5*time.Second }
	return &SensorCollector{interval:interval}
}
func (c *SensorCollector) Name() string { return "sensors" }
func (c *SensorCollector) Interval() time.Duration { return c.interval }
func (c *SensorCollector) Collect(ctx context.Context) ([]Sample, error) {
	temperatures, err := sensors.TemperaturesWithContext(ctx)
	if err != nil { return nil, err }
	now := time.Now().UTC()
	out := make([]Sample, 0, len(temperatures)*3)
	for _, sensor := range temperatures {
		if sensor.SensorKey == "" { continue }
		labels := map[string]string{"sensor":sensor.SensorKey}
		out = append(out, Sample{
			Name:"hardware.temperature.celsius", Labels:labels,
			Value:sensor.Temperature, Unit:"celsius", Timestamp:now,
		})
		if sensor.High > 0 {
			out = append(out, Sample{
				Name:"hardware.temperature.high_celsius", Labels:labels,
				Value:sensor.High, Unit:"celsius", Timestamp:now,
			})
		}
		if sensor.Critical > 0 {
			out = append(out, Sample{
				Name:"hardware.temperature.critical_celsius", Labels:labels,
				Value:sensor.Critical, Unit:"celsius", Timestamp:now,
			})
		}
	}
	return out, nil
}
