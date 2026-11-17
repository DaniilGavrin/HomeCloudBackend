package telemetry

import (
	"context"
	"errors"
	"math"
	"regexp"
	"time"
)

var metricNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+package telemetry

import (
	"context"
	"errors"
	"math"
	"regexp"
	"time"
)

)

// Sample is a single numeric measurement. Labels must contain stable dimensions
// (for example cpu="0" or interface="eth0"), never high-cardinality values.
type Sample struct {
	NodeID    string            `json:"node_id"`
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels,omitempty"`
	Value     float64           `json:"value"`
	Unit      string            `json:"unit,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

func (s Sample) Validate() error {
	if math.IsNaN(s.Value) || math.IsInf(s.Value, 0) {
		return errors.New("metric value must be finite")
	}
	if s.Name == "" || !metricNamePattern.MatchString(s.Name) {
		return errors.New("metric name must use dotted lowercase identifiers")
	}
	if len(s.Name) > 160 {
		return errors.New("metric name is too long")
	}
	if s.NodeID == "" {
		return errors.New("metric node id is required")
	}
	if len(s.NodeID) > 128 {
		return errors.New("node id is too long")
	}
	if len(s.Unit) > 32 {
		return errors.New("metric unit is too long")
	}
	if len(s.Labels) > 16 {
		return errors.New("too many metric labels")
	}
	for key, value := range s.Labels {
		if key == "" || len(key) > 64 || len(value) > 128 {
			return errors.New("invalid metric label")
		}
	}
	if s.Timestamp.IsZero() {
		return errors.New("metric timestamp is required")
	}
	return nil
}

// Collector periodically produces measurements. Collectors must honor ctx and
// should return promptly; Core isolates each collector with its own timeout.
type Collector interface {
	Name() string
	Interval() time.Duration
	Collect(context.Context) ([]Sample, error)
}

// History is the persistence contract used by Core. Implementations must bound
// their own write latency and must not retain caller-owned slices or maps.
type History interface {
	Append(context.Context, []Sample) error
	Query(context.Context, Query) ([]Point, error)
	Close() error
}

type Query struct {
	NodeID     string
	Metric     string
	From       time.Time
	To         time.Time
	Resolution time.Duration
	Limit      int
}

type Point struct {
	NodeID    string            `json:"node_id"`
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	Value     float64           `json:"value"`
	Min       float64           `json:"min,omitempty"`
	Max       float64           `json:"max,omitempty"`
	Count     int64             `json:"count,omitempty"`
	Unit      string            `json:"unit,omitempty"`
}

var (
	ErrClosed       = errors.New("telemetry core is closed")
	ErrQueueFull    = errors.New("telemetry write queue is full")
	ErrInvalidQuery = errors.New("invalid telemetry query")
)
