package telemetry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Core struct {
	nodeID    string
	history   History
	mu        sync.RWMutex
	collectors map[string]Collector
	latest    map[string]Sample
	cancel    context.CancelFunc
	started   bool
	closed    bool
	wg        sync.WaitGroup
	dropped   atomic.Uint64
	collectErrors atomic.Uint64
}

func NewCore(nodeID string, history History) (*Core, error) {
	if nodeID == "" {
		return nil, errors.New("telemetry node id is required")
	}
	if history == nil {
		return nil, errors.New("telemetry history is required")
	}
	return &Core{
		nodeID: nodeID, history: history,
		collectors: make(map[string]Collector),
		latest: make(map[string]Sample),
	}, nil
}

func (c *Core) Register(collector Collector) error {
	if collector == nil || collector.Name() == "" {
		return errors.New("collector and collector name are required")
	}
	if collector.Interval() < time.Second {
		return errors.New("collector interval must be at least one second")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.started {
		return ErrClosed
	}
	if _, exists := c.collectors[collector.Name()]; exists {
		return fmt.Errorf("collector %q is already registered", collector.Name())
	}
	c.collectors[collector.Name()] = collector
	return nil
}

func (c *Core) Start(parent context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if c.started {
		return errors.New("telemetry core already started")
	}
	if parent == nil {
		return errors.New("telemetry parent context is nil")
	}
	ctx, cancel := context.WithCancel(parent)
	c.cancel = cancel
	c.started = true
	for _, collector := range c.collectors {
		collector := collector
		c.wg.Add(1)
		go c.runCollector(ctx, collector)
	}
	return nil
}

func (c *Core) runCollector(ctx context.Context, collector Collector) {
	defer c.wg.Done()
	interval := collector.Interval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// Collect once immediately so a newly started node does not wait a full interval.
	c.collect(ctx, collector, interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collect(ctx, collector, interval)
		}
	}
}

func (c *Core) collect(parent context.Context, collector Collector, interval time.Duration) {
	timeout := interval
	if timeout > 10*time.Second {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	samples, err := collector.Collect(ctx)
	if err != nil {
		c.collectErrors.Add(1)
		return
	}
	if len(samples) == 0 {
		return
	}
	now := time.Now().UTC()
	for i := range samples {
		if samples[i].Timestamp.IsZero() {
			samples[i].Timestamp = now
		}
		if samples[i].NodeID == "" {
			samples[i].NodeID = c.nodeID
		}
		if math.IsNaN(samples[i].Value) || math.IsInf(samples[i].Value, 0) {
			samples[i].Name = ""
			c.collectErrors.Add(1)
			continue
		}
		if err := samples[i].Validate(); err != nil {
			c.collectErrors.Add(1)
			samples[i].Name = ""
		}
	}
	valid := samples[:0]
	for _, sample := range samples {
		if sample.Name != "" {
			valid = append(valid, sample)
		}
	}
	if len(valid) == 0 {
		return
	}
	c.mu.Lock()
	for _, sample := range valid {
		sample.Labels = cloneLabels(sample.Labels)
		key := sampleKey(sample)
		c.latest[key] = sample
	}
	c.mu.Unlock()
	if err := c.history.Append(parent, valid); err != nil {
		c.dropped.Add(uint64(len(valid)))
	}
}

func (c *Core) Latest(nodeID string, metric string) []Sample {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]Sample, 0)
	for _, sample := range c.latest {
		if (nodeID == "" || sample.NodeID == nodeID) && (metric == "" || sample.Name == metric) {
			sample.Labels = cloneLabels(sample.Labels)
			result = append(result, sample)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name == result[j].Name {
			return sampleKey(result[i]) < sampleKey(result[j])
		}
		return result[i].Name < result[j].Name
	})
	return result
}

func (c *Core) Query(ctx context.Context, query Query) ([]Point, error) {
	return c.history.Query(ctx, query)
}

func (c *Core) Stats() map[string]uint64 {
	return map[string]uint64{
		"dropped_samples": c.dropped.Load(),
		"collection_errors": c.collectErrors.Load(),
	}
}

func (c *Core) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.wg.Wait()
	return c.history.Close()
}

func sampleKey(sample Sample) string {
	labels, _ := encodeLabels(sample.Labels)
	return sample.NodeID + "|" + sample.Name + "|" + labels
}
