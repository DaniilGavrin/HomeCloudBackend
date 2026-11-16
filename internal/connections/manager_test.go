package connections

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"HomeCloudBackend/internal/commands"
)

func TestRegistrationAndLateUnregister(t *testing.T) {
	m := NewManager(3, 2)
	p := NewPeer("kitchen-tv", "television", 2, time.Second, func(context.Context, []byte) error { return nil }, nil)
	duplicate := NewPeer("kitchen-tv", "television", 2, time.Second, nil, nil)
	if err := m.Register(p); err != nil {
		t.Fatal(err)
	}
	if err := m.Register(duplicate); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("duplicate: %v", err)
	}
	m.Unregister(duplicate)
	if m.Online() != 1 {
		t.Fatal("wrong peer removed")
	}
	if m.List()[0]["device_id"] != "kitchen-tv" {
		t.Fatal("listing mismatch")
	}
	m.Unregister(p)
	replacement := NewPeer("kitchen-tv", "television", 2, time.Second, nil, nil)
	if err := m.Register(replacement); err != nil {
		t.Fatal(err)
	}
	m.Unregister(p)
	if m.Online() != 1 {
		t.Fatal("late unregister removed replacement")
	}
	m.Unregister(replacement)
}

func TestAdmissionAndShutdown(t *testing.T) {
	m := NewManager(2, 1)
	a, ok := m.Acquire("192.168.1.2")
	if !ok {
		t.Fatal("first admit")
	}
	if _, ok := m.Acquire("192.168.1.2"); ok {
		t.Fatal("per-IP limit failed")
	}
	b, ok := m.Acquire("192.168.1.3")
	if !ok {
		t.Fatal("second admit")
	}
	if _, ok := m.Acquire("192.168.1.4"); ok {
		t.Fatal("total limit failed")
	}
	a()
	a()
	c, ok := m.Acquire("192.168.1.4")
	if !ok {
		t.Fatal("release failed")
	}
	var n atomic.Int32
	_ = m.Track(func() { n.Add(1) })
	m.CloseAll()
	m.CloseAll()
	if n.Load() != 1 {
		t.Fatalf("shutdown count=%d", n.Load())
	}
	if _, ok := m.Acquire("192.168.1.5"); ok {
		t.Fatal("admit after shutdown")
	}
	b()
	c()
}

func TestWriterAndBoundedQueue(t *testing.T) {
	firstWriting := make(chan struct{})
	unblock := make(chan struct{})
	var calls atomic.Int32
	var mu sync.Mutex
	payloads := []string{}
	p := NewPeer("tv", "television", 1, 2*time.Second, func(ctx context.Context, b []byte) error {
		if calls.Add(1) == 1 {
			close(firstWriting)
			select {
			case <-unblock:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		mu.Lock()
		payloads = append(payloads, string(b))
		mu.Unlock()
		return nil
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.RunWriter(ctx)
	a := commands.Outbound{V: 1, Type: "event", Topic: "clients.message", ID: "a"}
	if err := p.QueueJSON(ctx, a); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstWriting:
	case <-time.After(time.Second):
		t.Fatal("writer not started")
	}
	if err := p.QueueJSON(ctx, a); err != nil {
		t.Fatalf("second enqueued: %v", err)
	}
	if err := p.QueueJSON(ctx, a); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected queue full, got %v", err)
	}
	close(unblock)
	deadline := time.After(time.Second)
	for calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("writer did not drain")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	p.Close()
	if err := p.QueueJSON(ctx, a); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected closed, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(payloads) != 2 {
		t.Fatalf("written=%d", len(payloads))
	}
}

func TestSendJSONAndRemoteOffline(t *testing.T) {
	m := NewManager(2, 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msg := commands.Outbound{V: 1, Type: "event"}
	if err := m.QueueToDevice(ctx, "missing", msg); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("not offline: %v", err)
	}
	p := NewPeer("pc", "desktop", 2, time.Second, func(context.Context, []byte) error { return nil }, nil)
	if err := m.Register(p); err != nil {
		t.Fatal(err)
	}
	go p.RunWriter(ctx)
	if err := m.SendToDevice(ctx, "pc", msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	m.Unregister(p)
}

func TestConcurrentListAndSend(t *testing.T) {
	m := NewManager(100, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p := NewPeer("receiver", "television", 5000, 2*time.Second, func(context.Context, []byte) error { return nil }, nil)
	if err := m.Register(p); err != nil {
		t.Fatal(err)
	}
	go p.RunWriter(ctx)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = m.List()
				if err := m.QueueToDevice(ctx, "receiver", commands.Outbound{V: 1, Type: "event"}); err != nil {
					t.Errorf("queue: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	m.Unregister(p)
}
