package events

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"HomeCloudBackend/internal/commands"
)

func TestSubscribePublishAndUnsubscribe(t *testing.T) {
	b := New()
	ctx := context.Background()
	var mu sync.Mutex
	received := map[string][]commands.Outbound{}
	recipient := func(id string) Delivery {
		return func(_ context.Context, out commands.Outbound) error {
			mu.Lock()
			received[id] = append(received[id], out)
			mu.Unlock()
			return nil
		}
	}
	pc := b.Attach("pc", recipient("pc"))
	tv := b.Attach("tv", recipient("tv"))
	if ok, err := b.Subscribe(pc, "media.playback.changed"); err != nil || !ok {
		t.Fatalf("pc subscribe %v %v", ok, err)
	}
	if ok, err := b.Subscribe(pc, "media.playback.changed"); err != nil || ok {
		t.Fatalf("duplicate %v %v", ok, err)
	}
	if ok, err := b.Subscribe(tv, "media.playback.changed"); err != nil || !ok {
		t.Fatalf("tv subscribe %v %v", ok, err)
	}
	if len(b.Topics(pc)) != 1 || !b.Has(tv, "media.playback.changed") {
		t.Fatal("subscriptions missing")
	}
	result, err := b.Publish(ctx, "media.playback.changed", "phone", map[string]any{"position_ms": 2000})
	if err != nil || result.Subscribers != 2 || result.Queued != 2 || result.Dropped != 0 {
		t.Fatalf("publish %+v %v", result, err)
	}
	mu.Lock()
	for _, id := range []string{"pc", "tv"} {
		if len(received[id]) != 1 || received[id][0].Topic != "media.playback.changed" {
			t.Errorf("received %s: %+v", id, received[id])
		}
		data := received[id][0].Data.(map[string]any)
		if data["from"] != "phone" {
			t.Errorf("sender not forwarded %s: %+v", id, data)
		}
	}
	mu.Unlock()
	if ok, err := b.Unsubscribe(tv, "media.playback.changed"); err != nil || !ok {
		t.Fatalf("unsubscribe %v %v", ok, err)
	}
	if ok, err := b.Unsubscribe(tv, "media.playback.changed"); err != nil || ok {
		t.Fatalf("duplicate unsubscribe %v %v", ok, err)
	}
	result, _ = b.Publish(ctx, "media.playback.changed", "phone", 1)
	if result.Queued != 1 || result.Subscribers != 1 {
		t.Fatalf("after unsubscribe %+v", result)
	}
}

func TestSessionCleanupAndReconnect(t *testing.T) {
	b := New()
	send := func(context.Context, commands.Outbound) error { return nil }
	old := b.Attach("tv", send)
	_, _ = b.Subscribe(old, "device.state.changed")
	b.Detach(old)
	b.Detach(old)
	newer := b.Attach("tv", send)
	if b.Has(newer, "device.state.changed") || len(b.Topics(newer)) != 0 {
		t.Fatal("subscription leaked on reconnect")
	}
	b.Detach(old)
	if ok, err := b.Subscribe(newer, "device.state.changed"); err != nil || !ok {
		t.Fatalf("newer session affected %v %v", ok, err)
	}
	if _, err := b.Subscribe(old, "device.state.changed"); !errors.Is(err, ErrDetached) {
		t.Fatalf("detached accepted %v", err)
	}
	result, _ := b.Publish(context.Background(), "device.state.changed", "backend", true)
	if result.Queued != 1 {
		t.Fatalf("unexpected delivery %+v", result)
	}
	b.Detach(newer)
	result, _ = b.Publish(context.Background(), "device.state.changed", "backend", true)
	if result.Subscribers != 0 {
		t.Fatalf("subscriptions leaked %+v", result)
	}
}

func TestValidationLimitsAndSlowConsumers(t *testing.T) {
	b := New()
	bad := b.Attach("bad", func(context.Context, commands.Outbound) error { return ErrDetached })
	ok := b.Attach("ok", func(context.Context, commands.Outbound) error { return nil })
	for _, name := range []string{"", "invalid", "topic.*", "Media.play", "home..bad", "x."} {
		if _, err := b.Subscribe(ok, name); !errors.Is(err, ErrInvalidTopic) {
			t.Errorf("accepted invalid topic %q: %v", name, err)
		}
		if _, err := b.Publish(context.Background(), name, "backend", nil); !errors.Is(err, ErrInvalidTopic) {
			t.Errorf("published invalid topic %q: %v", name, err)
		}
	}
	_, _ = b.Subscribe(bad, "home.changed")
	_, _ = b.Subscribe(ok, "home.changed")
	result, err := b.Publish(context.Background(), "home.changed", "server", 1)
	if err != nil || result.Queued != 1 || result.Dropped != 1 {
		t.Fatalf("slow client %+v %v", result, err)
	}
	for i := 0; i < maxTopicsPerSubscriber-1; i++ {
		if _, err := b.Subscribe(ok, fmt.Sprintf("device.value%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Subscribe(ok, "device.extra"); !errors.Is(err, ErrTopicLimit) {
		t.Fatalf("subscription limit %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Publish(ctx, "home.changed", "test", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel %v", err)
	}
}

func TestConcurrentSubscribePublishDetach(t *testing.T) {
	b := New()
	var deliveries atomic.Int64
	send := func(context.Context, commands.Outbound) error { deliveries.Add(1); return nil }
	peers := make([]*Subscriber, 20)
	for i := range peers {
		peers[i] = b.Attach(fmt.Sprintf("client-%d", i), send)
	}
	var wg sync.WaitGroup
	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_, _ = b.Subscribe(peer, "house.sensor.changed")
				if i%2 == 0 {
					_, _ = b.Unsubscribe(peer, "house.sensor.changed")
				}
			}
			b.Detach(peer)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_, _ = b.Publish(context.Background(), "house.sensor.changed", "backend", i)
		}
	}()
	wg.Wait()
	result, err := b.Publish(context.Background(), "house.sensor.changed", "backend", true)
	if err != nil || result.Subscribers != 0 {
		t.Fatalf("cleanup failed %+v %v", result, err)
	}
}
