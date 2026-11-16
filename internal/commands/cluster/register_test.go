package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"HomeCloudBackend/internal/commands"
)

func TestWSClusterCommandsAndValidation(t *testing.T) {
	r := commands.NewRegistry()
	var joined JoinRequest
	calls := 0
	d := Dependencies{
		Status:    func(context.Context) (any, error) { return map[string]any{"mode": "standalone"}, nil },
		List:      func(_ context.Context, removed bool) (any, error) { return removed, nil },
		Leader:    func(context.Context) (any, error) { return "leader", nil },
		Bootstrap: func(context.Context, string, string) (any, error) { return "bootstrapped", nil },
		Join:      func(_ context.Context, req JoinRequest) (any, error) { calls++; joined = req; return "joined", nil },
		Remove:    func(context.Context, string) (any, error) { return "removed", nil },
		Update:    func(context.Context, UpdateRequest) (any, error) { return "updated", nil },
	}
	if err := Register(r, d); err != nil {
		t.Fatal(err)
	}
	send := func(action, data string) commands.Outbound {
		return r.Dispatch(context.Background(), commands.Inbound{V: 1, ID: "test", Type: "command", Action: action, Data: []byte(data)}, commands.ClientContext{})
	}
	if x := send("cluster.nodes.join_by_address", `{"address":" 100.82.22.45 ","name":"Bedroom"}`); x.Type != "result" {
		t.Fatalf("join: %+v", x)
	}
	if calls != 1 || joined.Address != "100.82.22.45" {
		t.Fatal("join not normalized")
	}
	if x := send("cluster.nodes.join_by_address", `{"address":"10.0.0.1","evil":"field"}`); x.Type != "error" || calls != 1 {
		t.Fatalf("unknown field accepted: %+v", x)
	}
	if x := send("cluster.nodes.join_by_address", `{"address":"10.0.0.1"}{} `); x.Type != "error" {
		t.Fatal("trailing JSON accepted")
	}
	if x := send("cluster.nodes.list", `{"include_removed":true}`); x.Type != "result" || x.Data != true {
		t.Fatalf("list: %+v", x)
	}
	if x := send("cluster.bootstrap", `{"name":""}`); x.Type != "error" {
		t.Fatal("empty name accepted")
	}
	if x := send("cluster.nodes.update", `{"node_id":"hc-1","name":"New","expected_revision":1,"idempotency_key":"abcdef"}`); x.Type != "error" || !strings.Contains(x.Error, "timestamp") {
		t.Fatalf("update without timestamp: %+v", x)
	}
	x := send("cluster.nodes.update", `{"node_id":"hc-123","name":"Updated","expected_revision":3,"idempotency_key":"stable-key","timestamp":"2026-10-28T13:00:00Z"}`)
	if x.Type != "result" {
		t.Fatalf("update failed: %+v", x)
	}
	if x = send("cluster.nodes.remove", `{"node_id":""}`); x.Type != "error" {
		t.Fatal("empty remove allowed")
	}
	if x = send("cluster.status", "{}"); x.Type != "result" {
		t.Fatal(x)
	}
	if x = send("cluster.leader.get", "{}"); x.Type != "result" {
		t.Fatal(x)
	}
}
func TestClusterNoDependenciesAndCancellation(t *testing.T) {
	if err := Register(commands.NewRegistry(), Dependencies{}); err == nil {
		t.Fatal("expected missing dependencies")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	x, y := BoundWriteContext(ctx)
	defer y()
	if !errors.Is(x.Err(), context.Canceled) {
		t.Fatal("cancel not respected")
	}
	ctx2, cancel2 := BoundWriteContext(context.Background())
	defer cancel2()
	deadline, ok := ctx2.Deadline()
	if !ok || time.Until(deadline) > 35*time.Second {
		t.Fatal("missing write deadline")
	}
}
