package system

import (
	"context"
	"encoding/json"
	"testing"

	"HomeCloudBackend/internal/commands"
)

func TestProcessListCommandReturnsBoundedInventory(t *testing.T) {
	registry := commands.NewRegistry()
	RegisterProcessList(registry, false)
	data, err := json.Marshal(map[string]any{"limit":5, "sort_by":"cpu"})
	if err != nil { t.Fatal(err) }
	out := registry.Dispatch(context.Background(), commands.Inbound{
		V:1, Type:"command", Action:"system.processes", Data:data,
	}, commands.ClientContext{})
	if out.Type != "result" { t.Fatalf("expected result, got %#v", out) }
	result, ok := out.Data.(map[string]any)
	if !ok { t.Fatalf("unexpected response data: %#v", out.Data) }
	items, ok := result["processes"].([]ProcessRecord)
	if !ok { t.Fatalf("unexpected process list type: %#v", result["processes"]) }
	if len(items) > 5 { t.Fatalf("process list exceeded requested limit: %d", len(items)) }
	if count, ok := result["process_count"].(int); !ok || count < len(items) {
		t.Fatalf("invalid process count: %#v", result["process_count"])
	}
}

func TestProcessListCommandRejectsInvalidSort(t *testing.T) {
	registry := commands.NewRegistry()
	RegisterProcessList(registry, false)
	data, _ := json.Marshal(map[string]any{"sort_by":"command_line"})
	out := registry.Dispatch(context.Background(), commands.Inbound{
		V:1, Type:"command", Action:"system.processes", Data:data,
	}, commands.ClientContext{})
	if out.Type != "error" { t.Fatalf("expected validation error, got %#v", out) }
}
