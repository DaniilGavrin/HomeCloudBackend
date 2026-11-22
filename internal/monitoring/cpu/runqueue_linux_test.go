//go:build linux

package cpu

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestParseSchedDebugRunQueue(t *testing.T) {
	data := []byte("cpu#0, 0\n  .nr_running                 : 2\n  .load                        : 1024\ncpu#1, 1\n  .nr_running                 : 3\n")
	value, found, err := parseSchedDebugRunQueue(data)
	if err != nil || !found || value != 5 {
		t.Fatalf("runqueue = %d, found=%v, err=%v; want 5", value, found, err)
	}
}

func TestReadLinuxRunQueueUsesCompatibleSchedulerSource(t *testing.T) {
	root := t.TempDir()
	debug := filepath.Join(root, "sched_debug")
	if err := os.WriteFile(debug, []byte("cpu#0, 0\n .nr_running : 2\ncpu#1, 1\n .nr_running : 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	item := readLinuxRunQueue(context.Background(), []string{debug})
	if item.Status != StatusValid || item.Value != uint64(3) || item.SourceID != "linux.host.proc.sched_debug.nr_running" {
		t.Fatalf("scheduler debug result = %#v", item)
	}
}

func TestReadLinuxRunQueueDoesNotUseProcStatApproximation(t *testing.T) {
	item := readLinuxRunQueue(context.Background(), []string{filepath.Join(t.TempDir(), "missing")})
	if item.Status != StatusUnavailable || item.Value != nil || item.Reason != "no_semantically_compatible_source" {
		t.Fatalf("runqueue without compatible source must be unavailable: %#v", item)
	}
}
