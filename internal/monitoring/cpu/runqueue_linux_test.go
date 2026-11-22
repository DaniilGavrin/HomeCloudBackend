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

func TestReadLinuxRunQueuePrefersSchedulerDebug(t *testing.T) {
	root := t.TempDir()
	debug := filepath.Join(root, "sched_debug")
	procStat := filepath.Join(root, "stat")
	if err := os.WriteFile(debug, []byte("cpu#0, 0\n .nr_running : 2\ncpu#1, 1\n .nr_running : 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(procStat, []byte("procs_running 9\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	item := readLinuxRunQueue(context.Background(), procStat, []string{debug})
	if item.Status != StatusPartial || item.Value != uint64(3) || item.SourceID != "linux.host.proc.sched_debug" || item.Reason != "includes_running_tasks" {
		t.Fatalf("scheduler debug result = %#v", item)
	}
}

func TestReadLinuxRunQueueFallbackIsExplicitlyPartial(t *testing.T) {
	procStat := filepath.Join(t.TempDir(), "stat")
	if err := os.WriteFile(procStat, []byte("procs_running 4\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	item := readLinuxRunQueue(context.Background(), procStat, []string{filepath.Join(t.TempDir(), "missing")})
	if item.Status != StatusPartial || item.Value != uint64(4) || item.Reason != "includes_running_tasks" || item.ErrorCode != "PARTIAL_RESULT" {
		t.Fatalf("fallback runqueue must disclose its semantics: %#v", item)
	}
}
