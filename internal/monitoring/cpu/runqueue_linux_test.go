//go:build linux

package cpu

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestParseSchedDebugRunQueueExcludesCurrentlyRunningTasks(t *testing.T) {
	data := []byte("cpu#0, 0\n  .nr_running                 : 2\n  .curr->pid                  : 101\n  .load                        : 1024\ncpu#1, 1\n  .nr_running                 : 3\n  .curr->pid                  : 0\n")
	value, found, err := parseSchedDebugRunQueue(data)
	if err != nil || !found || value != 4 {
		t.Fatalf("waiting runqueue = %d, found=%v, err=%v; want 4", value, found, err)
	}
}

func TestParseSchedDebugRunQueueRejectsIncompleteCPUBlock(t *testing.T) {
	data := []byte("cpu#0, 0\n  .nr_running : 2\n")
	value, found, err := parseSchedDebugRunQueue(data)
	if err != nil || found || value != 0 {
		t.Fatalf("incomplete scheduler data must not produce a value: %d, found=%v, err=%v", value, found, err)
	}
}

func TestReadLinuxRunQueueUsesCompatibleSchedulerSource(t *testing.T) {
	root := t.TempDir()
	debug := filepath.Join(root, "sched_debug")
	data := "cpu#0, 0\n .nr_running : 2\n .curr->pid : 1\ncpu#1, 1\n .nr_running : 1\n .curr->pid : 0\n"
	if err := os.WriteFile(debug, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	item := readLinuxRunQueue(context.Background(), []string{debug})
	if item.Status != StatusValid || item.Value != uint64(2) || item.SourceID != "linux.host.proc.sched_debug.waiting_tasks" {
		t.Fatalf("scheduler debug result = %#v", item)
	}
}

func TestReadLinuxRunQueueDoesNotUseProcStatApproximation(t *testing.T) {
	item := readLinuxRunQueue(context.Background(), []string{filepath.Join(t.TempDir(), "missing")})
	if item.Status != StatusUnavailable || item.Value != nil || item.Reason != "no_semantically_compatible_source" {
		t.Fatalf("runqueue without compatible source must be unavailable: %#v", item)
	}
}
