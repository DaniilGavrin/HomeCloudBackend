package runtime

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDurableClusterRuntimeStateAndCorruption(t *testing.T) {
	p := filepath.Join(t.TempDir(), "nested", "runtime.json")
	_, ok, err := readState(p, "127.0.0.1:18331")
	if err != nil || ok {
		t.Fatalf("empty: %v %v", ok, err)
	}
	in := persisted{Version: stateVersion, ClusterID: "cluster-abc", Mode: "joining", LeaderID: "hc-00000000000000000000000000000000", RaftAddress: "127.0.0.1:18331"}
	if err = saveState(p, in); err != nil {
		t.Fatal(err)
	}
	out, ok, err := readState(p, in.RaftAddress)
	if err != nil || !ok || !reflect.DeepEqual(out, in) {
		t.Fatalf("reload mismatch %+v %v %v", out, ok, err)
	}
	if _, _, err = readState(p, "127.0.0.1:19000"); err == nil {
		t.Fatal("silent bind address change must fail")
	}
	if err = os.WriteFile(p, []byte("not json"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = readState(p, in.RaftAddress); err == nil {
		t.Fatal("corrupt state must not reset and rebootstrap")
	}
}
