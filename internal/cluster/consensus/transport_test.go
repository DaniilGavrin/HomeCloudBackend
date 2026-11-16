package consensus

import "testing"

func TestRaftTransportRestrictedToLoopback(t *testing.T) {
	good := []string{"127.0.0.1:19091", "[::1]:19092"}
	for _, s := range good {
		if err := loopbackAddress(s, false); err != nil {
			t.Fatalf("rejected local %q: %v", s, err)
		}
	}
	bad := []string{"0.0.0.0:19090", "100.82.22.45:19090", "192.168.39.11:19090", "localhost:19090", "[::]:19090", "127.0.0.1:0", "127.0.0.1:bad"}
	for _, s := range bad {
		if err := loopbackAddress(s, false); err == nil {
			t.Fatalf("accepted external/invalid %q", s)
		}
	}
}
