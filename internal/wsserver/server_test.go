package wsserver

import (
	"testing"

	"HomeCloudBackend/internal/connections"
)

func TestDeviceIDValidation(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"kitchen-tv", true}, {"pc-main-postman", true}, {"audio.zone-01", true},
		{"", false}, {"../../config", false}, {"bad ID", false}, {"\n", false},
	} {
		if got := connections.ValidDeviceID(tc.value); got != tc.valid {
			t.Errorf("device id %q got %v want %v", tc.value, got, tc.valid)
		}
	}
}

func TestDuplicateDeviceIDRejected(t *testing.T) {
	manager := connections.NewManager(10, 10)
	first := connections.NewPeer("kitchen-tv", "tv", 1, 0, nil, nil)
	second := connections.NewPeer("kitchen-tv", "tv", 1, 0, nil, nil)
	if err := manager.Register(first); err != nil {
		t.Fatal(err)
	}
	if err := manager.Register(second); err != connections.ErrDuplicateID {
		t.Fatalf("expected duplicate, got %v", err)
	}
	manager.Unregister(second)
	if got := manager.Online(); got != 1 {
		t.Fatalf("wrong cleanup: %d", got)
	}
	manager.Unregister(first)
	if got := manager.Online(); got != 0 {
		t.Fatalf("still online: %d", got)
	}
}
