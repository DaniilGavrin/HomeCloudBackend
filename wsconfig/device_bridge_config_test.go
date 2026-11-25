package wsconfig

import "testing"

func TestDeviceBridgeConfigValidation(t *testing.T) {
	valid := DeviceBridgeConfig{Enabled: true, BindAddress: "127.0.0.1:8094"}
	if err := valid.Validate(); err != nil { t.Fatal(err) }
	for _, address := range []string{"0.0.0.0:8094", "192.168.1.10:8094", "localhost:8094", "127.0.0.1", "127.0.0.1:0", "[::]:8094"} {
		cfg := valid
		cfg.BindAddress = address
		if err := cfg.Validate(); err == nil { t.Errorf("expected %q to be rejected", address) }
	}
	if err := (DeviceBridgeConfig{}).Validate(); err != nil { t.Fatalf("disabled config should be backward compatible: %v", err) }
}
