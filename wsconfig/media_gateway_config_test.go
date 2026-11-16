package wsconfig

import "testing"

func TestMediaGatewayConfigValidation(t *testing.T) {
	base := MediaGatewayConfig{Enabled: true, Executable: "gateway.exe", WorkingDir: "../gateway", ConfigPath: "config/config.yaml", HealthURL: "http://127.0.0.1:8091/healthz", StartupTimeoutSeconds: 10, ShutdownTimeoutSeconds: 10, RestartBackoffSeconds: 3, MaxRestarts: 2}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*MediaGatewayConfig)
	}{
		{"no executable", func(c *MediaGatewayConfig) { c.Executable = "" }},
		{"wildcard health", func(c *MediaGatewayConfig) { c.HealthURL = "http://0.0.0.0:8091/healthz" }},
		{"public health", func(c *MediaGatewayConfig) { c.HealthURL = "http://8.8.8.8:8091/healthz" }},
		{"unexpected endpoint", func(c *MediaGatewayConfig) { c.HealthURL = "http://127.0.0.1:8091/ws" }},
		{"negative restarts", func(c *MediaGatewayConfig) { c.MaxRestarts = -1 }},
		{"missing timeout", func(c *MediaGatewayConfig) { c.StartupTimeoutSeconds = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			if cfg.Validate() == nil {
				t.Fatal("expected validation failure")
			}
		})
	}
	if err := (MediaGatewayConfig{}).Validate(); err != nil {
		t.Fatalf("disabled mode must remain backwards compatible: %v", err)
	}
}
