package wsconfig

import (
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
)

// MediaGatewayConfig is only process-supervision startup configuration.
// It does not describe rooms, devices or runtime playback state.
type MediaGatewayConfig struct {
	Enabled                bool   `yaml:"enabled"`
	Executable             string `yaml:"executable"`
	WorkingDir             string `yaml:"working_dir"`
	ConfigPath             string `yaml:"config_path"`
	HealthURL              string `yaml:"health_url"`
	StartupTimeoutSeconds  int    `yaml:"startup_timeout_seconds"`
	ShutdownTimeoutSeconds int    `yaml:"shutdown_timeout_seconds"`
	MaxRestarts            int    `yaml:"max_restarts"`
	RestartBackoffSeconds  int    `yaml:"restart_backoff_seconds"`
}

func (c MediaGatewayConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if strings.TrimSpace(c.Executable) == "" || strings.TrimSpace(c.WorkingDir) == "" || strings.TrimSpace(c.ConfigPath) == "" {
		return errors.New("executable, working_dir and config_path are mandatory when enabled")
	}
	if c.StartupTimeoutSeconds < 1 || c.StartupTimeoutSeconds > 120 ||
		c.ShutdownTimeoutSeconds < 1 || c.ShutdownTimeoutSeconds > 120 ||
		c.MaxRestarts < 0 || c.MaxRestarts > 10 ||
		c.RestartBackoffSeconds < 1 || c.RestartBackoffSeconds > 120 {
		return errors.New("invalid startup/shutdown/restart timeouts or restart limit")
	}
	u, err := url.Parse(c.HealthURL)
	if err != nil {
		return fmt.Errorf("health_url: %w", err)
	}
	if u.Scheme != "http" || u.User != nil || u.Path != "/healthz" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("health_url must be http://PRIVATE-IP:PORT/healthz without credentials/query/fragment")
	}
	host := u.Hostname()
	ip, err := netip.ParseAddr(host)
	if err != nil || !(ip.IsLoopback() || ip.IsPrivate() || netip.MustParsePrefix("100.64.0.0/10").Contains(ip)) {
		return errors.New("health_url must use an explicit loopback/private/Tailscale IP")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return errors.New("health_url must contain a valid TCP port")
	}
	return nil
}
