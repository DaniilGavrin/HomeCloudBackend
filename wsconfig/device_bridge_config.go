package wsconfig

import (
	"errors"
	"net"
	"strconv"
	"strings"
)

func (c DeviceBridgeConfig) Validate() error {
	if !c.Enabled { return nil }
	host, portText, err := net.SplitHostPort(strings.TrimSpace(c.BindAddress))
	if err != nil { return errors.New("bind_address must be an explicit loopback host and TCP port") }
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() { return errors.New("bind_address must use a loopback IP address") }
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 { return errors.New("bind_address must contain a valid TCP port") }
	if strings.TrimSpace(c.Executable)=="" || strings.TrimSpace(c.WorkingDir)=="" || strings.TrimSpace(c.ConfigPath)=="" {
		return errors.New("executable, working_dir and config_path are required when enabled")
	}
	if c.StartupTimeoutSeconds<1 || c.StartupTimeoutSeconds>180 ||
		c.ShutdownTimeoutSeconds<1 || c.ShutdownTimeoutSeconds>120 ||
		c.HeartbeatTimeoutSeconds<15 || c.HeartbeatTimeoutSeconds>600 ||
		c.InitialBackoffSeconds<1 || c.InitialBackoffSeconds>60 ||
		c.MaxBackoffSeconds<c.InitialBackoffSeconds || c.MaxBackoffSeconds>600 {
		return errors.New("invalid Device Bridge timeout or backoff settings")
	}
	return nil
}
