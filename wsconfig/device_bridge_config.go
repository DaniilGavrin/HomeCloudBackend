package wsconfig

import (
	"errors"
	"net"
	"strconv"
	"strings"
)

func (c DeviceBridgeConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	host, portText, err := net.SplitHostPort(strings.TrimSpace(c.BindAddress))
	if err != nil {
		return errors.New("bind_address must be an explicit loopback host and TCP port")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("bind_address must use a loopback IP address")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("bind_address must contain a valid TCP port")
	}
	return nil
}
