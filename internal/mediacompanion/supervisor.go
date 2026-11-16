// Package mediacompanion starts and observes the separate HomeCloud Media Gateway.
// It never proxies video bytes or takes ownership of HomeCloud WS state.
package mediacompanion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"HomeCloudBackend/wsconfig"
)

const checkInterval = 5 * time.Second

type Supervisor struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Start supervises the separate executable. Failure of the optional media
// companion never brings down the primary Backend and never blocks HTTP serve.
func Start(parent context.Context, cfg wsconfig.MediaGatewayConfig, backendRoot string, log *slog.Logger) *Supervisor {
	if !cfg.Enabled {
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	s := &Supervisor{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		for attempt := 0; attempt <= cfg.MaxRestarts; attempt++ {
			if ctx.Err() != nil {
				return
			}
			err := runOnce(ctx, cfg, backendRoot, log)
			if ctx.Err() != nil {
				return
			}
			log.Error("Media Gateway companion stopped", "error", err, "attempt", attempt+1,
				"attempt_limit", cfg.MaxRestarts+1)
			if attempt == cfg.MaxRestarts {
				break
			}
			timer := time.NewTimer(time.Duration(cfg.RestartBackoffSeconds) * time.Second)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
		}
		log.Error("Media Gateway restart limit reached; HomeCloud Backend remains running")
	}()
	return s
}

// Stop triggers Gateway's portable stdin-EOF shutdown handshake and waits for
// the supervisor to reap the child. Calling Stop more than once is safe.
func (s *Supervisor) Stop() {
	if s == nil {
		return
	}
	s.cancel()
	<-s.done
}

func runOnce(ctx context.Context, cfg wsconfig.MediaGatewayConfig, root string, log *slog.Logger) error {
	wd := cfg.WorkingDir
	if !filepath.IsAbs(wd) {
		wd = filepath.Join(root, wd)
	}
	binary := cfg.Executable
	if !filepath.IsAbs(binary) {
		binary = filepath.Join(wd, binary)
	}

	cmd := exec.Command(binary, "-config", cfg.ConfigPath, "-supervised")
	cmd.Dir = wd
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	pipe, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("supervised stdin pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		_ = pipe.Close()
		return fmt.Errorf("start Gateway binary %s: %w", binary, err)
	}
	log.Info("Media Gateway process launched", "pid", cmd.Process.Pid, "binary", binary)
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	// Always close stdin first; child uses EOF to cancel its HTTP context.
	// On timeout we Kill and then reap the child on Windows and Linux alike.
	defer stopChild(cmd, pipe, exited, time.Duration(cfg.ShutdownTimeoutSeconds)*time.Second, log)

	transport := &http.Transport{Proxy: nil} // local health checks must never use an HTTP proxy
	client := &http.Client{Transport: transport, Timeout: 750 * time.Millisecond}
	defer transport.CloseIdleConnections()
	bootTimer := time.NewTimer(time.Duration(cfg.StartupTimeoutSeconds) * time.Second)
	defer bootTimer.Stop()
	poll := time.NewTicker(250 * time.Millisecond)
	defer poll.Stop()

readyLoop:
	for {
		select {
		case err := <-exited:
			// The channel value was consumed: return reaped result to stopChild.
			exited <- err
			return fmt.Errorf("Gateway exited before readiness: %v", err)
		case <-ctx.Done():
			return nil
		case <-bootTimer.C:
			return errors.New("Gateway readiness deadline exceeded")
		case <-poll.C:
			if err := probeHealth(ctx, client, cfg.HealthURL); err == nil {
				// Avoid announcing another process as healthy if our child
				// has already exited due to address/port collision.
				select {
				case err := <-exited:
					exited <- err
					return fmt.Errorf("Gateway exited while health was responding: %v", err)
				default:
				}
				log.Info("Media Gateway process ready", "pid", cmd.Process.Pid, "health_url", cfg.HealthURL)
				break readyLoop
			}
		}
	}
	healthTicker := time.NewTicker(checkInterval)
	defer healthTicker.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-exited:
			exited <- err
			return fmt.Errorf("Gateway process exited unexpectedly: %v", err)
		case <-healthTicker.C:
			err := probeHealth(ctx, client, cfg.HealthURL)
			if err == nil {
				failures = 0
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			failures++
			log.Warn("Media Gateway health probe failed", "failures", failures, "error", err)
			if failures >= 3 {
				return fmt.Errorf("Gateway health unavailable after 3 probes: %w", err)
			}
		}
	}
}

func probeHealth(ctx context.Context, client *http.Client, endpoint string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP health status %d", res.StatusCode)
	}
	var payload struct {
		Service string `json:"service"`
		Status  string `json:"status"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 512)).Decode(&payload); err != nil {
		return err
	}
	if payload.Service != "homecloud-media-gateway" || payload.Status != "alive" {
		return fmt.Errorf("unexpected health service=%q status=%q", payload.Service, payload.Status)
	}
	return nil
}

func stopChild(cmd *exec.Cmd, pipe io.WriteCloser, exited chan error, timeout time.Duration, log *slog.Logger) {
	_ = pipe.Close()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-exited:
		log.Info("Media Gateway child reaped", "pid", cmd.Process.Pid, "exit", err)
	case <-timer.C:
		log.Warn("Media Gateway shutdown timed out; killing child", "pid", cmd.Process.Pid)
		_ = cmd.Process.Kill()
		err := <-exited
		log.Info("Media Gateway child reaped after kill", "pid", cmd.Process.Pid, "exit", err)
	}
}
