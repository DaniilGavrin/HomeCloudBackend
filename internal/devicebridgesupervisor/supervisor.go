// Package devicebridgesupervisor manages the local Device Bridge child process.
package devicebridgesupervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"HomeCloudBackend/internal/devicebridge"
	"HomeCloudBackend/wsconfig"
)

type Supervisor struct {
	cancel context.CancelFunc
	done chan struct{}
}

func Start(parent context.Context, cfg wsconfig.DeviceBridgeConfig, root, wsURL string, tracker *devicebridge.Tracker, logger *slog.Logger) *Supervisor {
	if !cfg.Enabled { return nil }
	ctx, cancel := context.WithCancel(parent)
	s := &Supervisor{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		backoff := time.Duration(cfg.InitialBackoffSeconds)*time.Second
		maxBackoff := time.Duration(cfg.MaxBackoffSeconds)*time.Second
		for {
			if ctx.Err()!=nil { return }
			err := runOnce(ctx,cfg,root,wsURL,tracker,logger)
			if ctx.Err()!=nil { return }
			tracker.Unavailable("process_unavailable")
			logger.Error("Device Bridge stopped; restarting", "error",err,"backoff",backoff)
			jitter := time.Duration(float64(backoff)*(0.8+rand.Float64()*0.4))
			timer:=time.NewTimer(jitter)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() { select { case <-timer.C: default: } }
				return
			}
			backoff*=2
			if backoff>maxBackoff { backoff=maxBackoff }
		}
	}()
	return s
}

func (s *Supervisor) Stop() {
	if s==nil { return }
	s.cancel()
	<-s.done
}

func runOnce(ctx context.Context,cfg wsconfig.DeviceBridgeConfig,root,wsURL string,tracker *devicebridge.Tracker,logger *slog.Logger) error {
	wd:=cfg.WorkingDir
	if !filepath.IsAbs(wd) { wd=filepath.Join(root,wd) }
	binary:=cfg.Executable
	if !filepath.IsAbs(binary) { binary=filepath.Join(wd,binary) }
	configPath:=cfg.ConfigPath
	if !filepath.IsAbs(configPath) { configPath=filepath.Join(wd,configPath) }
	cmd:=exec.Command(binary)
	cmd.Dir=wd
	cmd.Stdout=os.Stdout
	cmd.Stderr=os.Stderr
	cmd.Env=append(os.Environ(),
		"HOMECLOUD_BACKEND_WS_URL="+wsURL,
		"HOMECLOUD_DEVICE_BRIDGE_CONFIG="+configPath,
		"HOMECLOUD_DEVICE_BRIDGE_SUPERVISED=1",
	)
	pipe,err:=cmd.StdinPipe()
	if err!=nil { return fmt.Errorf("bridge stdin pipe: %w",err) }
	if err=cmd.Start(); err!=nil { _=pipe.Close(); return fmt.Errorf("start Device Bridge %s: %w",binary,err) }
	logger.Info("Device Bridge process launched","pid",cmd.Process.Pid,"binary",binary)
	exited:=make(chan error,1)
	go func(){exited<-cmd.Wait()}()
	defer stopChild(cmd,pipe,exited,time.Duration(cfg.ShutdownTimeoutSeconds)*time.Second,logger)

	startup:=time.NewTimer(time.Duration(cfg.StartupTimeoutSeconds)*time.Second)
	defer startup.Stop()
	poll:=time.NewTicker(200*time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case err:=<-exited:
			exited<-err
			return fmt.Errorf("Bridge exited before ready: %v",err)
		case <-ctx.Done(): return nil
		case <-startup.C: return errors.New("Bridge registration/readiness deadline exceeded")
		case <-poll.C:
			if tracker.Snapshot().Ready {
				logger.Info("Device Bridge ready","pid",cmd.Process.Pid)
				goto ready
			}
		}
	}
ready:
	ticker:=time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done(): return nil
		case err:=<-exited:
			exited<-err
			return fmt.Errorf("Bridge process exited: %v",err)
		case <-ticker.C:
			status:=tracker.Snapshot()
			if !status.Connected || !status.Ready || time.Since(status.LastSeenAt)>time.Duration(cfg.HeartbeatTimeoutSeconds)*time.Second {
				tracker.Unavailable("heartbeat_timeout")
				return errors.New("Bridge heartbeat lost")
			}
		}
	}
}

func stopChild(cmd *exec.Cmd,pipe io.WriteCloser,exited chan error,timeout time.Duration,logger *slog.Logger) {
	_ = pipe.Close()
	timer:=time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err:=<-exited:
		logger.Info("Device Bridge child reaped","pid",cmd.Process.Pid,"exit",err)
	case <-timer.C:
		logger.Warn("Device Bridge shutdown timed out; killing child","pid",cmd.Process.Pid)
		_ = cmd.Process.Kill()
		err:=<-exited
		logger.Info("Device Bridge child reaped after kill","pid",cmd.Process.Pid,"exit",err)
	}
}
