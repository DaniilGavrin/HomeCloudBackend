package mediacompanion

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"HomeCloudBackend/wsconfig"
)

func TestProbeHealth(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("wrong method")
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "alive", "service": "homecloud-media-gateway"})
	}))
	defer s.Close()
	c := &http.Client{Timeout: time.Second}
	if err := probeHealth(context.Background(), c, s.URL); err != nil {
		t.Fatal(err)
	}
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"alive","service":"another-service"}`))
	}))
	defer other.Close()
	if err := probeHealth(context.Background(), c, other.URL); err == nil {
		t.Fatal("wrong service should be rejected")
	}
}

func TestDisabledCompanionIsNoop(t *testing.T) {
	s := Start(context.Background(), wsconfig.MediaGatewayConfig{}, ".", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if s != nil {
		t.Fatal("disabled gateway should not allocate a supervisor")
	}
	s.Stop()
}
