package wsconfig

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.yaml.in/yaml/v3"
)

var ErrCreated = errors.New("config/config.yaml создан; заполните настройки и перезапустите сервер")

const (
	StartFast   uint8 = 0xA1
	StartStrict uint8 = 0xA2
	DBSQLite           uint8 = 0x00
	DBPostgreSQLLocal  uint8 = 0x01
	DBPostgreSQLRemote uint8 = 0x02
)

//go:embed template.yaml
var template []byte

// Load expects to be called from the project root (or service working directory).
func Load() (*Config, error) {
	root, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("рабочая директория: %w", err)
	}
	return LoadAt(root)
}

func LoadAt(root string) (*Config, error) {
	dir := filepath.Join(root, "config")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("создать %s: %w", dir, err)
	}
	path := filepath.Join(dir, "config.yaml")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err == nil {
		_, writeErr := f.Write(template)
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			_ = os.Remove(path)
			return nil, fmt.Errorf("создание %s: запись=%v закрытие=%v", path, writeErr, closeErr)
		}
		return nil, fmt.Errorf("%w: %s; укажите type_start.start_byte и type_start.db_byte", ErrCreated, path)
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("проверка %s: %w", path, err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("чтение %s: %w", path, err)
	}

	// First decode only the mode. Unlike uint8, *uint8 distinguishes missing from 0x00.
	var header struct {
		TypeStart struct {
			StartByte *uint8 `yaml:"start_byte"`
		} `yaml:"type_start"`
	}
	if err := yaml.Unmarshal(content, &header); err != nil {
		return nil, fmt.Errorf("синтаксис YAML: %w", err)
	}
	if header.TypeStart.StartByte == nil {
		return nil, errors.New("укажите стартовый указатель type_start.start_byte: 0xA1 или 0xA2")
	}
	mode := *header.TypeStart.StartByte
	if mode != StartFast && mode != StartStrict {
		return nil, fmt.Errorf("неизвестный start_byte 0x%02X: допустимо 0xA1 или 0xA2", mode)
	}

	dec := yaml.NewDecoder(bytes.NewReader(content))
	dec.KnownFields(mode == StartStrict)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("YAML: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("config.yaml должен содержать только один YAML-документ")
		}
		return nil, fmt.Errorf("дополнительный YAML-документ: %w", err)
	}
	if mode == StartFast {
		if cfg.Server.Host == "" {
			cfg.Server.Host = "auto_private"
		}
		if cfg.Server.Port == 0 {
			cfg.Server.Port = 8080
		}
		if cfg.WebSocket.HelloTimeoutSeconds == 0 {
			cfg.WebSocket.HelloTimeoutSeconds = 10
		}
		if cfg.Server.WebSocketPath == "" {
			cfg.Server.WebSocketPath = "/ws"
		}
	}
	if err := CheckStart(&cfg, root); err != nil {
		return nil, fmt.Errorf("режим 0x%02X: %w", mode, err)
	}
	if err := cfg.MediaGateway.Validate(); err != nil {
		return nil, fmt.Errorf("media_gateway: %w", err)
	}
	if err := cfg.DeviceBridge.Validate(); err != nil {
		return nil, fmt.Errorf("device_bridge: %w", err)
	}
	return &cfg, nil
}
