package wsconfig

import (
	"errors"
	"fmt"
	"go.yaml.in/yaml/v3"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// Every YAML leaf is checked. The suite intentionally reads the actual project
// example; adding a field to schema.go forces the example and all cases to expand.
const referencePath = "../config/config.example.yaml"

func referenceDocument(t *testing.T) *yaml.Node {
	t.Helper()
	// This must be a VALID config: the public template intentionally has
	// blank start_byte/db_byte and empty bind settings.
	raw, err := yaml.Marshal(validConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return &doc
}

func yamlFields(t reflect.Type, prefix string, out *[]string) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		*out = append(*out, prefix)
		return
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		name := strings.Split(field.Tag.Get("yaml"), ",")[0]
		if name == "" || name == "-" {
			continue
		}
		next := name
		if prefix != "" {
			next = prefix + "." + name
		}
		yamlFields(field.Type, next, out)
	}
}

func nodeFields(node *yaml.Node, prefix string, out *[]string) {
	if node.Kind == yaml.DocumentNode {
		nodeFields(node.Content[0], prefix, out)
		return
	}
	if node.Kind != yaml.MappingNode {
		*out = append(*out, prefix)
		return
	}
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i].Value
		full := key
		if prefix != "" {
			full = prefix + "." + key
		}
		nodeFields(node.Content[i+1], full, out)
	}
}

func getYAMLValue(doc *yaml.Node, path string) (*yaml.Node, int, error) {
	node := doc.Content[0]
	parts := strings.Split(path, ".")
	for index, part := range parts {
		if node.Kind != yaml.MappingNode {
			return nil, 0, fmt.Errorf("%s: not a mapping", path)
		}
		pos := -1
		for i := 0; i < len(node.Content); i += 2 {
			if node.Content[i].Value == part {
				pos = i
				break
			}
		}
		if pos < 0 {
			return nil, 0, fmt.Errorf("%s: not found", path)
		}
		if index == len(parts)-1 {
			return node, pos, nil
		}
		node = node.Content[pos+1]
	}
	return nil, 0, fmt.Errorf("%s: not found", path)
}

func mutateYAML(t *testing.T, doc *yaml.Node, path string, replacement string, remove bool) {
	t.Helper()
	parent, pos, err := getYAMLValue(doc, path)
	if err != nil {
		t.Fatal(err)
	}
	if remove {
		parent.Content = append(parent.Content[:pos], parent.Content[pos+2:]...)
		return
	}
	var value yaml.Node
	if err := yaml.Unmarshal([]byte(replacement), &value); err != nil {
		t.Fatal(err)
	}
	*parent.Content[pos+1] = *value.Content[0]
}

func writeDocument(t *testing.T, doc *yaml.Node) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "config"), 0700); err != nil {
		t.Fatal(err)
	}
	bytes, err := yaml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "config", "config.yaml"), bytes, 0600); err != nil {
		t.Fatal(err)
	}
	return root
}

func allLeafPaths(t *testing.T) []string {
	t.Helper()
	var fields []string
	yamlFields(reflect.TypeOf(Config{}), "", &fields)
	slices.Sort(fields)
	return fields
}

func setupSecret(t *testing.T) {
	t.Helper()
	// The test fixture stores its signing key in YAML, not the process environment.
}

func TestConfigSchemaParity(t *testing.T) {
	t.Run("Schema / all declared leaves match both YAML templates", func(t *testing.T) {
		fromSchema := allLeafPaths(t)
		for _, file := range []string{referencePath, "template.yaml"} {
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var doc yaml.Node
			if err := yaml.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			var fromYAML []string
			nodeFields(&doc, "", &fromYAML)
			slices.Sort(fromYAML)
			if !slices.Equal(fromSchema, fromYAML) {
				t.Errorf("schema differs from %s: missing=%v extra=%v", file, difference(fromSchema, fromYAML), difference(fromYAML, fromSchema))
			}
		}
		if len(fromSchema) != 191 {
			t.Fatalf("expected 191 YAML leaves, got %d (update contract and tests intentionally)", len(fromSchema))
		}
	})
}

func difference(a, b []string) []string {
	var out []string
	for _, x := range a {
		if !slices.Contains(b, x) {
			out = append(out, x)
		}
	}
	return out
}

// Unlike `LoadAt`, this tests Go's exact typed YAML interpretation without
// conflating it with semantic validation (e.g. disabled integration parameters).
func TestConfigEveryFieldTypedInterpretation(t *testing.T) {
	for _, path := range allLeafPaths(t) {
		path := path
		t.Run("Interpret / "+path, func(t *testing.T) {
			doc := referenceDocument(t)
			field, err := fieldByYAMLPath(reflect.ValueOf(&Config{}), path)
			if err != nil {
				t.Fatal(err)
			}
			kind := field.Kind()
			if kind == reflect.Pointer {
				kind = field.Type().Elem().Kind()
			}
			var payload string
			switch kind {
			case reflect.String:
				payload = "\"interpretation-probe\""
			case reflect.Bool:
				payload = "true"
			case reflect.Int:
				payload = "42"
			case reflect.Uint8:
				payload = "0x7F"
			case reflect.Slice:
				payload = "[\"item-a\", \"item-b\"]"
			default:
				t.Fatalf("no interpretation case for %s %v", path, kind)
			}
			mutateYAML(t, doc, path, payload, false)
			raw, err := yaml.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			var cfg Config
			if err := yaml.Unmarshal(raw, &cfg); err != nil {
				t.Fatalf("decode %s: %v", path, err)
			}
			got, err := fieldByYAMLPath(reflect.ValueOf(&cfg), path)
			if err != nil {
				t.Fatal(err)
			}
			if got.Kind() == reflect.Pointer {
				if got.IsNil() {
					t.Fatal("null pointer")
				}
				got = got.Elem()
			}
			switch kind {
			case reflect.String:
				if got.String() != "interpretation-probe" {
					t.Fatalf("got %q", got.String())
				}
			case reflect.Bool:
				if !got.Bool() {
					t.Fatal("bool not true")
				}
			case reflect.Int:
				if got.Int() != 42 {
					t.Fatalf("got %d", got.Int())
				}
			case reflect.Uint8:
				if got.Uint() != 127 {
					t.Fatalf("got %d", got.Uint())
				}
			case reflect.Slice:
				if got.Len() != 2 || got.Index(0).String() != "item-a" {
					t.Fatal("slice decoding mismatch")
				}
			}
		})
	}
}

func TestConfigEveryFieldWrongYAMLType(t *testing.T) {
	for _, path := range allLeafPaths(t) {
		path := path
		t.Run("Wrong type / "+path, func(t *testing.T) {
			setupSecret(t)
			doc := referenceDocument(t)
			field, err := fieldByYAMLPath(reflect.ValueOf(&Config{}), path)
			if err != nil {
				t.Fatal(err)
			}
			kind := field.Kind()
			if kind == reflect.Pointer {
				kind = field.Type().Elem().Kind()
			}
			invalid := "[wrong, type]" // guaranteed invalid for strings, integers and bools
			if kind == reflect.Slice {
				invalid = "42"
			}
			mutateYAML(t, doc, path, invalid, false)
			root := writeDocument(t, doc)
			if _, err := LoadAt(root); err == nil {
				t.Fatalf("%s: wrong YAML type was accepted", path)
			}
		})
	}
}

// These parameters have no meaningful safe zero-value in STRICT mode.
// For optional settings omission must NOT make initialisation fail.
var requiredIfAbsent = map[string]bool{
	"config_version": true,
	"home.id":        true, "home.name": true, "home.timezone": true, "home.locale": true, "home.units": true,
	"server.host": true, "server.port": true, "server.websocket_path": true,
	"server.read_header_timeout_seconds": true, "server.shutdown_timeout_seconds": true, "server.tls.mode": true,
	"websocket.protocol_version": true, "websocket.encoding": true,
	"websocket.max_connections": true, "websocket.max_connections_per_ip": true,
	"websocket.max_message_bytes": true, "websocket.max_messages_per_second": true, "websocket.max_outbound_queue_messages": true,
	"websocket.hello_timeout_seconds": true, "websocket.ping_interval_seconds": true, "websocket.pong_timeout_seconds": true, "websocket.write_timeout_seconds": true,
	"message_bus.require_message_id": true, "message_bus.require_action_namespace": true, "message_bus.command_ack_timeout_seconds": true,
	"message_bus.max_pending_commands_per_client": true, "message_bus.slow_consumer_policy": true,

	"security.pairing.pairing_window_seconds": true, "security.pairing.max_attempts": true,
	"security.critical_actions.confirmation_timeout_seconds": true,
	"db_server.file_path": true, "db_server.connection_timeout": true, "db_server.max_open_connections": true,
	"db_server.sqlite.journal_mode": true, "db_server.sqlite.foreign_keys": true,
	"devices.heartbeat_interval_seconds": true, "devices.offline_after_seconds": true,
	"screens.default_content_timeout_seconds": true, "media.control_only": true,
	"audio.max_volume_percent":           false,
	"monitoring.health_path":             false,
	"monitoring.sample_interval_seconds": true,
	"logging.level":                      true, "logging.format": true, "logging.output": true,
	"audit.retention_days":  true,
	"type_start.start_byte": true, "type_start.db_byte": true,
}

func TestConfigEveryFieldMissing(t *testing.T) {
	for _, path := range allLeafPaths(t) {
		path := path
		t.Run("Absent / "+path, func(t *testing.T) {
			setupSecret(t)
			doc := referenceDocument(t)
			mutateYAML(t, doc, path, "", true)
			cfg, err := LoadAt(writeDocument(t, doc))
			if requiredIfAbsent[path] {
				if err == nil {
					t.Fatalf("required setting %s was absent but accepted: %+v", path, cfg)
				}
			} else if err != nil {
				t.Fatalf("optional setting %s was absent but loader failed: %v", path, err)
			}
		})
	}
}

func TestConfigNumericRanges(t *testing.T) {
	var allNumberFields []string
	for _, path := range allLeafPaths(t) {
		f, err := fieldByYAMLPath(reflect.ValueOf(&Config{}), path)
		if err != nil {
			t.Fatal(err)
		}
		if f.Kind() == reflect.Int {
			allNumberFields = append(allNumberFields, path)
		}
	}
	if len(allNumberFields) != len(NumericBounds) {
		t.Fatalf("numeric bounds missing/extra: %d declared vs %d tracked", len(allNumberFields), len(NumericBounds))
	}
	for _, path := range allNumberFields {
		path := path
		bound, ok := NumericBounds[path]
		if !ok {
			t.Fatalf("missing numeric policy: %s", path)
		}
		for _, tc := range []struct {
			name   string
			value  int64
			accept bool
		}{
			{"below", bound.Min - 1, false}, {"minimum", bound.Min, true},
			{"maximum", bound.Max, true}, {"above", bound.Max + 1, false},
		} {
			tc := tc
			t.Run("Boundary "+tc.name+" / "+path, func(t *testing.T) {
				setupSecret(t)
				doc := referenceDocument(t)
				mutateYAML(t, doc, path, fmt.Sprint(tc.value), false)
				raw, err := yaml.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				var cfg Config
				if err := yaml.Unmarshal(raw, &cfg); err != nil {
					t.Fatal(err)
				}
				boundaryErr := validateNumericBounds(&cfg)
				if tc.accept && boundaryErr != nil {
					t.Fatalf("expected value %d accepted, got %v", tc.value, boundaryErr)
				}
				if !tc.accept && boundaryErr == nil {
					t.Fatalf("invalid value %d accepted", tc.value)
				}
				if !tc.accept {
					if _, startupErr := LoadAt(writeDocument(t, doc)); startupErr == nil {
						t.Fatalf("invalid value %d passed full configuration initialization", tc.value)
					}
				}
			})
		}
	}
}

func TestConfigStartAndDatabaseBytes(t *testing.T) {
	for _, tc := range []struct {
		field, value string
		accepted     bool
	}{
		{"start_byte", "0xA1", true}, {"start_byte", "0xA2", true},
		{"start_byte", "0x00", false}, {"start_byte", "0xA0", false}, {"start_byte", "0xA3", false},
		{"start_byte", "0xFF", false}, {"start_byte", "-1", false}, {"start_byte", "0x100", false},
		{"start_byte", "null", false},
		{"db_byte", "0x00", true}, {"db_byte", "0x01", true}, {"db_byte", "0x02", true},
		{"db_byte", "0x03", false}, {"db_byte", "0x0A", false}, {"db_byte", "0xFF", false}, {"db_byte", "-1", false},
		{"db_byte", "0x100", false}, {"db_byte", "null", false},
	} {
		tc := tc
		t.Run("Byte / "+tc.field+" = "+tc.value, func(t *testing.T) {
			setupSecret(t)
			doc := referenceDocument(t)
			mutateYAML(t, doc, "type_start."+tc.field, tc.value, false)
			if tc.field == "db_byte" && (tc.value == "0x01" || tc.value == "0x02") {
				mutateYAML(t, doc, "db_server.database", "homecloud", false)
				mutateYAML(t, doc, "db_server.username", "homecloud", false)
				host := "127.0.0.1"
				if tc.value == "0x02" {
					host = "db.example.internal"
				}
				mutateYAML(t, doc, "db_server.host", "\"" + host + "\"", false)
			}
			_, err := LoadAt(writeDocument(t, doc))
			if tc.accepted && err != nil {
				t.Fatalf("valid byte was rejected: %v", err)
			}
			if !tc.accepted && err == nil {
				t.Fatal("invalid/unsupported byte was accepted")
			}
		})
	}
}

func TestConfigDecoderDefences(t *testing.T) {
	for _, tc := range []struct {
		name, content string
		ok            bool
	}{
		{"empty file", "", false},
		{"invalid indentation", "server:\n  host: 127.0.0.1\n broken: true\n", false},
		{"duplicate mapping key", "server: {host: localhost, host: other}\n", false},
		{"multiple documents", "type_start: {start_byte: 0xA2}\n---\nextra: true\n", false},
		{"unknown mode", "type_start: {start_byte: 0xA3}\n", false},
	} {
		tc := tc
		t.Run("Malformed YAML / "+tc.name, func(t *testing.T) {
			setupSecret(t)
			root := t.TempDir()
			if err := os.MkdirAll(filepath.Join(root, "config"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "config", "config.yaml"), []byte(tc.content), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadAt(root)
			if err == nil {
				t.Fatal("malformed YAML was accepted")
			}
			if errors.Is(err, ErrCreated) {
				t.Fatal("existing file was incorrectly recreated")
			}
		})
	}
	t.Run("STRICT unknown keys rejected", func(t *testing.T) {
		setupSecret(t)
		doc := referenceDocument(t)
		doc.Content[0].Content = append(doc.Content[0].Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "unsupported_option"}, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"})
		if _, err := LoadAt(writeDocument(t, doc)); err == nil {
			t.Fatal("strict accepted unknown parameter")
		}
	})
	t.Run("FAST may ignore unknown noncritical settings", func(t *testing.T) {
		setupSecret(t)
		doc := referenceDocument(t)
		mutateYAML(t, doc, "type_start.start_byte", "0xA1", false)
		doc.Content[0].Content = append(doc.Content[0].Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "future_extension"}, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"})
		if _, err := LoadAt(writeDocument(t, doc)); err != nil {
			t.Fatal(err)
		}
	})
}

func TestConfigFileCreationAndProtection(t *testing.T) {
	t.Run("No directory -> create template and stop", func(t *testing.T) {
		root := t.TempDir()
		_, err := LoadAt(root)
		if !errors.Is(err, ErrCreated) {
			t.Fatalf("expected ErrCreated, got %v", err)
		}
		raw, err := os.ReadFile(filepath.Join(root, "config", "config.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(strings.ReplaceAll(string(raw), "\r\n", "\n"), "  start_byte:\n") || !strings.Contains(strings.ReplaceAll(string(raw), "\r\n", "\n"), "  db_byte:\n") {
			t.Fatal("template should keep start/db bytes empty")
		}
	})
	t.Run("Existing YAML never overwritten", func(t *testing.T) {
		root := t.TempDir()
		p := filepath.Join(root, "config", "config.yaml")
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		original := []byte("type_start:\n  start_byte: 0xA3\n")
		if err := os.WriteFile(p, original, 0600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadAt(root)
		if err == nil {
			t.Fatal("invalid start expected")
		}
		got, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(original) {
			t.Fatal("existing config modified!")
		}
	})
	t.Run("config path is a file instead of a directory", func(t *testing.T) {
		root := t.TempDir()
		if err := os.WriteFile(filepath.Join(root, "config"), []byte("not a directory"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadAt(root); err == nil {
			t.Fatal("should fail")
		}
	})
	t.Run("config.yaml path is a directory", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "config", "config.yaml"), 0700); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadAt(root); err == nil {
			t.Fatal("should fail")
		}
	})
}

func TestConfigCrossFieldAndSecurityRules(t *testing.T) {
	tests := []struct {
		name         string
		replacements map[string]string
	}{
		{"public bind without TLS", map[string]string{"server.host": "\"0.0.0.0\""}},
		{"wildcard host with reverse proxy", map[string]string{"server.host": "\"0.0.0.0\"", "server.tls.mode": "\"reverse_proxy\""}},
		{"reverse proxy without trusted CIDR", map[string]string{"server.tls.mode": "\"reverse_proxy\""}},
		{"invalid WS origin", map[string]string{"websocket.allowed_origins": "[\"file:///etc/passwd\"]"}},
		{"origin with fragment", map[string]string{"websocket.allowed_origins": "[\"https://example.org/#frag\"]"}},
		{"origin with userinfo", map[string]string{"websocket.allowed_origins": "[\"https://u:password@example.org\"]"}},
		{"origin with extra path", map[string]string{"websocket.allowed_origins": "[\"https://example.org/test\"]"}},
		{"WS IP limit larger than global", map[string]string{"websocket.max_connections_per_ip": "257"}},
		{"WS pong timeout not smaller than ping", map[string]string{"websocket.pong_timeout_seconds": "30"}},
		{"monitoring path conflicts with WS", map[string]string{"monitoring.enabled": "true", "monitoring.health_path": "\"/ws\""}},
		{"remote access unsupported", map[string]string{"security.remote_access.enabled": "true"}},
		{"Unknown database byte", map[string]string{"type_start.db_byte": "0x03"}},
		{"SQLite WAL mode invalid", map[string]string{"db_server.sqlite.journal_mode": "\"bogus\""}},
		{"SQLite foreign keys disabled", map[string]string{"db_server.sqlite.foreign_keys": "false"}},
		{"SQLite TLS nonsensical", map[string]string{"db_server.tls_enabled": "true"}},
		{"SQLite pool idle exceeds open", map[string]string{"db_server.max_idle_connections": "11"}},
		{"devices offline too soon", map[string]string{"devices.offline_after_seconds": "30"}},
		{"media WS streaming not supported", map[string]string{"media.control_only": "false"}},
		{"audio default > maximum", map[string]string{"audio.default_volume_percent": "95"}},
		{"MQTT integration unsupported", map[string]string{"integrations.mqtt.enabled": "true"}},
		{"Home Assistant unsupported", map[string]string{"integrations.home_assistant.enabled": "true"}},
		{"agent module unsupported", map[string]string{"agents.enabled": "true"}},
		{"backup module unsupported", map[string]string{"backups.enabled": "true"}},
		{"metrics endpoint unsupported", map[string]string{"monitoring.metrics_endpoint_enabled": "true"}},
		{"device discovery unsupported", map[string]string{"devices.discovery.mdns_enabled": "true"}},
		{"event history unsupported", map[string]string{"message_bus.event_history.enabled": "true"}},
		{"log secrets prohibited", map[string]string{"logging.log_secrets": "true"}},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("Rejected combination / "+tc.name, func(t *testing.T) {
			setupSecret(t)
			doc := referenceDocument(t)
			for path, value := range tc.replacements {
				mutateYAML(t, doc, path, value, false)
			}
			if _, err := LoadAt(writeDocument(t, doc)); err == nil {
				t.Fatalf("expected rejection: %v", tc.replacements)
			}
		})
	}
}


func TestConfigAllowsRedactedMessagePayloadLogging(t *testing.T) {
	for _, mode := range []string{"0xA1", "0xA2"} {
		mode := mode
		t.Run("Mode "+mode, func(t *testing.T) {
			setupSecret(t)
			doc := referenceDocument(t)
			mutateYAML(t, doc, "type_start.start_byte", mode, false)
			mutateYAML(t, doc, "logging.log_message_payloads", "true", false)

			cfg, err := LoadAt(writeDocument(t, doc))
			if err != nil {
				t.Fatalf("redacted payload logging should be accepted: %v", err)
			}
			if !cfg.Logging.LogMessagePayloads {
				t.Fatal("log_message_payloads was not preserved")
			}
		})
	}
}

func TestConfigAllowedEnumerationValues(t *testing.T) {
	for _, tc := range []struct{ path, value string }{
		{"home.units", "metric"}, {"home.units", "imperial"},
		{"logging.level", "debug"}, {"logging.level", "info"}, {"logging.level", "warn"}, {"logging.level", "error"},
		{"logging.format", "text"}, {"logging.format", "json"},
		{"db_server.sqlite.journal_mode", "WAL"}, {"db_server.sqlite.journal_mode", "DELETE"},
		{"message_bus.slow_consumer_policy", "disconnect"},
		{"server.tls.mode", "disabled"},
	} {
		tc := tc
		t.Run("Accepted enum / "+tc.path+"="+tc.value, func(t *testing.T) {
			setupSecret(t)
			doc := referenceDocument(t)
			mutateYAML(t, doc, tc.path, fmt.Sprintf("%q", tc.value), false)
			if _, err := LoadAt(writeDocument(t, doc)); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, path := range []string{
		"home.units", "logging.level", "logging.format", "logging.output",
		"db_server.sqlite.journal_mode", "message_bus.slow_consumer_policy",
	} {
		path := path
		t.Run("Rejected enum / "+path, func(t *testing.T) {
			setupSecret(t)
			doc := referenceDocument(t)
			mutateYAML(t, doc, path, "\"unknown-enum-value\"", false)
			if _, err := LoadAt(writeDocument(t, doc)); err == nil {
				t.Fatal("invalid enum was accepted")
			}
		})
	}
}

func TestConfigStrictVsFast(t *testing.T) {
	t.Run("STRICT rejects missing server address", func(t *testing.T) {
		setupSecret(t)
		doc := referenceDocument(t)
		mutateYAML(t, doc, "server.host", "\"\"", false)
		if _, err := LoadAt(writeDocument(t, doc)); err == nil {
			t.Fatal("STRICT must not supply missing host")
		}
	})
	t.Run("FAST fills missing private bind address, port and WS path", func(t *testing.T) {
		setupSecret(t)
		doc := referenceDocument(t)
		mutateYAML(t, doc, "type_start.start_byte", "0xA1", false)
		mutateYAML(t, doc, "server.host", "\"\"", false)
		mutateYAML(t, doc, "server.port", "0", false)
		mutateYAML(t, doc, "server.websocket_path", "\"\"", false)
		got, err := LoadAt(writeDocument(t, doc))
		if err != nil {
			t.Fatal(err)
		}
		if got.Server.Host != "auto_private" || got.Server.Port != 8080 || got.Server.WebSocketPath != "/ws" {
			t.Fatalf("wrong FAST fallback: %+v", got.Server)
		}
	})
	t.Run("FAST rejects unknown DB byte", func(t *testing.T) {
		setupSecret(t)
		doc := referenceDocument(t)
		mutateYAML(t, doc, "type_start.start_byte", "0xA1", false)
		mutateYAML(t, doc, "type_start.db_byte", "0x03", false)
		if _, err := LoadAt(writeDocument(t, doc)); err == nil {
			t.Fatal("FAST must reject unknown database byte")
		}
	})
}

func TestConfigBooleanFalseAndEmptyLists(t *testing.T) {
	for _, path := range allLeafPaths(t) {
		path := path
		v, err := fieldByYAMLPath(reflect.ValueOf(&Config{}), path)
		if err != nil {
			t.Fatal(err)
		}
		switch v.Kind() {
		case reflect.Bool:
			t.Run("Interpret boolean false / "+path, func(t *testing.T) {
				doc := referenceDocument(t)
				mutateYAML(t, doc, path, "false", false)
				raw, err := yaml.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				var cfg Config
				if err := yaml.Unmarshal(raw, &cfg); err != nil {
					t.Fatal(err)
				}
				got, err := fieldByYAMLPath(reflect.ValueOf(&cfg), path)
				if err != nil || got.Bool() {
					t.Fatalf("boolean false not decoded correctly: %v", err)
				}
			})
		case reflect.Slice:
			t.Run("Interpret empty list / "+path, func(t *testing.T) {
				doc := referenceDocument(t)
				mutateYAML(t, doc, path, "[]", false)
				raw, err := yaml.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				var cfg Config
				if err := yaml.Unmarshal(raw, &cfg); err != nil {
					t.Fatal(err)
				}
				got, err := fieldByYAMLPath(reflect.ValueOf(&cfg), path)
				if err != nil || got.Len() != 0 {
					t.Fatalf("empty slice not decoded correctly: %v", err)
				}
			})
		}
	}
}

func TestConfigAbsentWholeSections(t *testing.T) {
	required := map[string]bool{
		"config_version": true,
		"home":           true, "server": true, "websocket": true, "message_bus": true,
		"security": true, "db_server": true, "devices": true, "screens": true,
		"media": true, "monitoring": true, "logging": true, "audit": true, "type_start": true,
	}
	doc := referenceDocument(t)
	var sections []string
	for i := 0; i < len(doc.Content[0].Content); i += 2 {
		sections = append(sections, doc.Content[0].Content[i].Value)
	}
	for _, name := range sections {
		name := name
		t.Run("Absent section / "+name, func(t *testing.T) {
			setupSecret(t)
			doc := referenceDocument(t)
			mutateYAML(t, doc, name, "", true)
			_, err := LoadAt(writeDocument(t, doc))
			if required[name] && err == nil {
				t.Fatalf("missing required section %s was accepted", name)
			}
			if !required[name] && err != nil {
				t.Fatalf("missing optional section %s was rejected: %v", name, err)
			}
		})
	}
}

func TestConfigNativeTLSFileValidation(t *testing.T) {
	t.Run("Native TLS / corrupt PEM must be rejected", func(t *testing.T) {
		setupSecret(t)
		doc := referenceDocument(t)
		mutateYAML(t, doc, "server.tls.mode", "\"native\"", false)
		mutateYAML(t, doc, "server.tls.cert_file", "\"cert.pem\"", false)
		mutateYAML(t, doc, "server.tls.key_file", "\"key.pem\"", false)
		root := writeDocument(t, doc)
		for _, name := range []string{"cert.pem", "key.pem"} {
			if err := os.WriteFile(filepath.Join(root, name), []byte("not a PEM"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := LoadAt(root); err == nil {
			t.Fatal("invalid TLS certificate pair accepted")
		}
	})
	t.Run("Native TLS / missing cert must be rejected", func(t *testing.T) {
		setupSecret(t)
		doc := referenceDocument(t)
		mutateYAML(t, doc, "server.tls.mode", "\"native\"", false)
		mutateYAML(t, doc, "server.tls.cert_file", "\"missing-cert.pem\"", false)
		mutateYAML(t, doc, "server.tls.key_file", "\"missing-key.pem\"", false)
		if _, err := LoadAt(writeDocument(t, doc)); err == nil {
			t.Fatal("missing TLS certificate accepted")
		}
	})
	t.Run("Reverse proxy / valid trusted CIDR on loopback", func(t *testing.T) {
		setupSecret(t)
		doc := referenceDocument(t)
		mutateYAML(t, doc, "server.tls.mode", "\"reverse_proxy\"", false)
		mutateYAML(t, doc, "server.host", "\"127.0.0.1\"", false)
		mutateYAML(t, doc, "server.reverse_proxy.trusted_cidrs", "[\"127.0.0.1/32\"]", false)
		if _, err := LoadAt(writeDocument(t, doc)); err != nil {
			t.Fatal(err)
		}
	})
}

func TestConfigTypeStartLast(t *testing.T) {
	for _, source := range []string{referencePath, "template.yaml"} {
		source := source
		t.Run("type_start must be final YAML section / "+source, func(t *testing.T) {
			data, err := os.ReadFile(source)
			if err != nil {
				t.Fatal(err)
			}
			var doc yaml.Node
			if err := yaml.Unmarshal(data, &doc); err != nil {
				t.Fatal(err)
			}
			keys := doc.Content[0].Content
			if len(keys) < 2 || keys[len(keys)-2].Value != "type_start" {
				t.Fatal("type_start is not the last YAML section")
			}
		})
	}
}

func TestConfigValidConfigWithExtraDocument(t *testing.T) {
	t.Run("Valid first YAML document + second document rejected", func(t *testing.T) {
		setupSecret(t)
		doc := referenceDocument(t)
		raw, err := yaml.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "config"), 0700); err != nil {
			t.Fatal(err)
		}
		combined := append(raw, []byte("\n---\nextra_document: true\n")...)
		if err := os.WriteFile(filepath.Join(root, "config", "config.yaml"), combined, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadAt(root); err == nil {
			t.Fatal("multiple YAML docs accepted")
		}
	})
	t.Run("Duplicate known YAML key in otherwise-valid document rejected", func(t *testing.T) {
		setupSecret(t)
		doc := referenceDocument(t)
		parent, pos, err := getYAMLValue(doc, "server.port")
		if err != nil {
			t.Fatal(err)
		}
		parent.Content = append(parent.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "port"},
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: "9000"},
		)
		if parent.Content[pos+1].Value != "8080" {
			t.Fatal("test fixture modified unexpectedly")
		}
		if _, err := LoadAt(writeDocument(t, doc)); err == nil {
			t.Fatal("duplicate key accepted")
		}
	})
}

func TestConfigCredentialsDecodedFromYAML(t *testing.T) {
	for _, tc := range []struct{ path, field string }{
		{"db_server.password", "database"},
		{"integrations.mqtt.password", "mqtt"},
		{"integrations.home_assistant.token", "home_assistant"},
		{"integrations.plex.token", "plex"},
		{"backups.encryption_key", "backup"},
	} {
		tc := tc
		t.Run("Credential in YAML / "+tc.field, func(t *testing.T) {
			doc := referenceDocument(t)
			secret := "temporary-test-only-value-123"
			mutateYAML(t, doc, tc.path, fmt.Sprintf("%q", secret), false)
			cfg, err := LoadAt(writeDocument(t, doc))
			if err != nil {
				t.Fatal(err)
			}
			value, err := fieldByYAMLPath(reflect.ValueOf(cfg), tc.path)
			if err != nil || value.String() != secret {
				t.Fatalf("YAML value not interpreted: %v", err)
			}
		})
	}
}
