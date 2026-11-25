package wsconfig

import (
	"fmt"
	"reflect"
	"strings"
)

// NumericBounds is the explicit contract of numeric settings in STRICT mode.
// Disabled subsystems may use zero where their documented settings are optional.
// start_byte and db_byte are 8-bit enums; those are checked separately.
type NumericBound struct{ Min, Max int64 }

var NumericBounds = map[string]NumericBound{
	"agents.heartbeat_interval_seconds":                      {0, 3600},
	"agents.offline_after_seconds":                           {0, 86400},
	"audio.default_volume_percent":                           {0, 100},
	"audio.max_volume_percent":                               {0, 100},
	"audit.retention_days":                                   {1, 3650},
	"automations.default_action_timeout_seconds":             {0, 86400},
	"automations.max_concurrent_actions":                     {0, 10000},
	"backups.keep_last":                                      {0, 10000},
	"config_version":                                         {1, 1},
	"db_server.connection_max_idle_seconds":                  {0, 604800},
	"db_server.connection_max_lifetime_seconds":              {0, 604800},
	"db_server.connection_timeout":                           {1, 3600},
	"db_server.max_idle_connections":                         {0, 100000},
	"db_server.max_open_connections":                         {1, 100000},
	"db_server.port":                                         {0, 65535},
	"db_server.sqlite.busy_timeout_ms":                       {0, 300000},
	"devices.heartbeat_interval_seconds":                     {1, 3600},
	"devices.offline_after_seconds":                          {2, 86400},
	"device_bridge.startup_timeout_seconds":                    {1, 180},
	"device_bridge.shutdown_timeout_seconds":                   {1, 120},
	"device_bridge.heartbeat_timeout_seconds":                 {15, 600},
	"device_bridge.initial_backoff_seconds":                   {1, 60},
	"device_bridge.max_backoff_seconds":                       {1, 600},
	"integrations.mqtt.port":                                 {0, 65535},
	"integrations.mqtt.qos":                                  {0, 2},
	"media_gateway.startup_timeout_seconds":                  {0, 120},
	"media_gateway.shutdown_timeout_seconds":                 {0, 120},
	"media_gateway.max_restarts":                             {0, 10},
	"media_gateway.restart_backoff_seconds":                  {0, 120},
	"message_bus.command_ack_timeout_seconds":                {1, 3600},
	"message_bus.deduplication_window_seconds":               {0, 86400},
	"message_bus.event_history.retention_days":               {0, 3650},
	"message_bus.max_pending_commands_per_client":            {1, 100000},
	"monitoring.metrics_retention_days":                      {0, 3650},
	"monitoring.sample_interval_seconds":                     {1, 86400},
	"notifications.deduplication_window_seconds":             {0, 86400},
	"screens.default_content_timeout_seconds":                {1, 86400},
	"security.critical_actions.confirmation_timeout_seconds": {1, 3600},
	"security.pairing.max_attempts":                          {1, 1000},
	"security.pairing.pairing_window_seconds":                {1, 3600},
	"server.port":                           {1, 65535},
	"server.read_header_timeout_seconds":    {1, 300},
	"server.shutdown_timeout_seconds":       {1, 3600},
	"storage.max_upload_size_mb":            {0, 1048576},
	"websocket.max_connections":             {1, 100000},
	"websocket.hello_timeout_seconds":       {1, 300},
	"websocket.max_connections_per_ip":      {1, 100000},
	"websocket.max_message_bytes":           {256, 4194304},
	"websocket.max_messages_per_second":     {1, 100000},
	"websocket.max_outbound_queue_messages": {1, 100000},
	"websocket.ping_interval_seconds":       {2, 3600},
	"websocket.pong_timeout_seconds":        {1, 3599},
	"websocket.protocol_version":            {1, 1},
	"websocket.write_timeout_seconds":       {1, 300},
}

func fieldByYAMLPath(value reflect.Value, path string) (reflect.Value, error) {
	for _, part := range strings.Split(path, ".") {
		for value.Kind() == reflect.Pointer {
			if value.IsNil() {
				return reflect.Value{}, fmt.Errorf("%s: nil pointer", path)
			}
			value = value.Elem()
		}
		if value.Kind() != reflect.Struct {
			return reflect.Value{}, fmt.Errorf("%s: expected struct", path)
		}
		typ := value.Type()
		found := false
		for i := 0; i < typ.NumField(); i++ {
			if strings.Split(typ.Field(i).Tag.Get("yaml"), ",")[0] == part {
				value = value.Field(i)
				found = true
				break
			}
		}
		if !found {
			return reflect.Value{}, fmt.Errorf("%s: YAML field not defined", path)
		}
	}
	return value, nil
}

func validateNumericBounds(c *Config) error {
	for key, bound := range NumericBounds {
		field, err := fieldByYAMLPath(reflect.ValueOf(c), key)
		if err != nil {
			return err
		}
		if field.Kind() != reflect.Int {
			return fmt.Errorf("%s: expected integer", key)
		}
		value := field.Int()
		if value < bound.Min || value > bound.Max {
			return fmt.Errorf("%s: %d не входит в диапазон %d..%d", key, value, bound.Min, bound.Max)
		}
	}
	return nil
}
