package devices

import (
	"context"
	"encoding/json"
	"testing"

	"HomeCloudBackend/internal/commands"
	devicestore "HomeCloudBackend/internal/devices"
)

func TestDevicesList(t *testing.T) {
	r := commands.NewRegistry()
	Register(r, Dependencies{List: func(context.Context) ([]devicestore.Record, error) {
		return []devicestore.Record{{DeviceID:"storage-1", ClientType:"storage_service", Name:"Сервер хранения", Online:true}}, nil
	}})
	out := r.Dispatch(context.Background(), commands.Inbound{V:1, ID:"list-1", Type:"command", Action:"devices.list"}, commands.ClientContext{})
	if out.Type != "result" || out.Action != "devices.list" { t.Fatalf("unexpected response: %+v", out) }
	data, err := json.Marshal(out.Data)
	if err != nil { t.Fatal(err) }
	var items []devicestore.Record
	if err := json.Unmarshal(data, &items); err != nil { t.Fatal(err) }
	if len(items) != 1 || items[0].Name != "Сервер хранения" || !items[0].Online { t.Fatalf("unexpected devices: %+v", items) }
}

func TestDevicesRenamePublishesUpdatedRecord(t *testing.T) {
	r := commands.NewRegistry()
	var publishedTopic string
	Register(r, Dependencies{
		Rename: func(_ context.Context, id, name string) (devicestore.Record, error) {
			return devicestore.Record{DeviceID:id, ClientType:"desktop", Name:name}, nil
		},
		Publish: func(_ context.Context, topic string, payload any) error { publishedTopic = topic; return nil },
	})
	out := r.Dispatch(context.Background(), commands.Inbound{V:1, ID:"rename-1", Type:"command", Action:"devices.rename", Data:json.RawMessage(`{"device_id":"desktop-1","name":"Рабочий компьютер"}`)}, commands.ClientContext{})
	if out.Type != "result" || publishedTopic != "devices.updated" { t.Fatalf("response=%+v topic=%q", out, publishedTopic) }
}
