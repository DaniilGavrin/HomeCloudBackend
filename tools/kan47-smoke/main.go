// KAN-47 integration smoke: two UIs, a simulated TV, and the real Gateway.
// The source ID is intentionally synthetic: no actual media is read or played.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/coder/websocket"
	"log"
	"strings"
	"time"
)

type packet struct {
	V          int    `json:"v"`
	ID         string `json:"id,omitempty"`
	Type       string `json:"type"`
	Action     string `json:"action,omitempty"`
	Topic      string `json:"topic,omitempty"`
	DeviceID   string `json:"device_id,omitempty"`
	ClientType string `json:"client_type,omitempty"`
	Data       any    `json:"data,omitempty"`
	Error      string `json:"error,omitempty"`
}
type incoming struct {
	V      int             `json:"v"`
	ID     string          `json:"id"`
	Type   string          `json:"type"`
	Action string          `json:"action"`
	Topic  string          `json:"topic"`
	Error  string          `json:"error"`
	Data   json.RawMessage `json:"data"`
}

func write(ctx context.Context, c *websocket.Conn, p packet) error {
	b, e := json.Marshal(p)
	if e != nil {
		return e
	}
	return c.Write(ctx, websocket.MessageText, b)
}
func read(ctx context.Context, c *websocket.Conn) (incoming, error) {
	var p incoming
	kind, b, e := c.Read(ctx)
	if e != nil {
		return p, e
	}
	if kind != websocket.MessageText {
		return p, fmt.Errorf("binary on control WS")
	}
	e = json.Unmarshal(b, &p)
	return p, e
}
func request(ctx context.Context, c *websocket.Conn, p packet) (incoming, error) {
	if e := write(ctx, c, p); e != nil {
		return incoming{}, e
	}
	for {
		in, e := read(ctx, c)
		if e != nil {
			return in, e
		}
		if in.Type == "event" {
			continue
		}
		if in.ID != p.ID {
			return in, fmt.Errorf("unexpected response %q vs %q", in.ID, p.ID)
		}
		if in.Type == "error" {
			return in, fmt.Errorf("%s: %s", p.Action, in.Error)
		}
		return in, nil
	}
}
func command(ctx context.Context, c *websocket.Conn, id, action string, payload any) (incoming, error) {
	return request(ctx, c, packet{V: 1, ID: id, Type: "command", Action: action, Data: payload})
}
func connect(ctx context.Context, url, id, kind string) (*websocket.Conn, error) {
	c, _, e := websocket.Dial(ctx, url, nil)
	if e != nil {
		return nil, e
	}
	c.SetReadLimit(65536)
	r, e := request(ctx, c, packet{V: 1, ID: "hello-" + id, Type: "hello", DeviceID: id, ClientType: kind})
	if e != nil || r.Type != "identified" {
		c.CloseNow()
		return nil, fmt.Errorf("hello %s: %+v %v", id, r, e)
	}
	return c, nil
}
func must(e error) {
	if e != nil {
		log.Fatal(e)
	}
}
func main() {
	url := flag.String("url", "ws://127.0.0.1:8080/ws", "Backend control WebSocket URL")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	ui, mErr := connect(ctx, *url, "kan47-ui-main", "desktop")
	must(mErr)
	defer ui.CloseNow()
	ui2, mErr := connect(ctx, *url, "kan47-ui-second", "desktop")
	must(mErr)
	defer ui2.CloseNow()
	tv, mErr := connect(ctx, *url, "kan47-tv-sim", "tv")
	must(mErr)
	defer tv.CloseNow()
	r, mErr := request(ctx, ui2, packet{V: 1, ID: "sub-changes", Type: "subscribe", Topic: "media.room.changed"})
	must(mErr)
	if r.Type != "subscribed" {
		log.Fatal("subscription failed")
	}
	var gateway struct {
		Online      bool   `json:"online"`
		OwnerNodeID string `json:"owner_node_id"`
		MediaReady  bool   `json:"media_ready"`
	}
	for i := 0; i < 15; i++ {
		r, mErr = command(ctx, ui, "status-"+fmt.Sprint(i), "media.gateway.status", map[string]any{})
		must(mErr)
		must(json.Unmarshal(r.Data, &gateway))
		if gateway.Online {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !gateway.Online || gateway.OwnerNodeID == "" {
		log.Fatal("Gateway is not registered")
	}
	req := map[string]any{"command_id": "kan47-smoke-start-" + fmt.Sprint(time.Now().UnixNano()), "target": map[string]string{"node_id": gateway.OwnerNodeID, "device_id": "kan47-tv-sim"}, "source": map[string]string{"storage_node_id": "kan47-test-storage", "file_id": "kan47-test-file"}, "network": "lan"}
	r, mErr = command(ctx, ui, "play-req", "media.rooms.play", req)
	must(mErr)
	var started struct {
		RoomID        string `json:"room_id"`
		PlaybackID    string `json:"playback_id"`
		CommandStatus string `json:"command_status"`
		RoomState     string `json:"room_state"`
	}
	must(json.Unmarshal(r.Data, &started))
	if started.CommandStatus != "accepted" || started.RoomID == "" || started.PlaybackID == "" {
		log.Fatalf("bad accepted: %+v", started)
	}
	offer, mErr := read(ctx, tv)
	must(mErr)
	if offer.Type != "event" || offer.Topic != "media.room.offer" {
		log.Fatalf("unexpected TV packet: %+v", offer)
	}
	var offered struct {
		RoomID       string `json:"room_id"`
		PlaybackID   string `json:"playback_id"`
		GatewayWSURL string `json:"gateway_ws_url"`
	}
	must(json.Unmarshal(offer.Data, &offered))
	if offered.RoomID != started.RoomID || offered.PlaybackID != started.PlaybackID || !strings.Contains(offered.GatewayWSURL, ":8091/ws/stream") || strings.Contains(offered.GatewayWSURL, "127.0.0.1") {
		log.Fatalf("invalid TV offer: %+v", offered)
	}
	uiEvent, mErr := read(ctx, ui2)
	must(mErr)
	if uiEvent.Type != "event" || uiEvent.Topic != "media.room.changed" {
		log.Fatalf("second panel did not receive room state: %+v", uiEvent)
	}
	r, mErr = command(ctx, tv, "tv-apply", "media.room.participant", map[string]any{"room_id": started.RoomID, "playback_id": started.PlaybackID, "participant": "tv", "phase": "APPLIED"})
	must(mErr)
	active := false
	for i := 0; i < 20; i++ {
		r, mErr = command(ctx, ui, "get-"+fmt.Sprint(i), "media.rooms.get", map[string]string{"room_id": started.RoomID})
		must(mErr)
		var snap struct {
			RoomState     string `json:"room_state"`
			PlaybackState string `json:"playback_state"`
			Participants  struct {
				TV      string `json:"tv"`
				Gateway string `json:"gateway"`
			} `json:"participants"`
		}
		must(json.Unmarshal(r.Data, &snap))
		if snap.RoomState == "ACTIVE" && snap.Participants.TV == "APPLIED" && snap.Participants.Gateway == "APPLIED" {
			active = true
			if snap.PlaybackState == "PLAYING" {
				log.Fatal("incorrectly claims media playback is playing")
			}
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !active {
		log.Fatal("Gateway/TV apply not received before deadline")
	}
	// Switch the film within the existing room: same room_id, new playback_id.
	switchRequest := map[string]any{"room_id": started.RoomID, "command_id": "kan47-switch-" + fmt.Sprint(time.Now().UnixNano()), "target": map[string]string{"node_id": gateway.OwnerNodeID, "device_id": "kan47-tv-sim"}, "source": map[string]string{"storage_node_id": "kan47-test-storage", "file_id": "kan47-test-file-2"}, "network": "lan"}
	switchedReply, swErr := command(ctx, ui, "switch-req", "media.rooms.play", switchRequest)
	must(swErr)
	var switched struct {
		RoomID     string `json:"room_id"`
		PlaybackID string `json:"playback_id"`
	}
	must(json.Unmarshal(switchedReply.Data, &switched))
	if switched.RoomID != started.RoomID || switched.PlaybackID == started.PlaybackID {
		log.Fatal("room changed during film switch")
	}
	nextOffer, swErr := read(ctx, tv)
	must(swErr)
	if nextOffer.Type != "event" || nextOffer.Topic != "media.room.offer" {
		log.Fatalf("expected switch offer: %+v", nextOffer)
	}
	_, swErr = command(ctx, tv, "tv-switch-applied", "media.room.participant", map[string]any{"room_id": switched.RoomID, "playback_id": switched.PlaybackID, "participant": "tv", "phase": "APPLIED"})
	must(swErr)
	switchApplied := false
	for i := 0; i < 20; i++ {
		r, swErr = command(ctx, ui, "switch-get-"+fmt.Sprint(i), "media.rooms.get", map[string]string{"room_id": switched.RoomID})
		must(swErr)
		var state struct {
			RoomID       string `json:"room_id"`
			PlaybackID   string `json:"playback_id"`
			Generation   uint32 `json:"generation"`
			Participants struct {
				TV      string `json:"tv"`
				Gateway string `json:"gateway"`
			} `json:"participants"`
		}
		must(json.Unmarshal(r.Data, &state))
		if state.PlaybackID == switched.PlaybackID && state.Generation == 2 && state.Participants.Gateway == "APPLIED" && state.Participants.TV == "APPLIED" {
			switchApplied = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !switchApplied {
		log.Fatal("Gateway/TV switch not applied")
	}
	r, mErr = command(ctx, ui, "stop-room", "media.rooms.stop", map[string]string{"room_id": started.RoomID, "command_id": "kan47-smoke-stop-" + fmt.Sprint(time.Now().UnixNano())})
	must(mErr)
	stoppedTV, closedErr := read(ctx, tv)
	must(closedErr)
	if stoppedTV.Type != "event" || stoppedTV.Topic != "media.room.closed" {
		log.Fatalf("TV did not receive STOP event: %+v", stoppedTV)
	}
	r, mErr = command(ctx, ui, "after-stop", "media.rooms.list", map[string]any{})
	must(mErr)
	var list []json.RawMessage
	must(json.Unmarshal(r.Data, &list))
	if len(list) != 0 {
		log.Fatalf("rooms leaked: %+v", list)
	}
	fmt.Println("KAN-47 PASS: two UI clients, Gateway+TV offers, LAN endpoint, acknowledgements, authoritative ACTIVE, STOP cleanup")
	fmt.Println("Room:", started.RoomID, "TV Gateway URL:", offered.GatewayWSURL)
	fmt.Println("Note: synthetic source; no real storage access or media playback")
}
