// KAN-48 integration smoke: real direct TV -> Gateway WebSocket, one-use ticket,
// refresh/reconnect, connection epoch, and authoritative Backend attachment state.
// Source is synthetic; no HCM1 payload, decoder or physical Android TV needed.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/coder/websocket"
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
}
type reply struct {
	V               int             `json:"v"`
	ID              string          `json:"id"`
	Type            string          `json:"type"`
	Action          string          `json:"action"`
	Topic           string          `json:"topic"`
	Error           string          `json:"error"`
	Code            string          `json:"code"`
	Data            json.RawMessage `json:"data"`
	ConnectionEpoch uint64          `json:"connection_epoch"`
}
type offer struct {
	RoomID       string `json:"room_id"`
	PlaybackID   string `json:"playback_id"`
	Generation   uint32 `json:"generation"`
	GatewayWSURL string `json:"gateway_ws_url"`
	AttachTicket string `json:"attach_ticket"`
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func send(ctx context.Context, c *websocket.Conn, m packet) error {
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	return c.Write(ctx, websocket.MessageText, b)
}
func recv(ctx context.Context, c *websocket.Conn) (reply, error) {
	var r reply
	kind, b, e := c.Read(ctx)
	if e != nil {
		return r, e
	}
	if kind != websocket.MessageText {
		return r, errors.New("unexpected binary frame")
	}
	e = json.Unmarshal(b, &r)
	return r, e
}
func cmd(ctx context.Context, c *websocket.Conn, id, action string, data any) (reply, error) {
	p := packet{V: 1, ID: id, Type: "command", Action: action, Data: data}
	if e := send(ctx, c, p); e != nil {
		return reply{}, e
	}
	for {
		r, e := recv(ctx, c)
		if e != nil {
			return r, e
		}
		if r.Type == "event" {
			continue
		}
		if r.ID != id {
			return r, fmt.Errorf("response ID %q, expected %q", r.ID, id)
		}
		if r.Type == "error" {
			return r, fmt.Errorf("%s: %s", action, r.Error)
		}
		return r, nil
	}
}
func connect(ctx context.Context, url, id, kind string) *websocket.Conn {
	c, _, e := websocket.Dial(ctx, url, nil)
	must(e)
	c.SetReadLimit(65536)
	must(send(ctx, c, packet{V: 1, ID: "hello-" + id, Type: "hello", DeviceID: id, ClientType: kind}))
	r, e := recv(ctx, c)
	must(e)
	if r.Type != "identified" {
		log.Fatalf("hello %s: %+v", id, r)
	}
	return c
}
func dialDirect(ctx context.Context, off offer, ticket string) (*websocket.Conn, reply, error) {
	c, _, e := websocket.Dial(ctx, off.GatewayWSURL, nil)
	if e != nil {
		return nil, reply{}, e
	}
	c.SetReadLimit(16384)
	b, e := json.Marshal(map[string]any{"v": 1, "type": "media.hello", "room_id": off.RoomID, "playback_id": off.PlaybackID, "generation": off.Generation, "device_id": "kan48-tv-sim", "attach_ticket": ticket, "capabilities": map[string]any{"containers": []string{"mp4"}, "video_codecs": []string{"h264"}, "audio_codecs": []string{"aac"}, "max_width": 1920, "max_height": 1080}})
	if e == nil {
		e = c.Write(ctx, websocket.MessageText, b)
	}
	if e != nil {
		c.CloseNow()
		return nil, reply{}, e
	}
	r, e := recv(ctx, c)
	if e != nil {
		c.CloseNow()
		return nil, reply{}, e
	}
	return c, r, nil
}
func state(ctx context.Context, ui *websocket.Conn, roomID string) (string, uint64, error) {
	r, e := cmd(ctx, ui, "state-"+fmt.Sprint(time.Now().UnixNano()), "media.rooms.get", map[string]string{"room_id": roomID})
	if e != nil {
		return "", 0, e
	}
	var s struct {
		TVAttach    string `json:"tv_attach"`
		AttachEpoch uint64 `json:"attach_epoch"`
	}
	e = json.Unmarshal(r.Data, &s)
	return s.TVAttach, s.AttachEpoch, e
}
func waitState(ctx context.Context, ui *websocket.Conn, id, phase string, epoch uint64) {
	for ctx.Err() == nil {
		p, ep, e := state(ctx, ui, id)
		must(e)
		if p == phase && ep == epoch {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Fatalf("Backend did not reach %s epoch %d", phase, epoch)
}
func main() {
	backend := flag.String("backend", "ws://127.0.0.1:8080/ws", "Backend control WebSocket URL")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	ui := connect(ctx, *backend, "kan48-ui", "desktop")
	defer ui.CloseNow()
	tv := connect(ctx, *backend, "kan48-tv-sim", "tv")
	defer tv.CloseNow()
	res, e := cmd(ctx, ui, "gw-check", "media.gateway.status", map[string]any{})
	must(e)
	var gw struct {
		Online      bool   `json:"online"`
		OwnerNodeID string `json:"owner_node_id"`
	}
	must(json.Unmarshal(res.Data, &gw))
	if !gw.Online || gw.OwnerNodeID == "" {
		log.Fatal("Gateway offline")
	}
	res, e = cmd(ctx, ui, "create-room", "media.rooms.play", map[string]any{
		"command_id": fmt.Sprintf("kan48-play-%d", time.Now().UnixNano()),
		"target":     map[string]any{"node_id": gw.OwnerNodeID, "device_id": "kan48-tv-sim"},
		"source":     map[string]any{"storage_node_id": "kan48-synthetic", "file_id": "kan48-synthetic-file"}, "network": "lan",
	})
	must(e)
	event, e := recv(ctx, tv)
	must(e)
	if event.Type != "event" || event.Topic != "media.room.offer" {
		log.Fatalf("unexpected TV event %+v", event)
	}
	var o offer
	must(json.Unmarshal(event.Data, &o))
	if o.RoomID == "" || o.PlaybackID == "" || len(o.AttachTicket) != 64 || strings.Contains(o.GatewayWSURL, "127.0.0.1") {
		log.Fatalf("invalid offer %+v", o)
	}
	defer func() {
		short, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_, _ = cmd(short, ui, "cleanup", "media.rooms.stop", map[string]string{"room_id": o.RoomID, "command_id": "kan48-cleanup"})
	}()
	// Wait for Gateway to process the offer and register the ticket.
	for i := 0; i < 35; i++ {
		r, er := cmd(ctx, ui, fmt.Sprintf("get-ack-%d", i), "media.rooms.get", map[string]string{"room_id": o.RoomID})
		must(er)
		var x struct {
			Participants struct {
				Gateway string `json:"gateway"`
			} `json:"participants"`
		}
		must(json.Unmarshal(r.Data, &x))
		if x.Participants.Gateway == "APPLIED" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	bad, badReply, er := dialDirect(ctx, o, "incorrect-ticket")
	must(er)
	if badReply.Type != "media.error" || badReply.Code != "ATTACH_REJECTED" {
		log.Fatalf("wrong ticket accepted: %+v", badReply)
	}
	bad.CloseNow()
	direct, ready, er := dialDirect(ctx, o, o.AttachTicket)
	must(er)
	if ready.Type != "media.ready" || ready.ConnectionEpoch != 1 {
		log.Fatalf("unexpected media.ready: %+v", ready)
	}
	waitState(ctx, ui, o.RoomID, "READY", ready.ConnectionEpoch)
	replay, replayed, er := dialDirect(ctx, o, o.AttachTicket)
	must(er)
	if replayed.Type != "media.error" || replayed.Code != "ATTACH_REJECTED" {
		log.Fatalf("one-use replay accepted: %+v", replayed)
	}
	replay.CloseNow()
	renewal, er := cmd(ctx, tv, "refresh", "media.room.ticket", map[string]string{"room_id": o.RoomID})
	must(er)
	var fresh offer
	must(json.Unmarshal(renewal.Data, &fresh))
	fresh.GatewayWSURL = o.GatewayWSURL
	if fresh.AttachTicket == o.AttachTicket || len(fresh.AttachTicket) != 64 {
		log.Fatal("ticket was not rotated")
	}
	var newer *websocket.Conn
	var ready2 reply
	for i := 0; i < 30; i++ {
		d, r, e := dialDirect(ctx, fresh, fresh.AttachTicket)
		if e == nil && r.Type == "media.ready" {
			newer = d
			ready2 = r
			break
		}
		if d != nil {
			d.CloseNow()
		}
		time.Sleep(100 * time.Millisecond)
	}
	if newer == nil {
		log.Fatal("refresh attach never succeeded")
	}
	defer newer.CloseNow()
	if ready2.ConnectionEpoch != 2 {
		log.Fatalf("epoch not incremented: %+v", ready2)
	}
	directReadCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, oldErr := recv(directReadCtx, direct)
	cancelRead()
	direct.CloseNow()
	if oldErr == nil {
		log.Fatal("old TV socket remained active")
	}
	waitState(ctx, ui, o.RoomID, "READY", ready2.ConnectionEpoch)
	_, er = cmd(ctx, ui, "stop", "media.rooms.stop", map[string]string{"room_id": o.RoomID, "command_id": fmt.Sprintf("kan48-stop-%d", time.Now().UnixNano())})
	must(er)
	closedCtx, stopRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, closeErr := recv(closedCtx, newer)
	stopRead()
	if closeErr == nil {
		log.Fatal("room STOP did not close direct TV")
	}
	fmt.Println("KAN-48 PASS: direct TV WS, one-use ticket, wrong-token denial, Backend READY, refresh, epoch fencing, STOP cleanup")
	fmt.Println("Room:", o.RoomID, "Direct endpoint:", o.GatewayWSURL)
	fmt.Println("Note: synthetic storage ID, no media bytes, no Android TV decoder")
}
