// stream-demo.go: local end-to-end streaming check, without Postman binary UI.
// Run: go run ./scripts/stream-demo.go -file path/to/sample.pdf
// Without -file, sends a generated ~256 KiB binary test fixture.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/coder/websocket"
)

type msg map[string]any

func send(ctx context.Context, c *websocket.Conn, m msg) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return c.Write(ctx, websocket.MessageText, b)
}
func recv(ctx context.Context, c *websocket.Conn) (msg, error) {
	t, b, err := c.Read(ctx)
	if err != nil {
		return nil, err
	}
	if t != websocket.MessageText {
		return nil, errors.New("expected JSON response")
	}
	var response msg
	if err := json.Unmarshal(b, &response); err != nil {
		return nil, err
	}
	if response["type"] == "error" {
		return nil, fmt.Errorf("server error: %v", response["error"])
	}
	return response, nil
}
func open(ctx context.Context, url string) (*websocket.Conn, error) {
	c, _, err := websocket.Dial(ctx, url, nil)
	return c, err
}
func attach(ctx context.Context, url, id, device, role string) (*websocket.Conn, error) {
	c, err := open(ctx, url)
	if err != nil {
		return nil, err
	}
	// coder/websocket defaults to a 32 KiB receive limit. Our binary chunks
	// are 64 KiB, so both data endpoints must explicitly allow 1 MiB messages.
	// This is a limit per WebSocket message, NOT a limit on the entire file.
	c.SetReadLimit(1 << 20)
	if err = send(ctx, c, msg{"v": 1, "type": "stream.hello", "stream_id": id, "device_id": device, "role": role}); err != nil {
		c.CloseNow()
		return nil, err
	}
	r, err := recv(ctx, c)
	if err != nil {
		c.CloseNow()
		return nil, err
	}
	if r["type"] != "stream.attached" {
		c.CloseNow()
		return nil, fmt.Errorf("unexpected stream response %+v", r)
	}
	return c, nil
}

func run() error {
	server := flag.String("server", "ws://127.0.0.1:8080/ws", "control WebSocket endpoint")
	filePath := flag.String("file", "", "optional file to stream")
	flag.Parse()
	if !strings.HasSuffix(*server, "/ws") {
		return errors.New("-server must end with /ws")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var input io.Reader
	var inputFile *os.File
	var size int64
	name := "demo.bin"
	if *filePath == "" {
		pattern := bytes.Repeat([]byte("HomeCloud binary stream test data."), 8192)
		input = bytes.NewReader(pattern)
		size = int64(len(pattern))
	} else {
		f, err := os.Open(*filePath)
		if err != nil {
			return err
		}
		defer f.Close()
		stat, err := f.Stat()
		if err != nil {
			return err
		}
		if stat.IsDir() {
			return errors.New("-file must point to a regular file")
		}
		inputFile = f
		input = f
		size = stat.Size()
		name = filepath.Base(*filePath)
	}
	_ = inputFile
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return err
	}
	id := hex.EncodeToString(suffix[:])
	from, to := "demo-source-"+id, "demo-tv-"+id
	pc, err := open(ctx, *server)
	if err != nil {
		return err
	}
	defer pc.CloseNow()
	tv, err := open(ctx, *server)
	if err != nil {
		return err
	}
	defer tv.CloseNow()
	for _, entry := range []struct {
		Conn *websocket.Conn
		ID   string
	}{{pc, from}, {tv, to}} {
		if err := send(ctx, entry.Conn, msg{"v": 1, "id": "hello-" + entry.ID, "type": "hello", "device_id": entry.ID, "client_type": "desktop"}); err != nil {
			return err
		}
		answer, err := recv(ctx, entry.Conn)
		if err != nil {
			return err
		}
		if answer["type"] != "identified" {
			return fmt.Errorf("unexpected hello %+v", answer)
		}
	}
	data := msg{"target": to, "name": name, "size_bytes": size, "mime_type": "application/octet-stream"}
	if err := send(ctx, pc, msg{"v": 1, "id": "create", "type": "command", "action": "streams.create", "data": data}); err != nil {
		return err
	}
	created, err := recv(ctx, pc)
	if err != nil {
		return err
	}
	if created["type"] != "result" {
		return fmt.Errorf("create %+v", created)
	}
	info := created["data"].(map[string]any)
	streamID := info["stream_id"].(string)
	offer, err := recv(ctx, tv)
	if err != nil {
		return err
	}
	if offer["topic"] != "streams.offer" {
		return fmt.Errorf("offer %+v", offer)
	}
	streamURL := strings.TrimSuffix(*server, "/ws") + "/ws/stream"
	receiver, err := attach(ctx, streamURL, streamID, to, "receiver")
	if err != nil {
		return err
	}
	defer receiver.CloseNow()
	sender, err := attach(ctx, streamURL, streamID, from, "sender")
	if err != nil {
		return err
	}
	defer sender.CloseNow()
	for _, c := range []*websocket.Conn{receiver, sender} {
		r, err := recv(ctx, c)
		if err != nil {
			return err
		}
		if r["type"] != "stream.ready" {
			return fmt.Errorf("not ready %+v", r)
		}
	}
	temp, err := os.CreateTemp("", "homecloud-stream-demo-*")
	if err != nil {
		return err
	}
	defer func() { temp.Close(); os.Remove(temp.Name()) }()
	type gotResult struct {
		bytes int64
		hash  string
		err   error
	}
	received := make(chan gotResult, 1)
	go func() {
		h := sha256.New()
		var n int64
		for {
			kind, reader, err := receiver.Reader(ctx)
			if err != nil {
				received <- gotResult{err: err}
				return
			}
			if kind == websocket.MessageBinary {
				copied, e := io.Copy(io.MultiWriter(temp, h), reader)
				if e != nil {
					received <- gotResult{err: e}
					return
				}
				n += copied
				continue
			}
			b, e := io.ReadAll(io.LimitReader(reader, 2048))
			if e != nil {
				received <- gotResult{err: e}
				return
			}
			var done msg
			if json.Unmarshal(b, &done) != nil || done["type"] != "stream.complete" {
				received <- gotResult{err: fmt.Errorf("unexpected completion: %s", b)}
				return
			}
			received <- gotResult{bytes: n, hash: hex.EncodeToString(h.Sum(nil))}
			return
		}
	}()
	h := sha256.New()
	dataBuffer := make([]byte, 64*1024)
	for {
		n, e := input.Read(dataBuffer)
		if n > 0 {
			if _, hErr := h.Write(dataBuffer[:n]); hErr != nil {
				return hErr
			}
			if err := sender.Write(ctx, websocket.MessageBinary, dataBuffer[:n]); err != nil {
				return err
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
	}
	if err := send(ctx, sender, msg{"v": 1, "type": "stream.end"}); err != nil {
		return err
	}
	senderDone, err := recv(ctx, sender)
	if err != nil {
		return err
	}
	if senderDone["type"] != "stream.complete" {
		return fmt.Errorf("sender completion %+v", senderDone)
	}
	result := <-received
	if result.err != nil {
		return result.err
	}
	expected := hex.EncodeToString(h.Sum(nil))
	if result.bytes != size || result.hash != expected {
		return fmt.Errorf("BAD TRANSFER: size %d/%d, sha256 %s/%s", result.bytes, size, result.hash, expected)
	}
	fmt.Printf("[OK] Relayed %d bytes from %s to %s\n[SHA256] %s\n", size, from, to, expected)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "[ERROR]", err)
		os.Exit(1)
	}
}
