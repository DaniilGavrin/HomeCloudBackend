package wsserver

import (
 "encoding/json"
 "testing"
)

func TestStorageFileEndpointValidation(t *testing.T) {
 good:=[]string{
  `{"endpoint":"ws://192.168.0.147:8093/ws/files","mode":"trusted_network"}`,
  `{"endpoint":"ws://10.8.1.3:8093/ws/files"}`,
  `{"endpoint":"ws://100.114.58.74:8093/ws/files"}`,
  `{"endpoint":"ws://192.168.0.147:8093/ws/files","endpoints":["ws://192.168.0.147:8093/ws/files","ws://100.114.58.74:8093/ws/files"],"mode":"trusted_network"}`,
 }
 for _,raw:=range good {
  if err:=validateStorageFileWS(json.RawMessage(raw));err!=nil{t.Fatalf("rejected auto-discovered file WS: %v (%s)",err,raw)}
 }
 bad:=[]string{
  `{"endpoint":"ws://8.8.8.8:8093/ws/files"}`,
  `{"endpoint":"wss://example.com:443/ws/files"}`,
  `{"endpoint":"ws://192.168.0.147:8093/ws/files?secret=x"}`,
  `{"endpoint":"ws://192.168.0.147:8093/evil"}`,
  `{"endpoint":"ws://192.168.0.147:8093/ws/files","mode":"token"}`,
  `{"endpoint":"ws://192.168.0.147:8093/ws/files","mode":"trusted_network","token":"should-not-exist"}`,
  `{"endpoint":"ws://192.168.0.147:8093/ws/files","endpoints":["ws://8.8.8.8:8093/ws/files"]}`,
  `{"endpoint":"ws://192.168.0.147:8093/ws/files","endpoints":["ws://100.114.58.74:8093/ws/files"]}`,
  `{"endpoint":"ws://192.168.0.147:8093/ws/files","endpoints":["ws://192.168.0.147:8093/ws/files","ws://192.168.0.147:8093/ws/files"]}`,
  `{"endpoint":"ws://192.168.0.147:8093/ws/files","endpoints":[],"auth_mode":"trusted_network"}`,
  `{"endpoint":"ws://192.168.0.147:8093/ws/files","endpoints":["ws://192.168.0.147:8093/ws/files","ws://public.example.com:8093/ws/files"]}`,
 }
 for _,raw:=range bad {
  if err:=validateStorageFileWS(json.RawMessage(raw));err==nil{t.Fatalf("unsafe endpoint accepted: %s",raw)}
 }
}
