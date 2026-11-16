package mediarooms

import (
	"strings"
	"testing"
)

func TestHCS1FileIDLengthWithoutRelaxingOtherIdentifiers(t *testing.T) {
	m := New(2)
	id := "hc1." + strings.Repeat("a", 600)
	base := PlayRequest{
		CommandID:"cmd-fileid-long", Network:"lan",
		Target:Target{NodeID:"backend-node",DeviceID:"tv-unit"},
		Source:Source{StorageNodeID:"storage-unit",FileID:id},
	}
	if _,err:=m.Play(base,"backend-node","gateway-id","ws://127.0.0.1:8091/ws/stream");err!=nil{
		t.Fatalf("root-scoped ID length should be accepted: %v",err)
	}
	bad:=base
	bad.CommandID="cmd-fileid-too-long"
	bad.Target.DeviceID="tv-other"
	bad.Source.FileID="hc1."+strings.Repeat("a", 2048)
	if _,err:=m.Play(bad,"backend-node","gateway-id","ws://127.0.0.1:8091/ws/stream");err==nil{
		t.Fatal("source ID over 2048 bytes accepted")
	}
	bad=base
	bad.CommandID="cmd-device-too-long"
	bad.Target.DeviceID=strings.Repeat("x",300)
	if _,err:=m.Play(bad,"backend-node","gateway-id","ws://127.0.0.1:8091/ws/stream");err==nil{
		t.Fatal("overlong device ID accepted")
	}
}
