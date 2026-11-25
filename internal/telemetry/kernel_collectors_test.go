package telemetry

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseDiskStatsLine(t *testing.T) {
	name, values, ok := parseDiskStatsLine("8 0 sda 10 2 100 4 20 3 200 5 1 6 7 8 9 10 11")
	if !ok || name!="sda" || len(values)!=15 || values[2]!=100 || values[14]!=11 {
		t.Fatalf("unexpected parse: name=%q values=%v ok=%v",name,values,ok)
	}
	if _,_,ok:=parseDiskStatsLine("bad row");ok {t.Fatal("accepted malformed diskstats row")}
}

func TestParseProtocolCounters(t *testing.T) {
	path:=filepath.Join(t.TempDir(),"snmp")
	content:="Tcp: RtoAlgorithm RtoMin ActiveOpens PassiveOpens\nTcp: 1 200 3 4\nIp: Forwarding DefaultTTL\nIp: 2 64\n"
	if err:=os.WriteFile(path,[]byte(content),0600);err!=nil{t.Fatal(err)}
	got,err:=parseProtocolCounters(path);if err!=nil{t.Fatal(err)}
	if got["Tcp"]["ActiveOpens"]!=3||got["Ip"]["DefaultTTL"]!=64{t.Fatalf("unexpected counters: %#v",got)}
}
