package telemetry

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// NetworkKernelCollector exports Linux protocol-stack counters and extended
// per-interface counters without collecting addresses, ports, or MACs.
type NetworkKernelCollector struct {
	interval time.Duration
	mu sync.Mutex
	previous map[string]networkKernelSnapshot
}
type networkKernelSnapshot struct { at time.Time; value uint64 }

func NewNetworkKernelCollector(interval time.Duration) *NetworkKernelCollector {
	if interval < 5*time.Second { interval = 5*time.Second }
	return &NetworkKernelCollector{interval:interval, previous:make(map[string]networkKernelSnapshot)}
}
func (c *NetworkKernelCollector) Name() string { return "network_kernel" }
func (c *NetworkKernelCollector) Interval() time.Duration { return c.interval }

func safeMetricPart(value string) string {
	value = strings.ToLower(value)
	var b strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' { b.WriteRune(r) } else { b.WriteByte('_') }
	}
	return strings.Trim(b.String(), "_")
}

func isNetworkGauge(protocol, counter string) bool {
	switch protocol + "." + counter {
	case "Ip.Forwarding", "Ip.DefaultTTL", "Tcp.RtoAlgorithm", "Tcp.RtoMin", "Tcp.RtoMax", "Tcp.MaxConn":
		return true
	default:
		return false
	}
}

func parseProtocolCounters(path string) (map[string]map[string]uint64, error) {
	file, err := os.Open(path); if err != nil { return nil, err }; defer file.Close()
	scanner := bufio.NewScanner(file)
	result := make(map[string]map[string]uint64)
	var header []string
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text()); if len(fields) < 2 { continue }
		if strings.HasSuffix(fields[0], ":") {
			if len(header) == 0 { header = fields; continue }
			protocol := strings.TrimSuffix(fields[0], ":")
			if len(fields) != len(header) { header = nil; continue }
			values := make(map[string]uint64)
			for i:=1; i<len(fields); i++ { if n,e:=strconv.ParseUint(fields[i],10,64); e==nil { values[header[i]]=n } }
			result[protocol]=values; header=nil
		}
	}
	if err:=scanner.Err(); err!=nil { return nil,err }
	return result,nil
}

func (c *NetworkKernelCollector) appendCounter(out *[]Sample, key, metric string, labels map[string]string, value uint64, now time.Time) {
	prev, ok := c.previous[key]
	if ok && value >= prev.value {
		seconds:=now.Sub(prev.at).Seconds()
		if seconds>0 { *out=append(*out,Sample{Name:metric+"_per_second",Labels:labels,Value:float64(value-prev.value)/seconds,Unit:"events_per_second",Timestamp:now}) }
	}
	*out=append(*out,Sample{Name:metric+"_total",Labels:labels,Value:float64(value),Unit:"events",Timestamp:now})
	c.previous[key]=networkKernelSnapshot{at:now,value:value}
}

func (c *NetworkKernelCollector) Collect(ctx context.Context) ([]Sample,error) {
	c.mu.Lock(); defer c.mu.Unlock()
	now:=time.Now().UTC(); out:=make([]Sample,0,256)
	for _,path:=range []string{"/proc/net/snmp","/proc/net/netstat"} {
		if err:=ctx.Err(); err!=nil{return out,err}
		protocols,err:=parseProtocolCounters(path)
		if err!=nil { if os.IsNotExist(err){continue}; return out,err }
		for protocol,counters:=range protocols {
			for counter,value:=range counters {
				if err:=ctx.Err();err!=nil{return out,err}
				p:=safeMetricPart(protocol); k:=safeMetricPart(counter)
				if p==""||k=="" {continue}
				metric:=fmt.Sprintf("network.stack.%s.%s",p,k)
				labels:=map[string]string{"protocol":p,"counter":k}
				if isNetworkGauge(protocol,counter) {
					out=append(out,Sample{Name:metric,Labels:labels,Value:float64(value),Unit:"value",Timestamp:now})
					continue
				}
				c.appendCounter(&out,metric,metric,labels,value,now)
			}
		}
	}
	// /proc/net/sockstat reports aggregate socket memory and allocation counters.
	if raw,err:=os.ReadFile("/proc/net/sockstat");err==nil{
		for _,line:=range strings.Split(string(raw),"\n"){
			fields:=strings.Fields(line); if len(fields)<2{continue}
			protocol:=safeMetricPart(strings.TrimSuffix(fields[0],":"))
			for i:=1;i+1<len(fields);i+=2{
				name:=safeMetricPart(fields[i]); value,e:=strconv.ParseUint(fields[i+1],10,64)
				if e!=nil||name==""||protocol==""{continue}
				out=append(out,Sample{Name:"network.sockets."+protocol+"."+name,Value:float64(value),Unit:"count",Timestamp:now})
			}
		}
	}
	interfaces,err:=filepath.Glob("/sys/class/net/*"); if err!=nil{return out,err}
	for _,path:=range interfaces{
		if err:=ctx.Err();err!=nil{return out,err}
		iface:=filepath.Base(path); labels:=map[string]string{"interface":iface}
		for _,field:=range []string{"rx_compressed","tx_compressed","multicast","collisions","rx_fifo_errors","tx_fifo_errors","rx_frame_errors","rx_length_errors","rx_crc_errors","rx_missed_errors","rx_over_errors","tx_carrier_errors","tx_aborted_errors","tx_window_errors","tx_heartbeat_errors"}{
			raw,e:=os.ReadFile(filepath.Join(path,"statistics",field));if e!=nil{continue}
			value,e:=strconv.ParseUint(strings.TrimSpace(string(raw)),10,64);if e!=nil{continue}
			metric:="network.interface."+strings.TrimSuffix(field,"_errors")
			if strings.HasSuffix(field,"_errors"){metric="network.interface."+strings.TrimSuffix(field,"_errors")+"_errors"}
			c.appendCounter(&out,"if:"+iface+":"+field,metric,labels,value,now)
		}
		for _,field:=range []string{"mtu","tx_queue_len","speed","carrier"}{
			raw,e:=os.ReadFile(filepath.Join(path,field));if e!=nil{continue}
			value,e:=strconv.ParseInt(strings.TrimSpace(string(raw)),10,64);if e!=nil||value<0{continue}
			unit:="count";if field=="speed"{unit="megabits_per_second"};if field=="mtu"{unit="bytes"}
			out=append(out,Sample{Name:"network.interface."+field,Labels:labels,Value:float64(value),Unit:unit,Timestamp:now})
		}
		if raw,e:=os.ReadFile(filepath.Join(path,"operstate"));e==nil{
			state:=strings.TrimSpace(string(raw)); up:=0.0;if state=="up"{up=1}
			out=append(out,Sample{Name:"network.interface.link_up",Labels:labels,Value:up,Unit:"boolean",Timestamp:now})
		}
		if raw,e:=os.ReadFile(filepath.Join(path,"duplex"));e==nil{
			duplex:=strings.TrimSpace(string(raw)); value:=0.0;if duplex=="full"{value=1}
			out=append(out,Sample{Name:"network.interface.full_duplex",Labels:labels,Value:value,Unit:"boolean",Timestamp:now})
		}
	}
	return out,nil
}
