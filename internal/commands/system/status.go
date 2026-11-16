package system

import (
	"context"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	gnet "github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/shirou/gopsutil/v4/sensors"
)

// systemStatus формирует один снимок состояния компьютера, на котором запущен
// HomeCloudBackend. Поддерживает Windows и Linux, без shell/PowerShell.
// Ошибка отдельного источника не мешает возврату остальных метрик.
// Данные доступны клиентам из доверенной сети через WebSocket.
func Status(parent context.Context, online func() int) map[string]any {
	ctx, cancel := context.WithTimeout(parent, 4*time.Second)
	defer cancel()

	now := time.Now()
	_, tzOffset := now.Zone()

	result := map[string]any{
		"status":       "running",
		"collected_at": now.UTC().Format(time.RFC3339Nano),
		"system": map[string]any{
			"goos":                    runtime.GOOS,
			"goarch":                  runtime.GOARCH,
			"os_family":               systemFamily(runtime.GOOS),
			"timezone":                now.Location().String(),
			"timezone_offset_seconds": tzOffset,
		},
		"backend": map[string]any{
			"connections":                online(),
			"go_version":                 runtime.Version(),
			"pid":                        os.Getpid(),
			"logical_cpus_visible_to_go": runtime.NumCPU(),
			"goroutines":                 runtime.NumGoroutine(),
			"gomaxprocs":                 runtime.GOMAXPROCS(0),
			"cgo_calls":                  runtime.NumCgoCall(),
		},
	}
	problems := make(map[string]string)

	// Операционная система: дистрибутив, версия, ядро, uptime, виртуализация.
	if h, err := host.InfoWithContext(ctx); err == nil {
		system := result["system"].(map[string]any)
		system["hostname"] = h.Hostname
		system["platform"] = h.Platform
		system["platform_family"] = h.PlatformFamily
		system["platform_version"] = h.PlatformVersion
		system["kernel_version"] = h.KernelVersion
		system["kernel_arch"] = h.KernelArch
		system["uptime_seconds"] = h.Uptime
		system["boot_time_unix"] = h.BootTime
		system["process_count"] = h.Procs
		system["virtualization"] = map[string]any{
			"system": h.VirtualizationSystem,
			"role":   h.VirtualizationRole,
		}
		// Не возвращаем hostID: это устойчивый идентификатор машины.
	} else {
		problems["system"] = err.Error()
	}

	// CPU: модель, ядра, частота, кэш, текущая нагрузка и нагрузка каждого ядра.
	processor := make(map[string]any)
	if n, err := cpu.CountsWithContext(ctx, true); err == nil {
		processor["logical_cores"] = n
	} else {
		problems["cpu.logical"] = err.Error()
	}
	if n, err := cpu.CountsWithContext(ctx, false); err == nil {
		processor["physical_cores"] = n
	} else {
		problems["cpu.physical"] = err.Error()
	}
	if infos, err := cpu.InfoWithContext(ctx); err == nil && len(infos) > 0 {
		first := infos[0]
		processor["model"] = first.ModelName
		processor["vendor"] = first.VendorID
		processor["frequency_mhz"] = first.Mhz
		processor["cache_kb"] = first.CacheSize
		processor["features"] = first.Flags
	} else if err != nil {
		problems["cpu.info"] = err.Error()
	}
	// Измерение за 200 мс точнее моментального сравнения с глобальным кэшем.
	if values, err := cpu.PercentWithContext(ctx, 200*time.Millisecond, false); err == nil && len(values) > 0 {
		processor["usage_percent"] = values[0]
	} else if err != nil {
		problems["cpu.usage"] = err.Error()
	}
	if values, err := cpu.PercentWithContext(ctx, 200*time.Millisecond, true); err == nil {
		processor["usage_per_core_percent"] = values
	} else if err != nil {
		problems["cpu.per_core"] = err.Error()
	}
	result["cpu"] = processor

	// RAM / swap: байты и проценты, доступно и на Windows, и на Linux.
	if m, err := mem.VirtualMemoryWithContext(ctx); err == nil {
		result["memory"] = map[string]any{
			"total_bytes":     m.Total,
			"available_bytes": m.Available,
			"used_bytes":      m.Used,
			"free_bytes":      m.Free,
			"used_percent":    m.UsedPercent,
		}
	} else {
		problems["memory"] = err.Error()
	}
	if m, err := mem.SwapMemoryWithContext(ctx); err == nil {
		result["swap"] = map[string]any{
			"total_bytes":  m.Total,
			"used_bytes":   m.Used,
			"free_bytes":   m.Free,
			"used_percent": m.UsedPercent,
		}
	} else {
		problems["swap"] = err.Error()
	}

	// Физические разделы/тома и счётчики дисковых операций.
	partitions := make([]map[string]any, 0)
	if items, err := disk.PartitionsWithContext(ctx, false); err == nil {
		for _, p := range items {
			entry := map[string]any{
				"device":     p.Device,
				"mountpoint": p.Mountpoint,
				"filesystem": p.Fstype,
			}
			if usage, err := disk.UsageWithContext(ctx, p.Mountpoint); err == nil {
				entry["total_bytes"] = usage.Total
				entry["used_bytes"] = usage.Used
				entry["free_bytes"] = usage.Free
				entry["used_percent"] = usage.UsedPercent
				if runtime.GOOS == "linux" {
					entry["inodes_total"] = usage.InodesTotal
					entry["inodes_used"] = usage.InodesUsed
				}
			} else {
				entry["error"] = err.Error()
			}
			partitions = append(partitions, entry)
		}
	} else {
		problems["disks.partitions"] = err.Error()
	}
	diskInfo := map[string]any{"partitions": partitions}
	if ioStats, err := disk.IOCountersWithContext(ctx); err == nil {
		// Не передаём серийные номера и прочие устойчивые идентификаторы дисков.
		counters := make(map[string]any, len(ioStats))
		for name, d := range ioStats {
			counters[name] = map[string]any{
				"read_bytes":       d.ReadBytes,
				"write_bytes":      d.WriteBytes,
				"read_operations":  d.ReadCount,
				"write_operations": d.WriteCount,
			}
		}
		diskInfo["io_counters"] = counters
	} else {
		problems["disks.io"] = err.Error()
	}
	result["disks"] = diskInfo

	// Сетевые интерфейсы: адреса, MAC, состояние и счётчики трафика.
	ifaceStats := make(map[string]gnet.IOCountersStat)
	if stats, err := gnet.IOCountersWithContext(ctx, true); err == nil {
		for _, st := range stats {
			ifaceStats[st.Name] = st
		}
	} else {
		problems["network.io"] = err.Error()
	}
	interfaces := make([]map[string]any, 0)
	if all, err := net.Interfaces(); err == nil {
		for _, iface := range all {
			item := map[string]any{
				"name":     iface.Name,
				"index":    iface.Index,
				"mtu":      iface.MTU,
				"mac":      iface.HardwareAddr.String(),
				"up":       iface.Flags&net.FlagUp != 0,
				"loopback": iface.Flags&net.FlagLoopback != 0,
			}
			if addrs, err := iface.Addrs(); err == nil {
				ipList := make([]string, 0, len(addrs))
				for _, addr := range addrs {
					ipList = append(ipList, addr.String())
				}
				item["addresses"] = ipList
			} else {
				item["address_error"] = err.Error()
			}
			if st, ok := ifaceStats[iface.Name]; ok {
				item["traffic"] = map[string]any{
					"bytes_sent":       st.BytesSent,
					"bytes_received":   st.BytesRecv,
					"packets_sent":     st.PacketsSent,
					"packets_received": st.PacketsRecv,
				}
			}
			interfaces = append(interfaces, item)
		}
	} else {
		problems["network.interfaces"] = err.Error()
	}
	result["network"] = map[string]any{"interfaces": interfaces}

	// Сам процесс HomeCloudBackend (без командной строки и переменных окружения).
	backend := result["backend"].(map[string]any)
	var goMem runtime.MemStats
	runtime.ReadMemStats(&goMem)
	backend["go_heap"] = map[string]any{
		"allocated_bytes":       goMem.Alloc,
		"heap_in_use_bytes":     goMem.HeapInuse,
		"heap_objects":          goMem.HeapObjects,
		"gc_cycles":             goMem.NumGC,
		"total_allocated_bytes": goMem.TotalAlloc,
	}
	if p, err := process.NewProcessWithContext(ctx, int32(os.Getpid())); err == nil {
		if m, err := p.MemoryInfoWithContext(ctx); err == nil {
			backend["process_memory"] = map[string]any{
				"rss_bytes": m.RSS,
				"vms_bytes": m.VMS,
			}
		} else {
			problems["backend.memory"] = err.Error()
		}
		if n, err := p.NumThreadsWithContext(ctx); err == nil {
			backend["threads"] = n
		} else {
			problems["backend.threads"] = err.Error()
		}
		if created, err := p.CreateTimeWithContext(ctx); err == nil {
			backend["started_at"] = time.UnixMilli(created).UTC().Format(time.RFC3339)
			backend["uptime_seconds"] = int64(time.Since(time.UnixMilli(created)).Seconds())
		} else {
			problems["backend.uptime"] = err.Error()
		}
	} else {
		problems["backend.process"] = err.Error()
	}

	// Зависит от платформы и доступных датчиков.
	if runtime.GOOS == "linux" {
		if avg, err := load.AvgWithContext(ctx); err == nil {
			result["load_average"] = map[string]any{
				"one_minute":      avg.Load1,
				"five_minutes":    avg.Load5,
				"fifteen_minutes": avg.Load15,
			}
		} else {
			problems["load_average"] = err.Error()
		}
	}
	if temperatures, err := sensors.TemperaturesWithContext(ctx); err == nil && len(temperatures) > 0 {
		list := make([]map[string]any, 0, len(temperatures))
		for _, t := range temperatures {
			list = append(list, map[string]any{
				"sensor":           t.SensorKey,
				"celsius":          t.Temperature,
				"high_celsius":     t.High,
				"critical_celsius": t.Critical,
			})
		}
		result["temperatures"] = list
	} else if err != nil {
		problems["temperatures"] = err.Error()
	}

	if len(problems) > 0 {
		result["collection_errors"] = problems
	}
	return result
}

func systemFamily(goos string) string {
	switch goos {
	case "windows":
		return "Windows"
	case "linux":
		return "Linux"
	default:
		return goos
	}
}
