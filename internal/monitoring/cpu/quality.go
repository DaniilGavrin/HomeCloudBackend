package cpu

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"time"
)

const (
	StatusValid           = "valid"
	StatusStale           = "stale"
	StatusPartial         = "partial"
	StatusInvalid         = "invalid"
	StatusUnknown         = "unknown"
	StatusUnavailable     = "unavailable"
	StatusUnsupported     = "unsupported"
	StatusPermissionDenied = "permission_denied"
	StatusSourceError     = "source_error"
	StatusTimeout         = "timeout"
	StatusCancelled       = "cancelled"
)

func statusItem(metricID, objectID, status, reason, errorCode, sourceID string, observedAt time.Time) MetricItem {
	item := MetricItem{
		MetricID: metricID,
		ObjectID: objectID,
		Status: status,
		Reason: reason,
		ErrorCode: errorCode,
		SourceID: sourceID,
	}
	if !observedAt.IsZero() {
		item.ObservedAt = observedAt.UTC().Format(time.RFC3339Nano)
	}
	return item
}

func classifySourceError(err error) (status, reason, code string) {
	switch {
	case err == nil:
		return StatusValid, "", ""
	case errors.Is(err, context.Canceled):
		return StatusCancelled, "cancelled", "REQUEST_CANCELLED"
	case errors.Is(err, context.DeadlineExceeded):
		return StatusTimeout, "source_timeout", "SOURCE_TIMEOUT"
	case errors.Is(err, ErrMetricUnsupported):
		return StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED"
	case errors.Is(err, fs.ErrPermission), errors.Is(err, os.ErrPermission):
		return StatusPermissionDenied, "permission_denied", "SOURCE_PERMISSION_DENIED"
	case errors.Is(err, fs.ErrNotExist):
		return StatusUnavailable, "not_present", "SOURCE_UNAVAILABLE"
	default:
		return StatusSourceError, "source_failure", "SOURCE_UNAVAILABLE"
	}
}

func hasMetricStatus(items []MetricItem, metricID, objectID string) bool {
	for _, item := range items {
		if item.MetricID == metricID && item.ObjectID == objectID {
			return true
		}
	}
	return false
}

func allPerfMetricIDs() []string {
	return []string{
		MetricPerfCycles, MetricPerfRefCycles, MetricPerfInstructions,
		MetricPerfBranches, MetricPerfBranchMisses, MetricPerfCacheReferences,
		MetricPerfCacheMisses, MetricPerfBusCycles, MetricPerfStalledFrontend,
		MetricPerfStalledBackend,
		MetricPerfL1DReadAccess, MetricPerfL1DReadMiss,
		MetricPerfL1IReadAccess, MetricPerfL1IReadMiss,
		MetricPerfL2ReadAccess, MetricPerfL2ReadMiss,
		MetricPerfLLCReadAccess, MetricPerfLLCReadMiss,
		MetricPerfDTLBReadAccess, MetricPerfDTLBReadMiss,
		MetricPerfITLBReadAccess, MetricPerfITLBReadMiss,
		MetricPerfIPC, MetricPerfBranchMissRate, MetricPerfCacheMissRate,
		MetricPerfL1DReadMissRate, MetricPerfL1IReadMissRate,
		MetricPerfL2ReadMissRate, MetricPerfLLCReadMissRate,
		MetricPerfDTLBReadMissRate, MetricPerfITLBReadMissRate,
	}
}

func selectPerfMetricIDs(targets []string) []string {
	if len(targets) == 0 {
		return allPerfMetricIDs()
	}
	selected := make(map[string]bool)
	for _, target := range targets {
		if target == MetricPerfTarget {
			return allPerfMetricIDs()
		}
		selected[target] = true
	}
	result := make([]string, 0, len(selected))
	for _, metricID := range allPerfMetricIDs() {
		if selected[metricID] {
			result = append(result, metricID)
		}
	}
	return result
}

func selectPerfEventIDs(targets []string) []string {
	selectedMetrics := selectPerfMetricIDs(targets)
	selected := make(map[string]bool)
	for _, metricID := range selectedMetrics {
		switch metricID {
		case MetricPerfIPC:
			selected[MetricPerfInstructions] = true
			selected[MetricPerfCycles] = true
		case MetricPerfBranchMissRate:
			selected[MetricPerfBranchMisses] = true
			selected[MetricPerfBranches] = true
		case MetricPerfCacheMissRate:
			selected[MetricPerfCacheMisses] = true
			selected[MetricPerfCacheReferences] = true
		case MetricPerfL1DReadMissRate:
			selected[MetricPerfL1DReadMiss] = true
			selected[MetricPerfL1DReadAccess] = true
		case MetricPerfL1IReadMissRate:
			selected[MetricPerfL1IReadMiss] = true
			selected[MetricPerfL1IReadAccess] = true
		case MetricPerfL2ReadMissRate:
			selected[MetricPerfL2ReadMiss] = true
			selected[MetricPerfL2ReadAccess] = true
		case MetricPerfLLCReadMissRate:
			selected[MetricPerfLLCReadMiss] = true
			selected[MetricPerfLLCReadAccess] = true
		case MetricPerfDTLBReadMissRate:
			selected[MetricPerfDTLBReadMiss] = true
			selected[MetricPerfDTLBReadAccess] = true
		case MetricPerfITLBReadMissRate:
			selected[MetricPerfITLBReadMiss] = true
			selected[MetricPerfITLBReadAccess] = true
		default:
			selected[metricID] = true
		}
	}
	result := make([]string, 0, len(selected))
	for _, metricID := range allPerfMetricIDs() {
		if numerator, denominator := isPerfDerivedMetric(metricID); numerator == "" && denominator == "" && selected[metricID] {
			result = append(result, metricID)
		}
	}
	return result
}

func isPerfDerivedMetric(metricID string) (string, string) {
	switch metricID {
	case MetricPerfIPC:
		return MetricPerfInstructions, MetricPerfCycles
	case MetricPerfBranchMissRate:
		return MetricPerfBranchMisses, MetricPerfBranches
	case MetricPerfCacheMissRate:
		return MetricPerfCacheMisses, MetricPerfCacheReferences
	case MetricPerfL1DReadMissRate:
		return MetricPerfL1DReadMiss, MetricPerfL1DReadAccess
	case MetricPerfL1IReadMissRate:
		return MetricPerfL1IReadMiss, MetricPerfL1IReadAccess
	case MetricPerfL2ReadMissRate:
		return MetricPerfL2ReadMiss, MetricPerfL2ReadAccess
	case MetricPerfLLCReadMissRate:
		return MetricPerfLLCReadMiss, MetricPerfLLCReadAccess
	case MetricPerfDTLBReadMissRate:
		return MetricPerfDTLBReadMiss, MetricPerfDTLBReadAccess
	case MetricPerfITLBReadMissRate:
		return MetricPerfITLBReadMiss, MetricPerfITLBReadAccess
	default:
		return "", ""
	}
}
