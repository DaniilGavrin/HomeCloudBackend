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
