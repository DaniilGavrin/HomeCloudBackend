// Package devicecontracts contains the Backend's local implementation of
// the normative HomeCloud Device Contracts v1 model. Keep this package in
// parity with HomeCloudDeviceBridge/internal/devicecontracts.
package devicecontracts

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const ContractsVersion = 1

type IdentityQuality string

const (
	IdentityStable      IdentityQuality = "stable"
	IdentitySessionOnly IdentityQuality = "session_only"
	IdentityWeak        IdentityQuality = "weak"
)

type DeviceType struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
}

type Identity struct {
	IntegrationID  string          `json:"integration_id"`
	NativeDeviceID string          `json:"native_device_id"`
	Quality        IdentityQuality `json:"identity_quality"`
}

type Capability struct {
	ID      string          `json:"id"`
	Version int             `json:"version"`
	Raw     json.RawMessage `json:"-"`
}

// UnmarshalJSON retains the complete declaration. The normative capability
// schema is validated separately; unknown optional fields must not be dropped.
func (c *Capability) UnmarshalJSON(data []byte) error {
	var header struct {
		ID      string `json:"id"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return err
	}
	c.ID, c.Version = header.ID, header.Version
	c.Raw = append(c.Raw[:0], data...)
	return nil
}

type EntityDefinition struct {
	ID           string          `json:"id"`
	ValueType    string          `json:"value_type"`
	Unit         string          `json:"unit,omitempty"`
	CapabilityID string          `json:"capability_id"`
	Raw          json.RawMessage `json:"-"`
}

type Claim struct {
	ContractsVersion   int             `json:"contracts_version"`
	Identity           Identity        `json:"identity"`
	DeviceType         DeviceType      `json:"device_type"`
	Metadata           json.RawMessage `json:"metadata,omitempty"`
	CapabilityRevision int64           `json:"capability_revision"`
	Capabilities       []Capability    `json:"capabilities"`
}

var stableID = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)

func ValidateClaim(c Claim) error {
	if c.ContractsVersion != ContractsVersion {
		return fmt.Errorf("unsupported contracts_version %d", c.ContractsVersion)
	}
	if !stableID.MatchString(c.Identity.IntegrationID) {
		return errors.New("invalid integration_id")
	}
	if strings.TrimSpace(c.Identity.NativeDeviceID) == "" || len(c.Identity.NativeDeviceID) > 512 {
		return errors.New("invalid native_device_id")
	}
	switch c.Identity.Quality {
	case IdentityStable, IdentitySessionOnly, IdentityWeak:
	default:
		return errors.New("invalid identity_quality")
	}
	if !stableID.MatchString(c.DeviceType.ID) || c.DeviceType.Version < 1 {
		return errors.New("invalid device_type")
	}
	if c.CapabilityRevision < 1 {
		return errors.New("capability_revision must be positive")
	}
	if len(c.Metadata) > 0 && !json.Valid(c.Metadata) {
		return errors.New("metadata must be valid JSON")
	}
	seen := make(map[string]struct{}, len(c.Capabilities))
	for _, capability := range c.Capabilities {
		if !stableID.MatchString(capability.ID) || capability.Version < 1 {
			return errors.New("invalid capability declaration")
		}
		if _, ok := seen[capability.ID]; ok {
			return fmt.Errorf("duplicate capability %q", capability.ID)
		}
		seen[capability.ID] = struct{}{}
		if len(capability.Raw) > 0 && !json.Valid(capability.Raw) {
			return fmt.Errorf("capability %q must be valid JSON", capability.ID)
		}
	}
	return nil
}
