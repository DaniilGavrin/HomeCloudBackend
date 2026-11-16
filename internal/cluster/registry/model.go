// Package registry is the durable projection of the HomeCloud Cluster node
// directory. It deliberately contains neither an HTTP/WS handler nor a
// consensus implementation. Future Raft code must apply committed commands
// through Store.ApplyCommitted; user input must never call it directly.
package registry

import (
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const MaxEndpoints = 16

var (
	ErrNotFound         = errors.New("cluster registry: node not found")
	ErrNodeExists       = errors.New("cluster registry: node already exists")
	ErrNodeRemoved      = errors.New("cluster registry: node was removed; rejoin requires an explicit future protocol")
	ErrRevisionConflict = errors.New("cluster registry: stale expected revision")
	ErrClusterMismatch  = errors.New("cluster registry: cluster ID mismatch")
	ErrOperationIDReuse = errors.New("cluster registry: operation ID reused with different content")
	ErrSchemaMissing    = errors.New("cluster registry: schema missing; run go run . init-db")
)

var idPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{2,127}$`)
var nodeIDPattern = regexp.MustCompile(`^hc-[0-9a-f]{32}$`)

// Address is a network endpoint for a client connecting directly to a Node.
// A Node may advertise several routes; no address is used as its identity.
// These are configuration records, not proof of endpoint reachability.
type Endpoint struct {
	Network  string `json:"network"`  // lan | tailscale | custom
	Protocol string `json:"protocol"` // ws | wss
	Host     string `json:"host"`     // IPv6 without [] or zone; DNS or IPv4
	Port     int    `json:"port"`
	Priority int    `json:"priority"` // 0..1000, larger is preferred
}

// NodeRecord contains only replicated membership metadata. No local files,
// IoT devices, metrics, credentials or live online state are included.
type NodeRecord struct {
	NodeID     string     `json:"node_id"`
	ClusterID  string     `json:"cluster_id"`
	HomeID     string     `json:"home_id,omitempty"`
	Name       string     `json:"name"`
	Membership string     `json:"membership"` // pending | active | removed
	Endpoints  []Endpoint `json:"endpoints"`
	Revision   int64      `json:"revision"`
	Removed    bool       `json:"removed"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

type ChangeType string

const (
	ChangeAdd    ChangeType = "add"
	ChangeUpdate ChangeType = "update"
	ChangeRemove ChangeType = "remove"
)

// Command is a deterministic, pre-authorized change submitted AFTER consensus
// has committed it (KAN-7). Timestamp must come from the log, not time.Now()
// inside the state machine. This package performs structural validation only;
// authorization, trust and Raft are separate work in KAN-7/KAN-8/KAN-9.
type Command struct {
	OperationID      string     `json:"operation_id"`
	ClusterID        string     `json:"cluster_id"`
	ExpectedRevision int64      `json:"expected_revision"`
	Type             ChangeType `json:"type"`
	Node             NodeRecord `json:"node,omitempty"`
	NodeID           string     `json:"node_id,omitempty"` // remove only
	Timestamp        time.Time  `json:"timestamp"`
}

type ApplyResult struct {
	Revision int64 `json:"revision"`
	Replayed bool  `json:"replayed"`
}

type Snapshot struct {
	ClusterID string       `json:"cluster_id"`
	Revision  int64        `json:"revision"`
	Nodes     []NodeRecord `json:"nodes"`
}

func validateCommand(cmd *Command) error {
	cmd.OperationID = strings.TrimSpace(cmd.OperationID)
	cmd.ClusterID = strings.TrimSpace(cmd.ClusterID)
	if !idPattern.MatchString(cmd.OperationID) || !idPattern.MatchString(cmd.ClusterID) {
		return errors.New("invalid operation_id or cluster_id")
	}
	if cmd.ExpectedRevision < 0 || cmd.Timestamp.IsZero() {
		return errors.New("expected_revision >= 0 and nonzero timestamp required")
	}
	cmd.Timestamp = cmd.Timestamp.UTC()
	switch cmd.Type {
	case ChangeAdd, ChangeUpdate:
		if cmd.NodeID != "" {
			return errors.New("node_id is only for removal")
		}
		cmd.Node.ClusterID = strings.TrimSpace(cmd.Node.ClusterID)
		cmd.Node.Name = strings.TrimSpace(cmd.Node.Name)
		cmd.Node.HomeID = strings.TrimSpace(cmd.Node.HomeID)
		if cmd.Node.ClusterID != cmd.ClusterID {
			return ErrClusterMismatch
		}
		if !nodeIDPattern.MatchString(cmd.Node.NodeID) {
			return errors.New("invalid node_id")
		}
		if !utf8.ValidString(cmd.Node.Name) || cmd.Node.Name == "" || len(cmd.Node.Name) > 160 {
			return errors.New("invalid node name")
		}
		if cmd.Node.HomeID != "" && !idPattern.MatchString(cmd.Node.HomeID) {
			return errors.New("invalid home_id")
		}
		if cmd.Node.Membership != "active" && cmd.Node.Membership != "pending" {
			return errors.New("membership must be pending or active")
		}
		if cmd.Node.Removed || cmd.Node.Revision != 0 || !cmd.Node.CreatedAt.IsZero() || !cmd.Node.UpdatedAt.IsZero() {
			return errors.New("node contains server-managed fields")
		}
		if len(cmd.Node.Endpoints) > MaxEndpoints {
			return fmt.Errorf("too many endpoints: maximum %d", MaxEndpoints)
		}
		if cmd.Type == ChangeAdd && len(cmd.Node.Endpoints) == 0 {
			return errors.New("new node requires at least one endpoint")
		}
		seen := map[string]struct{}{}
		for i := range cmd.Node.Endpoints {
			ep := &cmd.Node.Endpoints[i]
			if err := validateEndpoint(ep); err != nil {
				return fmt.Errorf("endpoint %d: %w", i, err)
			}
			key := ep.Network + "|" + ep.Protocol + "|" + ep.Host + "|" + fmt.Sprint(ep.Port)
			if _, ok := seen[key]; ok {
				return fmt.Errorf("duplicate endpoint: %s", key)
			}
			seen[key] = struct{}{}
		}
	case ChangeRemove:
		if !nodeIDPattern.MatchString(cmd.NodeID) {
			return errors.New("invalid node_id for removal")
		}
		if cmd.Node.NodeID != "" || cmd.Node.Name != "" || len(cmd.Node.Endpoints) != 0 {
			return errors.New("remove must not contain a node record")
		}
	default:
		return fmt.Errorf("invalid change type %q", cmd.Type)
	}
	return nil
}

func validateEndpoint(ep *Endpoint) error {
	ep.Host = strings.TrimSpace(ep.Host)
	if ep.Network != "lan" && ep.Network != "tailscale" && ep.Network != "custom" {
		return errors.New("unsupported network")
	}
	if ep.Protocol != "ws" && ep.Protocol != "wss" {
		return errors.New("unsupported protocol")
	}
	if ep.Port < 1 || ep.Port > 65535 || ep.Priority < 0 || ep.Priority > 1000 {
		return errors.New("port/priority out of range")
	}
	if addr, err := netip.ParseAddr(ep.Host); err == nil {
		if addr.Zone() != "" {
			return errors.New("IP zone is not allowed")
		}
		ep.Host = addr.String()
		return nil
	}
	if len(ep.Host) == 0 || len(ep.Host) > 253 || strings.ContainsAny(ep.Host, "/:@?#[ ]\\\t\r\n") {
		return errors.New("invalid DNS name or raw IP address")
	}
	for _, label := range strings.Split(ep.Host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return errors.New("invalid DNS label")
		}
		for _, b := range []byte(label) {
			if (b < 'a' || b > 'z') && (b < 'A' || b > 'Z') && (b < '0' || b > '9') && b != '-' {
				return errors.New("invalid DNS label characters")
			}
		}
	}
	ep.Host = strings.ToLower(ep.Host)
	return nil
}
