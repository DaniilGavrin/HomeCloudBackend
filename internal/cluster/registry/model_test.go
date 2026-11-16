package registry

import (
	"errors"
	"testing"
	"time"
)

const testNodeID1 = "hc-11111111111111111111111111111111"
const testNodeID2 = "hc-22222222222222222222222222222222"
const testClusterID = "hc-cluster-demo"

func testNode(nodeID string) NodeRecord {
	return NodeRecord{NodeID: nodeID, ClusterID: testClusterID, HomeID: "home-kostroma", Name: "Кострома", Membership: "active", Endpoints: []Endpoint{{Network: "tailscale", Protocol: "ws", Host: "100.82.22.45", Port: 8080, Priority: 100}, {Network: "lan", Protocol: "ws", Host: "192.168.39.11", Port: 8080, Priority: 50}}}
}
func testCommand(op string, kind ChangeType, expected int64, n NodeRecord) Command {
	return Command{OperationID: op, ClusterID: testClusterID, ExpectedRevision: expected, Type: kind, Node: n, Timestamp: time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)}
}
func TestValidationNormalizesIPv6AndDNS(t *testing.T) {
	cmd := testCommand("operation-1", ChangeAdd, 0, testNode(testNodeID1))
	cmd.Node.Endpoints = []Endpoint{{Network: "tailscale", Protocol: "wss", Host: "FD7A:115C:A1E0::1", Port: 8080}, {Network: "custom", Protocol: "wss", Host: "Example.TS.NET", Port: 443}}
	if err := validateCommand(&cmd); err != nil {
		t.Fatal(err)
	}
	if got := cmd.Node.Endpoints[0].Host; got != "fd7a:115c:a1e0::1" {
		t.Fatalf("IPv6 normalization: %s", got)
	}
	if got := cmd.Node.Endpoints[1].Host; got != "example.ts.net" {
		t.Fatalf("DNS normalization: %s", got)
	}
}
func TestValidationRejectsAmbiguousOrUnsafeEndpoints(t *testing.T) {
	for _, host := range []string{"example.com:8080", "http://test", "user@host", "fd7a::1%eth0", "host name", "host/path", "bad..host", "bad-.local", "[::1]"} {
		cmd := testCommand("operation-safe", ChangeAdd, 0, testNode(testNodeID1))
		cmd.Node.Endpoints[0].Host = host
		if err := validateCommand(&cmd); err == nil {
			t.Errorf("accepted host %q", host)
		}
	}
}
func TestValidateRejectsFakeIdentityAndReplayInput(t *testing.T) {
	cmd := testCommand("operation-safe", ChangeAdd, 0, testNode("node-123"))
	if err := validateCommand(&cmd); err == nil {
		t.Fatal("expected invalid node ID")
	}
	cmd = testCommand("operation-safe", ChangeAdd, 0, testNode(testNodeID1))
	cmd.Node.Revision = 2
	if err := validateCommand(&cmd); err == nil {
		t.Fatal("accepted caller-managed revision")
	}
	cmd = testCommand("operation-safe", ChangeRemove, 0, NodeRecord{})
	cmd.NodeID = testNodeID1
	if err := validateCommand(&cmd); err != nil {
		t.Fatal(err)
	}
}
func TestErrorSentinels(t *testing.T) {
	if !errors.Is(ErrRevisionConflict, ErrRevisionConflict) {
		t.Fatal("error equality")
	}
}
