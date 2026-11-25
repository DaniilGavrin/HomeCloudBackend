package devicecontracts

import (
	"encoding/json"
	"testing"
)

func TestValidateClaimAndPreserveCapabilityDeclaration(t *testing.T) {
	const payload = `{
		"contracts_version":1,
		"identity":{"integration_id":"wifi.local","native_device_id":"aa:bb:cc","identity_quality":"stable"},
		"device_type":{"id":"smart_plug","version":1},
		"metadata":{"manufacturer":"Example"},
		"capability_revision":1,
		"capabilities":[{"id":"switch","version":1,"commands":[{"id":"turn_on"}],"entities":[{"id":"switch_state"}]}]
	}`
	var claim Claim
	if err := json.Unmarshal([]byte(payload), &claim); err != nil { t.Fatal(err) }
	if err := ValidateClaim(claim); err != nil { t.Fatalf("valid claim rejected: %v", err) }
	if len(claim.Capabilities) != 1 || len(claim.Capabilities[0].Raw) == 0 {
		t.Fatal("capability declaration was not retained")
	}
	var declaration map[string]any
	if err := json.Unmarshal(claim.Capabilities[0].Raw, &declaration); err != nil { t.Fatal(err) }
	if _, ok := declaration["commands"]; !ok { t.Fatal("commands schema was dropped") }
}

func TestValidateClaimRejectsUnstableRequiredFields(t *testing.T) {
	claim := Claim{
		ContractsVersion: ContractsVersion,
		Identity: Identity{IntegrationID:"wifi.local",NativeDeviceID:"id",Quality:IdentityWeak},
		DeviceType: DeviceType{ID:"smart_plug",Version:1},
		CapabilityRevision:1,
	}
	if err := ValidateClaim(claim); err != nil { t.Fatalf("valid weak identity claim rejected: %v",err) }
	claim.Identity.Quality = "mac_address"
	if err := ValidateClaim(claim); err == nil { t.Fatal("unknown identity quality accepted") }
}
