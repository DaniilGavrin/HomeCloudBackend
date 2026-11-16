package roomheartbeat

import (
	"errors"
	"testing"
	"time"
)

func TestIndependentDeadlinesAndGrace(t *testing.T) {
	base := time.Now()
	tr := New()
	if e := tr.Start("r", "p", 1, 42, 20*time.Second, 10*time.Second, base); e != nil {
		t.Fatal(e)
	}
	if _, e := tr.Beat("r", "p", 1, "gateway", 42, base.Add(20*time.Second)); e != nil {
		t.Fatal(e)
	}
	if got := tr.Expire(base.Add(30 * time.Second)); len(got) != 0 {
		t.Fatalf("one missed heartbeat expired before grace: %+v", got)
	}
	if got := tr.Expire(base.Add(30*time.Second + time.Millisecond)); len(got) != 1 || got[0].Participant != "tv" {
		t.Fatalf("expected TV lost %+v", got)
	}
	if _, e := tr.Beat("r", "p", 1, "tv", 12, base.Add(31*time.Second)); !errors.Is(e, ErrUnknown) {
		t.Fatal(e)
	}
}
func TestGatewayExpiryEvenIfTVHealthy(t *testing.T) {
	base := time.Now()
	tr := New()
	_ = tr.Start("r", "p", 1, 1, 25*time.Second, 10*time.Second, base)
	if _, e := tr.Beat("r", "p", 1, "tv", 2, base.Add(25*time.Second)); e != nil {
		t.Fatal(e)
	}
	x := tr.Expire(base.Add(35*time.Second + time.Millisecond))
	if len(x) != 1 || x[0].Participant != "gateway" {
		t.Fatal(x)
	}
}
func TestEpochSessionGenerationAndEOFIndependent(t *testing.T) {
	base := time.Now()
	tr := New()
	_ = tr.Start("r", "p", 1, 99, 20*time.Second, 10*time.Second, base)
	if _, e := tr.Beat("r", "p", 1, "tv", 42, base.Add(10*time.Second)); e != nil {
		t.Fatal(e)
	}
	if _, e := tr.Beat("r", "p", 1, "tv", 41, base.Add(11*time.Second)); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if _, e := tr.Beat("r", "p", 1, "gateway", 98, base.Add(11*time.Second)); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if _, e := tr.Beat("r", "old", 1, "tv", 42, base.Add(11*time.Second)); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if e := tr.Start("r", "next", 2, 100, 20*time.Second, 10*time.Second, base.Add(12*time.Second)); e != nil {
		t.Fatal(e)
	}
	if _, e := tr.Beat("r", "p", 1, "tv", 42, base.Add(13*time.Second)); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if _, e := tr.Beat("r", "next", 2, "tv", 43, base.Add(25*time.Second)); e != nil {
		t.Fatal(e)
	}
	// An EOF does not remove the ACTIVE room's tracker: an idle TV can pulse.
	if _, e := tr.Beat("r", "next", 2, "gateway", 100, base.Add(26*time.Second)); e != nil {
		t.Fatal(e)
	}
	if got := tr.Expire(base.Add(45 * time.Second)); len(got) != 0 {
		t.Fatal(got)
	}
	tr.Remove("r")
	if _, ok := tr.Get("r"); ok {
		t.Fatal("cleanup leak")
	}
}
func TestPolicyAndNoPrematureResurrection(t *testing.T) {
	if ValidPolicy(19*time.Second, 10*time.Second) || ValidPolicy(25*time.Second, 9*time.Second) || !ValidPolicy(30*time.Second, 10*time.Second) {
		t.Fatal("bad policy")
	}
	base := time.Now()
	tr := New()
	_ = tr.Start("r", "p", 1, 1, 20*time.Second, 10*time.Second, base)
	if _, e := tr.Beat("r", "p", 1, "gateway", 1, base.Add(31*time.Second)); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if got := tr.Expire(base.Add(31 * time.Second)); len(got) != 1 {
		t.Fatal(got)
	}
}
