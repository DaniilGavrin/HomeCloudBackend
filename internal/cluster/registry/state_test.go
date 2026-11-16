package registry

import (
	"context"
	"reflect"
	"testing"
)

func TestDurableStateRestoreAndIdempotency(t *testing.T) {
	db, ctx := testDB(t)
	original := NewStore(db)
	c := testCommand("first-add", ChangeAdd, 0, testNode(testNodeID1))
	if _, err := original.ApplyCommitted(ctx, c); err != nil {
		t.Fatal(err)
	}
	rm := testCommand("remove-kostroma", ChangeRemove, 1, NodeRecord{})
	rm.NodeID = testNodeID1
	if _, err := original.ApplyCommitted(ctx, rm); err != nil {
		t.Fatal(err)
	}
	state, err := original.ReadDurableState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Registry.Revision != 2 || len(state.Registry.Nodes) != 1 || !state.Registry.Nodes[0].Removed || len(state.Applied) != 2 {
		t.Fatalf("incomplete snapshot: %+v", state)
	}
	targetDB, _ := testDB(t)
	target := NewStore(targetDB)
	if err = target.RestoreDurableState(ctx, state); err != nil {
		t.Fatal(err)
	}
	restored, err := target.ReadDurableState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(state, restored) {
		t.Fatalf("state mismatch: original=%+v restored=%+v", state, restored)
	}
	if result, err := target.ApplyCommitted(ctx, c); err != nil || !result.Replayed || result.Revision != 1 {
		t.Fatalf("lost idempotency: %+v %v", result, err)
	}
	if result, err := target.ApplyCommitted(ctx, rm); err != nil || !result.Replayed || result.Revision != 2 {
		t.Fatalf("lost tombstone: %+v %v", result, err)
	}
}

func TestRestoreInvalidIsNonDestructive(t *testing.T) {
	db, ctx := testDB(t)
	store := NewStore(db)
	cmd := testCommand("first-add", ChangeAdd, 0, testNode(testNodeID1))
	if _, err := store.ApplyCommitted(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	valid, err := store.ReadDurableState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	broken := valid
	broken.Applied = nil
	if err = store.RestoreDurableState(context.Background(), broken); err == nil {
		t.Fatal("missing ledger accepted")
	}
	after, err := store.ReadDurableState(ctx)
	if err != nil || !reflect.DeepEqual(valid, after) {
		t.Fatalf("bad restore mutated data: %+v %v", after, err)
	}
}
