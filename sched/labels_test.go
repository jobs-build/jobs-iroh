package sched

import (
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/wire"
)

func TestSortedNamedInputsOrderAndNames(t *testing.T) {
	in := func(b byte) builddef.Input {
		return builddef.Input{Kind: builddef.KindTree, Definition: []byte{b}}
	}
	got := sortedNamedInputs(
		map[string]builddef.Input{"zeta": in(1), "alpha": in(2)},
		map[string]builddef.Input{"mid": in(3), "aaa": in(4)},
	)
	wantNames := []string{"alpha", "zeta", "aaa", "mid"} // plugins first, each name-sorted
	if len(got) != 4 {
		t.Fatalf("got %d inputs, want 4", len(got))
	}
	for i, n := range wantNames {
		if got[i].Name != n {
			t.Fatalf("input %d name %q, want %q", i, got[i].Name, n)
		}
	}
}

func TestRequireLabelFirstWins(t *testing.T) {
	e := newEnv(t)
	e.s.mu.Lock()
	defer e.s.mu.Unlock()

	var k key.Key
	k[0] = 7
	n := e.s.require(wire.KindBuildValue, k, nil, testPlatform, nil, nil, "gitlab")
	if n.label != "gitlab" {
		t.Fatalf("label %q, want gitlab", n.label)
	}
	again := e.s.require(wire.KindBuildValue, k, nil, testPlatform, nil, nil, "other-name")
	if again != n || n.label != "gitlab" {
		t.Fatalf("join must keep the first label, got %q", n.label)
	}
	unnamed := e.s.require(wire.KindPin, k, nil, testPlatform, nil, nil, "")
	if unnamed.label != "" {
		t.Fatalf("empty edge name must leave label empty, got %q", unnamed.label)
	}
}

func TestApplyRecipeLabelOverridesPipeline(t *testing.T) {
	e := newEnv(t)
	e.s.mu.Lock()
	defer e.s.mu.Unlock()

	var k key.Key
	k[0] = 9
	bv := e.s.require(wire.KindBuildValue, k, nil, testPlatform, nil, nil, "dep_name")
	pin := e.s.require(wire.KindPin, k, nil, testPlatform, bv, nil, "dep_name")

	e.s.applyRecipeLabelLocked(pin, []wire.RefProposal{
		{Name: "some-other:ref"},
		{Name: "build-pinned:" + k.String(), Label: "nice name"},
	})
	if pin.label != "nice name" {
		t.Fatalf("pin label %q, want recipe override", pin.label)
	}
	if bv.label != "nice name" {
		t.Fatalf("buildvalue dependent label %q, want recipe override", bv.label)
	}

	// No labeled build-pinned proposal: labels stay put.
	e.s.applyRecipeLabelLocked(pin, []wire.RefProposal{{Name: "build-pinned:" + k.String()}})
	if pin.label != "nice name" {
		t.Fatalf("unlabeled proposal must not clear, got %q", pin.label)
	}
}
