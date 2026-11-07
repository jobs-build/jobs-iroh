package sched

import (
	"fmt"

	"github.com/amber-store/core/key"

	"sort"

	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/cover"
	"github.com/jobs-build/jobs-iroh/wire"
)

// KPTreeRef and PinCoverRef are the shared ref-name helpers (cover owns
// them — the runner's local pipeline and the boot self-test use the same
// names).
func KPTreeRef(kp key.Key) string  { return cover.KPTreeRef(kp) }
func PinCoverRef(f key.Key) string { return cover.PinCoverRef(f) }
func KTTreeRef(kt key.Key) string  { return cover.KTTreeRef(kt) }

// deriveKP computes and durably records the KP binding for a pinned F: read
// build-pinned:F, derive KP via the shared cover.Derive (the SAME
// implementation the pin runner and the local pipeline use —
// identity-critical), then write build-pinned:<KP> (the alias unfold, the
// gate's cache check, and the buildrun runner read), kp-tree/<KP> (the pull
// carrier), and pin-cover/<v>:F LAST — its presence implies the others
// landed. Pure store computation, idempotent, callable off-lock (pin commit)
// or under s.mu (the on-demand heal): a crash between any two writes is
// healed by the next call re-deriving everything (design §6.3 [INV]).
func (s *Sched) deriveKP(f key.Key) (key.Key, error) {
	pinnedKey, ok, err := s.store.GetKey(s.ctx, "build-pinned:"+f.String())
	if err != nil {
		return key.Key{}, err
	}
	if !ok {
		return key.Key{}, fmt.Errorf("build-pinned:%s not found", f.String())
	}
	pinnedBytes, err := s.store.ReadFile(s.ctx, pinnedKey)
	if err != nil {
		return key.Key{}, fmt.Errorf("read pinned blob: %w", err)
	}
	pinned, err := builddef.DecodePinned(pinnedBytes)
	if err != nil {
		return key.Key{}, fmt.Errorf("decode pinned: %w", err)
	}
	env, ok, err := s.store.TreeSubdir(s.ctx, f, "env")
	if err != nil || !ok {
		return key.Key{}, fmt.Errorf("F-tree env subtree: ok=%v err=%w", ok, err)
	}
	platKey, ok, err := s.store.TreeSubdir(s.ctx, f, "platform")
	if err != nil || !ok {
		return key.Key{}, fmt.Errorf("F-tree platform entry: ok=%v err=%w", ok, err)
	}
	platform, err := s.store.ReadFile(s.ctx, platKey)
	if err != nil {
		return key.Key{}, fmt.Errorf("read F-tree platform: %w", err)
	}

	kp, err := cover.Derive(s.ctx, s.store, pinnedBytes, pinned, string(platform), env)
	if err != nil {
		return key.Key{}, fmt.Errorf("cover.Derive: %w", err)
	}
	if err := s.store.PutRef(s.ctx, "build-pinned:"+kp.String(), pinnedKey); err != nil {
		return key.Key{}, err
	}
	if err := s.store.PutRef(s.ctx, KPTreeRef(kp), kp); err != nil {
		return key.Key{}, err
	}
	if err := s.store.PutRef(s.ctx, PinCoverRef(f), kp); err != nil {
		return key.Key{}, err
	}
	return kp, nil
}

// resolveKPLocked returns the KP binding for a pinned F, re-deriving on
// demand when the pin-cover ref is missing (crash window between
// build-pinned:F and the derived refs, or a pre-existing pin from before a
// KPVersion bump) — absence of a derived ref is NEVER a failure (design §6.3).
func (s *Sched) resolveKPLocked(f key.Key) (key.Key, error) {
	kp, ok, err := s.store.GetKey(s.ctx, PinCoverRef(f))
	if err != nil {
		return key.Key{}, err
	}
	if ok {
		return kp, nil
	}
	return s.deriveKP(f)
}

// waiterFs collects the resolved F keys of the buildvalues (testvalues)
// depending on a KP-keyed buildrun (KT-keyed testrun) — FailureRecord.ForF,
// the KP→F / KT→F human bridge. Empty for every other node kind. Caller
// holds s.mu.
func waiterFs(n *node) []string {
	if n.id.kind != wire.KindBuildRun && n.id.kind != wire.KindTestRun {
		return nil
	}
	var out []string
	for d := range n.dependents {
		if orchestrator(d.id.kind) && d.fResolved {
			out = append(out, d.f.String())
		}
	}
	sort.Strings(out)
	return out
}

// deriveKT computes and durably records the test-run key for the pinned
// test at KP (tests design §4.2, §5): resolve every pinned input to content
// (the caller has verified they are all done), derive KT via the shared
// cover.DeriveTest — the SAME implementation the local pipeline uses —
// then write build-pinned:<KT> (the alias unfold, the gate's cache check,
// and the testrun runner read) and kt-tree/<KT> (the pull carrier). Pure
// store computation, idempotent; re-run on every testvalue advance that
// still lacks the binding, which is the crash heal.
func (s *Sched) deriveKT(kp key.Key) (key.Key, error) {
	pinnedKey, ok, err := s.store.GetKey(s.ctx, "build-pinned:"+kp.String())
	if err != nil {
		return key.Key{}, err
	}
	if !ok {
		return key.Key{}, fmt.Errorf("build-pinned:%s not found", kp.String())
	}
	pinnedBytes, err := s.store.ReadFile(s.ctx, pinnedKey)
	if err != nil {
		return key.Key{}, fmt.Errorf("read pinned blob: %w", err)
	}
	pinned, err := builddef.DecodePinned(pinnedBytes)
	if err != nil {
		return key.Key{}, fmt.Errorf("decode pinned: %w", err)
	}
	platKey, ok, err := s.store.TreeSubdir(s.ctx, kp, "platform")
	if err != nil || !ok {
		return key.Key{}, fmt.Errorf("KP-tree platform entry: ok=%v err=%w", ok, err)
	}
	platform, err := s.store.ReadFile(s.ctx, platKey)
	if err != nil {
		return key.Key{}, fmt.Errorf("read KP-tree platform: %w", err)
	}
	resolved, err := cover.ResolveInputs(s.ctx, s.store, pinned.Inputs)
	if err != nil {
		return key.Key{}, err
	}
	kt, err := cover.DeriveTest(s.ctx, s.store, kp, pinned, string(platform), resolved)
	if err != nil {
		return key.Key{}, fmt.Errorf("cover.DeriveTest: %w", err)
	}
	if err := s.store.PutRef(s.ctx, "build-pinned:"+kt.String(), pinnedKey); err != nil {
		return key.Key{}, err
	}
	if err := s.store.PutRef(s.ctx, KTTreeRef(kt), kt); err != nil {
		return key.Key{}, err
	}
	return kt, nil
}

// ensureTestAliasLocked republishes a done testrun's memo under the test's
// F name (test-passed:F → KT): the testvalue doneness fast-path and the
// client's pull-home read it. Idempotent; runs BEFORE nodeDoneLocked(tv)
// so a terminal snapshot implies the alias is durable.
func (s *Sched) ensureTestAliasLocked(f, kt key.Key) error {
	if _, ok, err := s.store.GetKey(s.ctx, cover.TestPassedRef(f)); err != nil {
		return err
	} else if ok {
		return nil
	}
	if _, ok, err := s.store.GetKey(s.ctx, cover.TestPassedRef(kt)); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("%s missing for done testrun", cover.TestPassedRef(kt))
	}
	return s.store.PutRef(s.ctx, cover.TestPassedRef(f), kt)
}

// ensureFAliasesLocked republishes buildrun's KP-keyed output refs under
// their F names — build-output-deps:F STRICTLY before build-output:F (a
// crash between the two must never leave a done-looking F without its
// runtime closure — design §10.2 rule 2). Idempotent (skips when the F
// output already exists); called on every buildvalue advance-to-done, which
// uniformly covers the fresh-build, memo-hit, late-join, and crash-heal
// paths — and always runs BEFORE nodeDoneLocked(bv), so a terminal watch
// snapshot implies the aliases are durable (rule 3).
func (s *Sched) ensureFAliasesLocked(f, kp key.Key) error {
	if _, ok, err := s.store.GetKey(s.ctx, "build-output:"+f.String()); err != nil {
		return err
	} else if ok {
		return nil
	}
	deps, depsOK, err := s.store.GetKey(s.ctx, "build-output-deps:"+kp.String())
	if err != nil {
		return err
	}
	out, outOK, err := s.store.GetKey(s.ctx, "build-output:"+kp.String())
	if err != nil {
		return err
	}
	if !outOK || !depsOK {
		return fmt.Errorf("build-output(-deps):%s missing for done buildrun", kp.String())
	}
	if err := s.store.PutRef(s.ctx, "build-output-deps:"+f.String(), deps); err != nil {
		return err
	}
	return s.store.PutRef(s.ctx, "build-output:"+f.String(), out)
}
