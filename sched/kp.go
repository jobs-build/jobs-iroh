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

// waiterFs collects the resolved F keys of the buildvalues depending on a
// KP-keyed buildrun (FailureRecord.ForF — the KP→F human bridge). Empty for
// every other node kind. Caller holds s.mu.
func waiterFs(n *node) []string {
	if n.id.kind != wire.KindBuildRun {
		return nil
	}
	var out []string
	for d := range n.dependents {
		if d.id.kind == wire.KindBuildValue && d.fResolved {
			out = append(out, d.f.String())
		}
	}
	sort.Strings(out)
	return out
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
