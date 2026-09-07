package sched

import (
	"fmt"

	"github.com/amber-store/core/key"

	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/importdef"
	"github.com/jobs-build/jobs-iroh/wire"
)

// FTreeRef names the server-written closure carrier for one F tree:
// "f-tree/<F>" → F, published at buildfrom-commit time alongside the gated
// batch. It exists because build-from-tree:<F> is a runner-produced,
// gate-scoped name — the server-side f-tree/ namespace is the one ref per F
// that is GUARANTEED to exist the moment any F-stage becomes schedulable, so
// runners can always pull an F closure by ref.
func FTreeRef(f key.Key) string { return "f-tree/" + f.String() }

// healCarrierLocked writes a missing name==value carrier ref (f-tree/<F>,
// kp-tree/<KP>): a crash between a stage's gated commit and the server-side
// carrier write used to leave the next stage's pull failing retryably until
// budget-exhausted-hard, forever (the latent f-tree window the
// sibling-sources arc fixed — design §6.3). The carrier's value IS its name
// key and the closure provably exists once the stage is done, so the heal is
// a single idempotent PutRef.
func (s *Sched) healCarrierLocked(name string, k key.Key) error {
	if _, ok, err := s.store.GetKey(s.ctx, name); err != nil || ok {
		return err
	}
	return s.store.PutRef(s.ctx, name, k)
}

// computePullRefsLocked derives the exact set of refs whose closures the
// runner must pull before running the node's stage driver — everything the
// driver resolves locally (docs/research/jobs-runner-stages.md), computed
// against the server store at enqueue time (every dep is done, so its
// output refs exist):
//
//	import        shell:<platform> (the hermetic import root's userland);
//	              FetcherDef → build-from:K_f + build-output:F_f (artifact
//	              two-hop) + build-output-deps:F_f (the fetcher's runtime
//	              closure, mounted into the import root); named seed
//	              fetcher → fetcher:<name>:<platform>
//	buildfrom     source import → import-output:K_src; source build →
//	              build-from:K_src + build-output:F_src (+deps); source tree →
//	              build-from-tree:<T>
//	pluginresolve f-tree/<F>
//	pin           f-tree/<F> + build-plugin-resolved:F + shell:<platform>
//	              (plugin sandboxes) + every plugin/resolution-dep input's
//	              output refs
//	buildrun      f-tree/<F> + build-pinned:F + shell:<platform> + every
//	              pinned input's output refs (build inputs incl.
//	              build-output-deps — the runtime-closure read) + declared
//	              build-cache:<id>:<platform>
//
// Refs that may legitimately be absent (seed fetcher/shell before bootstrap,
// cold caches) are included only when present — a nonexistent name in
// PullRefs would fail the runner's pull retryably forever, while its absence
// merely reproduces the driver's own (better-worded) failure or a cache cold
// start.
func (s *Sched) computePullRefsLocked(n *node) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	add := func(names ...string) {
		for _, name := range names {
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
	}
	addIfPresent := func(name string) error {
		_, ok, err := s.store.GetKey(s.ctx, name)
		if err != nil {
			return err
		}
		if ok {
			add(name)
		}
		return nil
	}
	addInputOutputs := func(in builddef.Input, withDeps bool) error {
		k, err := in.Key()
		if err != nil {
			return fmt.Errorf("input key: %w", err)
		}
		switch in.Kind {
		case builddef.KindImport:
			add("import-output:" + k.String())
		case builddef.KindBuild:
			f, ok, err := s.store.GetKey(s.ctx, "build-from:"+k.String())
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("build-from:%s missing for done input", k)
			}
			add("build-from:"+k.String(), "build-output:"+f.String())
			if withDeps {
				add("build-output-deps:" + f.String())
			}
		case builddef.KindTree:
			tk, err := builddef.DecodeTreeKey(in.Definition)
			if err != nil {
				return fmt.Errorf("decode tree input: %w", err)
			}
			add("build-from-tree:" + tk.String())
		}
		return nil
	}

	switch n.id.kind {
	case wire.KindImport:
		def, err := importdef.Decode(n.def)
		if err != nil {
			return nil, fmt.Errorf("decode import def: %w", err)
		}
		platform := def.Platform
		if platform == "" {
			platform = n.platform
		}
		// The import root is built from the shell (hermetic-imports design):
		// present-only, like the build stages — absent means pre-bootstrap.
		if err := addIfPresent("shell:" + platform); err != nil {
			return nil, err
		}
		if len(def.FetcherDef) > 0 {
			fk, err := (builddef.Input{Kind: builddef.KindBuild, Definition: def.FetcherDef}).Key()
			if err != nil {
				return nil, fmt.Errorf("fetcher def key: %w", err)
			}
			ff, ok, err := s.store.GetKey(s.ctx, "build-from:"+fk.String())
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("build-from:%s missing for done fetcher build", fk)
			}
			add("build-from:"+fk.String(), "build-output:"+ff.String())
			// The fetcher's runtime closure: what its BUILD.jobs declared as
			// runtime_deps (a toolchain, typically). Present-only — a fetcher
			// built before build-output-deps existed simply has none.
			if err := addIfPresent("build-output-deps:" + ff.String()); err != nil {
				return nil, err
			}
		} else {
			if err := addIfPresent("fetcher:" + def.Fetcher + ":" + platform); err != nil {
				return nil, err
			}
		}

	case wire.KindBuildFrom:
		def, err := builddef.DecodeDefinition(n.def)
		if err != nil {
			return nil, fmt.Errorf("decode build def: %w", err)
		}
		switch def.Source.Kind {
		case builddef.KindImport, builddef.KindBuild, builddef.KindTree:
			if err := addInputOutputs(def.Source, def.Source.Kind == builddef.KindBuild); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown source kind %q", def.Source.Kind)
		}

	case wire.KindPluginResolve:
		if err := s.healCarrierLocked(FTreeRef(n.id.key), n.id.key); err != nil {
			return nil, err
		}
		add(FTreeRef(n.id.key))

	case wire.KindPin:
		if err := s.healCarrierLocked(FTreeRef(n.id.key), n.id.key); err != nil {
			return nil, err
		}
		add(FTreeRef(n.id.key), "build-plugin-resolved:"+n.id.key.String())
		if err := addIfPresent("shell:" + n.platform); err != nil {
			return nil, err
		}
		for _, in := range n.pinInputs {
			// Plugin binaries and resolution deps are consumed via their c/
			// artifact — no runtime-closure pull.
			if err := addInputOutputs(in, false); err != nil {
				return nil, err
			}
		}

	case wire.KindBuildRun:
		// KP-keyed (sibling-sources design §10.1): the runner pulls the
		// covered closure by its kp-tree carrier — never the monorepo — plus
		// the build-pinned:<KP> alias written at KP derivation.
		if err := s.healCarrierLocked(KPTreeRef(n.id.key), n.id.key); err != nil {
			return nil, err
		}
		add(KPTreeRef(n.id.key), "build-pinned:"+n.id.key.String())
		if err := addIfPresent("shell:" + n.platform); err != nil {
			return nil, err
		}
		for _, in := range n.runInputs {
			// collectStore reads each build input's runtime closure
			// (build-output-deps:F) — pull it too.
			if err := addInputOutputs(in, true); err != nil {
				return nil, err
			}
		}
		for _, c := range n.caches {
			// A cold cache has no ref yet — that is a valid start state, not
			// a missing pull.
			if err := addIfPresent(builddef.CacheRefName(c.ID, n.platform)); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}
