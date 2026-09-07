//go:build linux

package runner

import (
	"context"
	"fmt"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/cover"
)

// localBuildFrom computes the content key F for a LOCAL target build: ingest
// the source dir (honoring .amberignore) and assemble the build-from tree —
// the local analogue of RunBuildFrom minus the build-from:K→F bridge (a local
// source has no submission K; F is the identity). Mode follows the same rule
// as every def constructor (sibling-sources design §3.2): cfg.Dir == "" is a
// root build ({env/=tree, params, platform}, byte-identical to before);
// cfg.Dir != "" is a widened-context build (env/ = the WHOLE ingested tree,
// dir carried in the F-tree). The build-from tree is shared by key with the
// server's, so a local F equals the server's F for the same pushed source.
func localBuildFrom(ctx context.Context, st *amber.Store, brc BuildRunCfg, cfg DevelopConfig) (key.Key, error) {
	srcDir := cfg.SourceDir
	if srcDir == "" {
		srcDir = "."
	}
	sourceKey, err := st.IngestSourceDir(ctx, srcDir)
	if err != nil {
		return key.Key{}, fmt.Errorf("ingest source %s: %w", srcDir, err)
	}
	buildRoot, err := resolveSubtreeKey(ctx, st, sourceKey, cfg.Dir)
	if err != nil {
		return key.Key{}, fmt.Errorf("source dir %q not found: %w", cfg.Dir, err)
	}
	// Same recipe resolution + override normalization as the server's RunBuildFrom,
	// so a local F equals the server's F for the same environment (cache join).
	override, err := resolveRecipeOverride(ctx, st, buildRoot, cfg.BuildFile, nil)
	if err != nil {
		return key.Key{}, err
	}
	envKey, dirEntry := buildRoot, ""
	if cfg.Dir != "" {
		envKey, dirEntry = sourceKey, cfg.Dir
	}
	f, err := st.BuildFromTree(ctx, envKey, dirEntry, cfg.Params, brc.Platform, override)
	if err != nil {
		return key.Key{}, fmt.Errorf("assemble build-from tree: %w", err)
	}
	return f, nil
}

// driveFStages drives the F-keyed pipeline for f, skipping any stage whose result
// ref already exists (the join) and reporting each step via p. With runFinal it
// also runs build-output:F (jobs run); without it stops after build-pinned:F
// (jobs develop). It ensures discovered plugins/inputs between stages via the
// developDriver (recursive, nested progress). The drivers write the step refs.
func (d *developDriver) driveFStages(f key.Key, runFinal bool, p *Progress) error {
	// Full join: jobs run can execute an existing build-output:F directly.
	if runFinal {
		if _, ok, err := d.st.GetKey(d.ctx, "build-output:"+f.String()); err != nil {
			return err
		} else if ok {
			p.Cached("build")
			return nil
		}
	}

	pinnedExists, err := refExists(d.ctx, d.st, "build-pinned:"+f.String())
	if err != nil {
		return err
	}
	if pinnedExists {
		p.Cached("plugin-resolve")
		p.Cached("pin")
	} else {
		// plugin-resolve (skip if already resolved)
		prExists, err := refExists(d.ctx, d.st, "build-plugin-resolved:"+f.String())
		if err != nil {
			return err
		}
		if prExists {
			p.Cached("plugin-resolve")
		} else {
			done := p.Start("plugin-resolve")
			if err := outcomeErr("plugin-resolve "+f.String(), RunPluginResolve(d.ctx, d.st, d.rw, d.brc, f)); err != nil {
				done(err)
				return err
			}
			done(nil)
		}
		if err := d.ensurePinDeps(f, p); err != nil {
			return err
		}
		// pin
		done := p.Start("pin")
		if err := outcomeErr("pin "+f.String(), RunPin(d.ctx, d.st, d.rw, d.brc, f)); err != nil {
			done(err)
			return err
		}
		done(nil)
	}

	// ensure the pinned inputs are present (needed for develop's sandbox + run's build)
	if err := d.ensureInputs(f, p); err != nil {
		return err
	}

	// KP derivation + memo (sibling-sources design §10.2(4), §11.2): the same
	// shared cover.Derive the server runs — a covered-content join point across
	// arbitrary out-of-cover context changes. A memo hit republishes the F-level
	// aliases (deps strictly first) so every existing resolver keeps working.
	kp, err := d.deriveKP(f)
	if err != nil {
		return err
	}

	if runFinal {
		if _, ok, err := d.st.GetKey(d.ctx, "build-output:"+kp.String()); err != nil {
			return err
		} else if ok {
			if err := d.writeFAliases(f, kp); err != nil {
				return err
			}
			p.Cached("build")
			return nil
		}
		done := p.Start("build")
		if err := outcomeErr("build "+kp.String(), RunBuild(d.ctx, d.st, d.rw, d.brc, NamespaceBuildExecutor{}, kp)); err != nil {
			done(err)
			return err
		}
		done(nil)
		if err := d.writeFAliases(f, kp); err != nil {
			return err
		}
	}
	return nil
}

// deriveKP computes KP for the pinned build at F via the shared cover.Derive
// (identity-critical: bit-identical to the server's derivation) and writes the
// local build-pinned:<KP> alias RunBuild/assembleBuildSpec read. Idempotent —
// re-derivation after a partial earlier run converges on the same refs.
func (d *developDriver) deriveKP(f key.Key) (key.Key, error) {
	pinnedKey, ok, err := d.st.GetKey(d.ctx, "build-pinned:"+f.String())
	if err != nil {
		return key.Key{}, err
	}
	if !ok {
		return key.Key{}, fmt.Errorf("build-pinned:%s missing after pin", f.String())
	}
	pinnedBytes, err := pullFileBytes(d.ctx, d.st, pinnedKey)
	if err != nil {
		return key.Key{}, err
	}
	pinned, err := builddef.DecodePinned(pinnedBytes)
	if err != nil {
		return key.Key{}, err
	}
	env, err := resolveSubdirKey(d.ctx, d.st, f, "env")
	if err != nil {
		return key.Key{}, fmt.Errorf("resolve F-tree env: %w", err)
	}
	platform, err := readTreeFile(d.ctx, d.st, f, "platform")
	if err != nil {
		return key.Key{}, fmt.Errorf("read F-tree platform: %w", err)
	}
	kp, err := cover.Derive(d.ctx, d.st, pinnedBytes, pinned, string(platform), env)
	if err != nil {
		return key.Key{}, fmt.Errorf("derive KP: %w", err)
	}
	if err := d.st.PutRef(d.ctx, "build-pinned:"+kp.String(), pinnedKey); err != nil {
		return key.Key{}, err
	}
	// Record the F → KP binding locally too (findable by the boot self-test's
	// cleanup and by pullHome's memo-join; same name the server writes).
	if err := d.st.PutRef(d.ctx, cover.PinCoverRef(f), kp); err != nil {
		return key.Key{}, err
	}
	return kp, nil
}

// writeFAliases republishes the KP-keyed output refs under their F names,
// deps strictly before output (a crash between the two must never leave a
// done-looking F without its runtime closure — design §10.2 rule 2).
func (d *developDriver) writeFAliases(f, kp key.Key) error {
	deps, depsOK, err := d.st.GetKey(d.ctx, "build-output-deps:"+kp.String())
	if err != nil {
		return err
	}
	out, outOK, err := d.st.GetKey(d.ctx, "build-output:"+kp.String())
	if err != nil {
		return err
	}
	if !outOK || !depsOK {
		return fmt.Errorf("build-output(-deps):%s missing after build", kp.String())
	}
	if err := d.st.PutRef(d.ctx, "build-output-deps:"+f.String(), deps); err != nil {
		return err
	}
	return d.st.PutRef(d.ctx, "build-output:"+f.String(), out)
}

// ensurePinDeps reads build-plugin-resolved:F and ensures everything the pin
// stage consumes is built: each plugin AND each resolution dep.
func (d *developDriver) ensurePinDeps(f key.Key, p *Progress) error {
	prKey, ok, err := d.st.GetKey(d.ctx, "build-plugin-resolved:"+f.String())
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("build-plugin-resolved:%s missing after resolve", f.String())
	}
	prBytes, err := pullFileBytes(d.ctx, d.st, prKey)
	if err != nil {
		return err
	}
	pr, err := builddef.DecodePluginResolved(prBytes)
	if err != nil {
		return err
	}
	for name, in := range pr.Plugins {
		if err := d.ensureInput(in, name, p.Sub()); err != nil {
			return fmt.Errorf("plugin %s: %w", name, err)
		}
	}
	// Resolution deps are pin-stage dependencies exactly like plugins
	// (resolution-deps design §6.3): build each before RunPin materializes it.
	for name, in := range pr.Deps {
		if err := d.ensureInput(in, name, p.Sub()); err != nil {
			return fmt.Errorf("resolution dep %s: %w", name, err)
		}
	}
	return nil
}

// ensureInputs reads build-pinned:F and ensures each build input is built.
func (d *developDriver) ensureInputs(f key.Key, p *Progress) error {
	pinnedKey, ok, err := d.st.GetKey(d.ctx, "build-pinned:"+f.String())
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("build-pinned:%s missing after pin", f.String())
	}
	pinnedBytes, err := pullFileBytes(d.ctx, d.st, pinnedKey)
	if err != nil {
		return err
	}
	pinned, err := builddef.DecodePinned(pinnedBytes)
	if err != nil {
		return err
	}
	for _, pi := range pinned.Inputs {
		if err := d.ensureInput(builddef.Input{Kind: pi.Kind, Definition: pi.Definition}, pi.Name, p.Sub()); err != nil {
			return fmt.Errorf("input %s: %w", pi.Name, err)
		}
	}
	return nil
}

// refExists reports whether a ref name resolves in the store.
func refExists(ctx context.Context, st *amber.Store, name string) (bool, error) {
	_, ok, err := st.GetKey(ctx, name)
	return ok, err
}
