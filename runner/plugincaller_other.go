//go:build !linux

package runner

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/recipe"
)

// SandboxedPluginCaller off Linux has no user-namespace hermetic sandbox, so
// it materializes the plugin artifact to disk and delegates to the
// non-hermetic recipe.SubprocessPlugin bridge (CBOR over stdio, ./plugin as
// CWD, source passed as a path). This is a dev fallback (e.g. on macOS); it
// does NOT provide net=none or a read-only root — those guarantees are
// Linux-only.
type SandboxedPluginCaller struct {
	Cl        *amber.Store
	PluginKey key.Key
	SourceDir string
	ShellKey  key.Key
	Ctx       context.Context // nil → context.Background()
	// StderrSink mirrors the Linux caller's field (struct shape must match
	// across build tags); the non-hermetic fallback does not tee stderr.
	StderrSink io.Writer
	// DepDirs mirrors the Linux caller's resolution-dep mounts; the
	// non-hermetic fallback does not expose them to the plugin.
	DepDirs map[string]string
	// Dir mirrors the Linux caller's build dir within the source tree
	// (sibling-sources design §9). The SubprocessPlugin bridge's request
	// carries only {call, source} — there is no field to announce the
	// consumer dir in — so this fallback REFUSES a widened build rather
	// than letting the plugin resolve against the context root.
	Dir string
}

var _ recipe.PluginCaller = SandboxedPluginCaller{}

func (p SandboxedPluginCaller) Call(kwargs map[string]any) (any, error) {
	ctx := p.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	// Fail fast instead of silently resolving without the declared deps — the
	// SubprocessPlugin bridge has no deps mounts, and a plugin that would read
	// them must not emit a wrong import set on this fallback path.
	if len(p.DepDirs) > 0 {
		return nil, fmt.Errorf("resolution deps are not supported by the non-Linux plugin fallback")
	}
	// Same reasoning for a widened (sibling-sources) build: handing the plugin
	// the context root with no consumer dir would make it resolve the wrong
	// directory and emit a wrong import set — which becomes a wrong cover and
	// a wrong KP, i.e. corrupt identity rather than an honest failure.
	if p.Dir != "" {
		return nil, fmt.Errorf("subdirectory builds (dir %q) are not supported by the non-Linux plugin fallback: the plugin bridge cannot announce the build dir", p.Dir)
	}
	// Materialize the plugin artifact to disk so SubprocessPlugin can exec it.
	dir, err := os.MkdirTemp("", "jobs-plugin-")
	if err != nil {
		return nil, fmt.Errorf("create plugin work dir: %w", err)
	}
	defer os.RemoveAll(dir)

	rc, err := p.Cl.Tar(ctx, p.PluginKey, "")
	if err != nil {
		return nil, fmt.Errorf("tar plugin %s: %w", p.PluginKey, err)
	}
	if err := extractTar(rc, dir); err != nil {
		rc.Close()
		return nil, fmt.Errorf("extract plugin: %w", err)
	}
	rc.Close()

	return recipe.SubprocessPlugin{Dir: dir, SourceDir: p.SourceDir, Ctx: ctx}.Call(kwargs)
}
