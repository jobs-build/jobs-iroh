//go:build linux

package runner_test

import (
	"context"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/runner"
)

// labelRecordingRefWriter captures every ref batch with labels intact.
type labelRecordingRefWriter struct {
	inner runner.RefWriter
	refs  []runner.Ref
}

func (r *labelRecordingRefWriter) WriteRef(ctx context.Context, name string, k key.Key) error {
	r.refs = append(r.refs, runner.Ref{Name: name, Key: k})
	return r.inner.WriteRef(ctx, name, k)
}

func (r *labelRecordingRefWriter) WriteRefs(ctx context.Context, refs []runner.Ref) (runner.PushTotals, error) {
	r.refs = append(r.refs, refs...)
	return r.inner.WriteRefs(ctx, refs)
}

// TestRunPin_PublishesRecipeName: the recipe's display-only name= must ride
// the build-pinned ref proposal's Label (never the Pinned bytes).
func TestRunPin_PublishesRecipeName(t *testing.T) {
	ctx := context.Background()
	st := buildEvalStore(t)
	platform := runner.Platform()

	recipeSrc := `
def build():
    return struct(
        inputs = {},
        env = {},
        script = "true",
        runtime_deps = [],
        name = "shiny demo build",
    )
`
	srcInput, _ := mkImportInputWithOutput(t, ctx, st, "src-fetcher",
		"https://example.com/pin-name-src.tgz", "BUILD.jobs", recipeSrc)
	_, buildK := makeBuildDef(t, ctx, st, srcInput, platform)

	brc := runner.BuildRunCfg{Platform: platform, CacheDir: t.TempDir()}
	rw := &labelRecordingRefWriter{inner: runner.NewLocalRefWriter(st)}

	bf := runner.RunBuildFrom(ctx, st, rw, brc, buildK)
	if bf.Failed || bf.Decline || bf.Cancelled {
		t.Fatalf("RunBuildFrom: %+v", bf)
	}
	f := bf.OutputKey
	if out := runner.RunPluginResolve(ctx, st, rw, brc, f); out.Failed || out.Decline || out.Cancelled {
		t.Fatalf("RunPluginResolve: %+v", out)
	}
	if out := runner.RunPin(ctx, st, rw, brc, f); out.Failed || out.Decline || out.Cancelled {
		t.Fatalf("RunPin: %+v", out)
	}

	label := ""
	for _, r := range rw.refs {
		if r.Name == "build-pinned:"+f.String() {
			label = r.Label
		}
	}
	if label != "shiny demo build" {
		t.Fatalf("build-pinned ref label = %q, want the recipe name (refs: %+v)", label, rw.refs)
	}
}
