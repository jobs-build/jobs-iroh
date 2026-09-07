package sched

// PullRefs correctness per kind: exact ref lists against a real store, one
// case per stage-driver read pattern (docs/research/jobs-runner-stages.md).

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/amber-store/core/key"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/importdef"
	"github.com/jobs-build/jobs-iroh/wire"
)

// pullEnv is a store-only harness — computePullRefsLocked needs no NATS.
type pullEnv struct {
	t   *testing.T
	ctx context.Context
	st  *amber.Store
	s   *Sched
}

func newPullEnv(t *testing.T) *pullEnv {
	t.Helper()
	st, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	ctx := context.Background()
	s := &Sched{
		store: st,
		ctx:   ctx,
		log:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}
	return &pullEnv{t: t, ctx: ctx, st: st, s: s}
}

func (e *pullEnv) ingest(data string) key.Key {
	e.t.Helper()
	k, err := e.st.IngestFile(e.ctx, []byte(data))
	if err != nil {
		e.t.Fatal(err)
	}
	return k
}

func (e *pullEnv) putRef(name string, k key.Key) {
	e.t.Helper()
	if err := e.st.PutRef(e.ctx, name, k); err != nil {
		e.t.Fatal(err)
	}
}

func (e *pullEnv) pullRefs(n *node) []string {
	e.t.Helper()
	refs, err := e.s.computePullRefsLocked(n)
	if err != nil {
		e.t.Fatalf("computePullRefs: %v", err)
	}
	return refs
}

func (e *pullEnv) assertExact(got, want []string) {
	e.t.Helper()
	if len(got) != len(want) {
		e.t.Fatalf("pullRefs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			e.t.Fatalf("pullRefs = %v, want %v", got, want)
		}
	}
}

func importDefBytes(t *testing.T, def importdef.Definition) []byte {
	t.Helper()
	b, err := def.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func buildDefBytes(t *testing.T, def builddef.Definition) []byte {
	t.Helper()
	b, err := def.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func inputKey(t *testing.T, in builddef.Input) key.Key {
	t.Helper()
	k, err := in.Key()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestPullRefsImportNamedFetcher(t *testing.T) {
	e := newPullEnv(t)
	def := importDefBytes(t, importdef.Definition{Fetcher: "github", Params: []byte{0xf6}, Platform: testPlatform})
	k := inputKey(t, builddef.Input{Kind: builddef.KindImport, Definition: def})
	n := &node{id: nodeID{kind: wire.KindImport, key: k}, def: def, platform: testPlatform}

	// Seed ref absent: no pull (the driver reports the better error).
	e.assertExact(e.pullRefs(n), nil)

	// Seed ref present: pulled.
	e.putRef("fetcher:github:"+testPlatform, e.ingest("github fetcher artifact"))
	e.assertExact(e.pullRefs(n), []string{"fetcher:github:" + testPlatform})

	// The shell (the hermetic import root's userland) rides along once it
	// exists — present-only, like the build stages.
	e.putRef("shell:"+testPlatform, e.ingest("shell artifact"))
	e.assertExact(e.pullRefs(n), []string{
		"shell:" + testPlatform,
		"fetcher:github:" + testPlatform,
	})
}

func TestPullRefsImportFetcherDef(t *testing.T) {
	e := newPullEnv(t)
	fetcherBuild := buildDefBytes(t, builddef.Definition{
		Source:   builddef.Input{Kind: builddef.KindImport, Definition: importDefBytes(t, importdef.Definition{Fetcher: "tarball+https", Params: []byte{0xf6}, Platform: testPlatform})},
		Platform: testPlatform,
		Params:   []byte{0xf6},
	})
	kf := inputKey(t, builddef.Input{Kind: builddef.KindBuild, Definition: fetcherBuild})
	ff := e.ingest("fetcher F tree")
	e.putRef("build-from:"+kf.String(), ff)

	def := importDefBytes(t, importdef.Definition{Fetcher: "custom", Params: []byte{0xf6}, Platform: testPlatform, FetcherDef: fetcherBuild})
	k := inputKey(t, builddef.Input{Kind: builddef.KindImport, Definition: def})
	n := &node{id: nodeID{kind: wire.KindImport, key: k}, def: def, platform: testPlatform}

	// The artifact two-hop: build-from:K_f then build-output:F_f.
	e.assertExact(e.pullRefs(n), []string{
		"build-from:" + kf.String(),
		"build-output:" + ff.String(),
	})

	// Shell + the fetcher's runtime closure (build-output-deps:F_f) join once
	// they exist: the import root mounts both. Present-only, so a fetcher
	// built before runtime closures were published still schedules.
	e.putRef("shell:"+testPlatform, e.ingest("shell artifact"))
	e.putRef("build-output-deps:"+ff.String(), e.ingest("fetcher runtime closure"))
	e.assertExact(e.pullRefs(n), []string{
		"shell:" + testPlatform,
		"build-from:" + kf.String(),
		"build-output:" + ff.String(),
		"build-output-deps:" + ff.String(),
	})
}

func TestPullRefsBuildFrom(t *testing.T) {
	e := newPullEnv(t)

	t.Run("import source", func(t *testing.T) {
		src := builddef.Input{Kind: builddef.KindImport, Definition: importDefBytes(t, importdef.Definition{Fetcher: "github", Params: []byte{0xf6}, Platform: testPlatform})}
		def := buildDefBytes(t, builddef.Definition{Source: src, Platform: testPlatform, Params: []byte{0xf6}})
		k := inputKey(t, builddef.Input{Kind: builddef.KindBuild, Definition: def})
		n := &node{id: nodeID{kind: wire.KindBuildFrom, key: k}, def: def, platform: testPlatform}
		e.assertExact(e.pullRefs(n), []string{"import-output:" + inputKey(t, src).String()})
	})

	t.Run("build source", func(t *testing.T) {
		inner := buildDefBytes(t, builddef.Definition{
			Source:   builddef.Input{Kind: builddef.KindImport, Definition: importDefBytes(t, importdef.Definition{Fetcher: "github", Params: []byte{0xf6}, Platform: testPlatform})},
			Platform: testPlatform, Params: []byte{0xf6},
		})
		src := builddef.Input{Kind: builddef.KindBuild, Definition: inner}
		srcK := inputKey(t, src)
		fs := e.ingest("source F")
		e.putRef("build-from:"+srcK.String(), fs)

		def := buildDefBytes(t, builddef.Definition{Source: src, Platform: testPlatform, Params: []byte{0xf6}})
		k := inputKey(t, builddef.Input{Kind: builddef.KindBuild, Definition: def})
		n := &node{id: nodeID{kind: wire.KindBuildFrom, key: k}, def: def, platform: testPlatform}
		e.assertExact(e.pullRefs(n), []string{
			"build-from:" + srcK.String(),
			"build-output:" + fs.String(),
			"build-output-deps:" + fs.String(),
		})
	})

	t.Run("tree source", func(t *testing.T) {
		tk := e.ingest("the source tree")
		src, err := builddef.TreeInput(tk)
		if err != nil {
			t.Fatal(err)
		}
		def := buildDefBytes(t, builddef.Definition{Source: src, Platform: testPlatform, Params: []byte{0xf6}})
		k := inputKey(t, builddef.Input{Kind: builddef.KindBuild, Definition: def})
		n := &node{id: nodeID{kind: wire.KindBuildFrom, key: k}, def: def, platform: testPlatform}
		e.assertExact(e.pullRefs(n), []string{"build-from-tree:" + tk.String()})
	})
}

func TestPullRefsPluginResolve(t *testing.T) {
	e := newPullEnv(t)
	f := e.ingest("the F tree")
	n := &node{id: nodeID{kind: wire.KindPluginResolve, key: f}, platform: testPlatform}
	e.assertExact(e.pullRefs(n), []string{FTreeRef(f)})
}

func TestPullRefsPin(t *testing.T) {
	e := newPullEnv(t)
	f := e.ingest("the F tree")
	e.putRef("shell:"+testPlatform, e.ingest("shell artifact"))

	plugin := builddef.Input{Kind: builddef.KindImport, Definition: importDefBytes(t, importdef.Definition{Fetcher: "goplugin", Params: []byte{0xf6}, Platform: testPlatform})}
	depDef := buildDefBytes(t, builddef.Definition{
		Source:   builddef.Input{Kind: builddef.KindImport, Definition: importDefBytes(t, importdef.Definition{Fetcher: "github", Params: []byte{0xf6}, Platform: testPlatform})},
		Platform: testPlatform, Params: []byte{0xf6},
	})
	dep := builddef.Input{Kind: builddef.KindBuild, Definition: depDef}
	depK := inputKey(t, dep)
	fd := e.ingest("dep F")
	e.putRef("build-from:"+depK.String(), fd)

	n := &node{
		id:        nodeID{kind: wire.KindPin, key: f},
		platform:  testPlatform,
		pinInputs: []builddef.Input{plugin, dep},
	}
	e.assertExact(e.pullRefs(n), []string{
		FTreeRef(f),
		"build-plugin-resolved:" + f.String(),
		"shell:" + testPlatform,
		"import-output:" + inputKey(t, plugin).String(),
		"build-from:" + depK.String(),
		"build-output:" + fd.String(),
	})
}

func TestPullRefsBuildRun(t *testing.T) {
	e := newPullEnv(t)
	f := e.ingest("the F tree")

	imp := builddef.Input{Kind: builddef.KindImport, Definition: importDefBytes(t, importdef.Definition{Fetcher: "github", Params: []byte{0xf6}, Platform: testPlatform})}
	bldDef := buildDefBytes(t, builddef.Definition{
		Source:   builddef.Input{Kind: builddef.KindImport, Definition: importDefBytes(t, importdef.Definition{Fetcher: "tarball+https", Params: []byte{0xf6}, Platform: testPlatform})},
		Platform: testPlatform, Params: []byte{0xf6},
	})
	bld := builddef.Input{Kind: builddef.KindBuild, Definition: bldDef}
	bldK := inputKey(t, bld)
	fb := e.ingest("input F")
	e.putRef("build-from:"+bldK.String(), fb)

	// One warm cache (ref present), one cold (absent) — only the warm one is
	// pullable. No shell ref: buildrun still enqueues, the driver reports.
	e.putRef(builddef.CacheRefName("warm", testPlatform), e.ingest("cache state"))

	n := &node{
		id:        nodeID{kind: wire.KindBuildRun, key: f},
		platform:  testPlatform,
		runInputs: []builddef.Input{imp, bld},
		caches: []builddef.PinnedCache{
			{ID: "warm", Path: "/build/cache-warm"},
			{ID: "cold", Path: "/build/cache-cold"},
		},
	}
	// The buildrun node key is KP since the sibling-sources arc: the runner
	// pulls the kp-tree carrier (covered closure) + the build-pinned:<KP>
	// alias, never the F tree.
	e.assertExact(e.pullRefs(n), []string{
		KPTreeRef(f),
		"build-pinned:" + f.String(),
		"import-output:" + inputKey(t, imp).String(),
		"build-from:" + bldK.String(),
		"build-output:" + fb.String(),
		"build-output-deps:" + fb.String(),
		builddef.CacheRefName("warm", testPlatform),
	})
}
