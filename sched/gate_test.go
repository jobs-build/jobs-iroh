package sched

// Gate allow-table tests — the same case matrix as jobs sched/gate, over the
// jobs-iroh types.

import (
	"testing"

	"github.com/amber-store/core/key"

	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/wire"
)

func fakeKey(t *testing.T, payload string) key.Key {
	t.Helper()
	k, err := key.New(key.Blob, uint64(len(payload)), []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func caches(ids ...string) []builddef.PinnedCache {
	out := make([]builddef.PinnedCache, 0, len(ids))
	for _, id := range ids {
		out = append(out, builddef.PinnedCache{ID: id})
	}
	return out
}

func TestGateAllowed(t *testing.T) {
	K := fakeKey(t, "node-key")
	F := fakeKey(t, "from-f-key")
	otherF := fakeKey(t, "a-different-f-key")

	tests := []struct {
		name     string
		kind     string
		nodeKey  key.Key
		platform string
		refs     []wire.RefProposal
		caches   []builddef.PinnedCache
		wantErr  bool
	}{
		{name: "import: exactly import-output:K", kind: wire.KindImport, nodeKey: K,
			refs: []wire.RefProposal{ref("import-output:"+K.String(), F)}},
		{name: "import: any other name fails", kind: wire.KindImport, nodeKey: K,
			refs: []wire.RefProposal{ref("import-output:"+F.String(), F)}, wantErr: true},
		{name: "buildfrom: build-from:K plus matching build-from-tree:F", kind: wire.KindBuildFrom, nodeKey: K,
			refs: []wire.RefProposal{ref("build-from:"+K.String(), F), ref("build-from-tree:"+F.String(), F)}},
		{name: "buildfrom: build-from-tree without build-from fails", kind: wire.KindBuildFrom, nodeKey: K,
			refs: []wire.RefProposal{ref("build-from-tree:"+F.String(), F)}, wantErr: true},
		{name: "buildfrom: build-from-tree for a different F fails", kind: wire.KindBuildFrom, nodeKey: K,
			refs:    []wire.RefProposal{ref("build-from:"+K.String(), F), ref("build-from-tree:"+otherF.String(), otherF)},
			wantErr: true},
		{name: "pluginresolve: own ref plus any tree (name==value)", kind: wire.KindPluginResolve, nodeKey: F,
			refs: []wire.RefProposal{ref("build-plugin-resolved:"+F.String(), K), ref("build-from-tree:"+otherF.String(), otherF)}},
		{name: "pluginresolve: tree name != value fails", kind: wire.KindPluginResolve, nodeKey: F,
			refs: []wire.RefProposal{ref("build-from-tree:"+otherF.String(), F)}, wantErr: true},
		{name: "pin: own ref", kind: wire.KindPin, nodeKey: F,
			refs: []wire.RefProposal{ref("build-pinned:"+F.String(), K)}},
		{name: "buildrun: output pair", kind: wire.KindBuildRun, nodeKey: F,
			refs: []wire.RefProposal{ref("build-output-deps:"+F.String(), K), ref("build-output:"+F.String(), K)}},
		{name: "buildrun: declared cache on the placement platform", kind: wire.KindBuildRun, nodeKey: F,
			platform: testPlatform, caches: caches("gocache"),
			refs: []wire.RefProposal{ref(builddef.CacheRefName("gocache", testPlatform), K)}},
		{name: "buildrun: undeclared cache fails", kind: wire.KindBuildRun, nodeKey: F,
			platform: testPlatform, caches: caches("other"),
			refs:    []wire.RefProposal{ref(builddef.CacheRefName("gocache", testPlatform), K)},
			wantErr: true},
		{name: "buildrun: cache on a foreign platform fails", kind: wire.KindBuildRun, nodeKey: F,
			platform: testPlatform, caches: caches("gocache"),
			refs:    []wire.RefProposal{ref(builddef.CacheRefName("gocache", "linux/arm64"), K)},
			wantErr: true},
		{name: "buildrun: build-from-tree not permitted", kind: wire.KindBuildRun, nodeKey: F,
			refs: []wire.RefProposal{ref("build-from-tree:"+F.String(), F)}, wantErr: true},
		{name: "buildvalue publishes nothing, ever", kind: wire.KindBuildValue, nodeKey: K,
			refs: []wire.RefProposal{ref("build-output:"+K.String(), F)}, wantErr: true},
		{name: "bookkeeping namespaces fail closed", kind: wire.KindBuildFrom, nodeKey: K,
			refs: []wire.RefProposal{ref("build:"+K.String(), K)}, wantErr: true},
		{name: "seed namespaces fail closed", kind: wire.KindImport, nodeKey: K,
			refs: []wire.RefProposal{ref("fetcher:github:"+testPlatform, K)}, wantErr: true},
		{name: "f-tree namespace is server-only", kind: wire.KindBuildFrom, nodeKey: K,
			refs: []wire.RefProposal{ref("build-from:"+K.String(), F), ref(FTreeRef(F), F)}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := gateAllowed(tc.kind, tc.nodeKey, tc.platform, tc.refs, tc.caches)
			if (err != nil) != tc.wantErr {
				t.Fatalf("gateAllowed err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
