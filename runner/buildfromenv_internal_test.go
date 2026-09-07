package runner

import (
	"context"
	"testing"

	"github.com/amber-store/core/key"
)

func TestEnsureBuildFromTreeNoopWhenAbsentLocally(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	var f key.Key
	f[0] = 0x11 // a key whose build-from-tree:F ref does not exist
	// An absent ref must be a clean no-op (best-effort), not an error.
	if o := ensureBuildFromTree(ctx, st, f); o != nil {
		t.Fatalf("ensureBuildFromTree(absent) = %+v, want nil", *o)
	}
}

func TestEnsureBuildFromTreeNoopWhenPresentLocally(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	k, err := st.IngestFile(ctx, []byte("tree-stand-in"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "build-from-tree:"+k.String(), k); err != nil {
		t.Fatal(err)
	}
	if o := ensureBuildFromTree(ctx, st, k); o != nil {
		t.Fatalf("ensureBuildFromTree(present) = %+v, want nil", *o)
	}
}

func TestEnsureBuildDefNoopWhenAbsentLocally(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	var k key.Key
	k[0] = 0x22 // a key whose build:K ref does not exist
	if o := ensureBuildDef(ctx, st, k); o != nil {
		t.Fatalf("ensureBuildDef(absent) = %+v, want nil", *o)
	}
}

func TestEnsureBuildDefNoopWhenPresentLocally(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	k, err := st.IngestFile(ctx, []byte("def-stand-in"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutRef(ctx, "build:"+k.String(), k); err != nil {
		t.Fatal(err)
	}
	if o := ensureBuildDef(ctx, st, k); o != nil {
		t.Fatalf("ensureBuildDef(present) = %+v, want nil", *o)
	}
}
