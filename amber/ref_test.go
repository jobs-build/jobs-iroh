package amber_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/jobs-build/jobs-iroh/amber"
)

func mustFileKey(t *testing.T, data string) key.Key {
	t.Helper()
	k, err := amber.FileKey([]byte(data))
	if err != nil {
		t.Fatalf("FileKey: %v", err)
	}
	return k
}

func TestRefs(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	k1 := mustFileKey(t, "one")
	k2 := mustFileKey(t, "two")

	before := time.Now()
	if err := s.PutRef(ctx, "builds/a", k1); err != nil {
		t.Fatalf("PutRef: %v", err)
	}
	ri, err := s.GetRef(ctx, "builds/a")
	if err != nil {
		t.Fatalf("GetRef: %v", err)
	}
	if ri.Name != "builds/a" || ri.Key != k1 {
		t.Errorf("GetRef = %+v, want builds/a -> %s", ri, k1)
	}
	if ri.CreatedAt.Before(before.Add(-time.Second)) || ri.CreatedAt.After(time.Now().Add(time.Second)) {
		t.Errorf("CreatedAt %v out of range", ri.CreatedAt)
	}
	gk, ok, err := s.GetKey(ctx, "builds/a")
	if err != nil || !ok || gk != k1 {
		t.Errorf("GetKey = (%s, %v, %v), want (%s, true, nil)", gk, ok, err, k1)
	}

	// Absent name: GetKey soft, GetRef sentinel.
	gk, ok, err = s.GetKey(ctx, "builds/absent")
	if err != nil || ok || gk != (key.Key{}) {
		t.Errorf("GetKey absent = (%s, %v, %v), want (zero, false, nil)", gk, ok, err)
	}
	if _, err := s.GetRef(ctx, "builds/absent"); !errors.Is(err, amber.ErrRefNotFound) {
		t.Errorf("GetRef absent err = %v, want ErrRefNotFound", err)
	}

	// Overwrite wins.
	if err := s.PutRef(ctx, "builds/a", k2); err != nil {
		t.Fatal(err)
	}
	if gk, _, _ := s.GetKey(ctx, "builds/a"); gk != k2 {
		t.Errorf("after overwrite GetKey = %s, want %s", gk, k2)
	}

	// List is complete and lexicographic.
	if err := s.PutRef(ctx, "builds/b", k1); err != nil {
		t.Fatal(err)
	}
	refs, err := s.ListRefs(ctx)
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	if len(refs) != 2 || refs[0].Name != "builds/a" || refs[1].Name != "builds/b" {
		t.Errorf("ListRefs = %+v, want [builds/a builds/b]", refs)
	}

	// The stored record is a canonical, unsigned reference.Reference.
	raw, err := s.RefStore().Get("builds/b")
	if err != nil {
		t.Fatalf("raw ref read: %v", err)
	}
	rec, err := reference.Decode(raw)
	if err != nil {
		t.Fatalf("reference.Decode of stored record: %v", err)
	}
	if rec.User != "" || len(rec.Signature) != 0 || len(rec.PublicKey) != 0 {
		t.Errorf("stored record is not unsigned/anonymous: %+v", rec)
	}

	// Delete, then delete again.
	if err := s.DeleteRef(ctx, "builds/a"); err != nil {
		t.Fatalf("DeleteRef: %v", err)
	}
	if _, err := s.GetRef(ctx, "builds/a"); !errors.Is(err, amber.ErrRefNotFound) {
		t.Errorf("GetRef after delete err = %v, want ErrRefNotFound", err)
	}
	if err := s.DeleteRef(ctx, "builds/a"); !errors.Is(err, amber.ErrRefNotFound) {
		t.Errorf("second DeleteRef err = %v, want ErrRefNotFound", err)
	}

	// Invalid names are rejected before hitting the store.
	if err := s.PutRef(ctx, "bad@name", k1); err == nil {
		t.Error("PutRef with '@' in the name succeeded")
	}
}

func TestCorruptRefIsErrorNotAbsence(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	// A present-but-undecodable record must surface as an error, never as
	// absence — corruption reading as ok=false would make callers re-run work
	// that already completed (doneness = ref existence).
	if err := s.RefStore().Put("builds/corrupt", []byte("not a canonical reference record")); err != nil {
		t.Fatalf("raw put: %v", err)
	}
	if _, ok, err := s.GetKey(ctx, "builds/corrupt"); err == nil || ok {
		t.Errorf("GetKey corrupt = (ok=%v, err=%v), want (false, decode error)", ok, err)
	}
	if _, err := s.GetRef(ctx, "builds/corrupt"); err == nil || errors.Is(err, amber.ErrRefNotFound) {
		t.Errorf("GetRef corrupt err = %v, want a decode error, not ErrRefNotFound", err)
	}
	if _, err := s.ListRefs(ctx); err == nil {
		t.Error("ListRefs with a corrupt record succeeded, want error")
	}
}

func TestResolveBuildChain(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	mkOut := func(withC bool) key.Key {
		dir := t.TempDir()
		if withC {
			write(t, filepath.Join(dir, "c", "artifact.txt"), []byte("artifact"), 0o644)
		} else {
			write(t, filepath.Join(dir, "meta.txt"), []byte("not an artifact"), 0o644)
		}
		k, err := s.IngestDir(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}

	k := mustFileKey(t, "definition")
	f := mustFileKey(t, "from-tree")
	out := mkOut(true)
	if err := s.PutRef(ctx, "build-from:"+k.String(), f); err != nil {
		t.Fatal(err)
	}
	if err := s.PutRef(ctx, "build-output:"+f.String(), out); err != nil {
		t.Fatal(err)
	}

	// Happy two-hop path.
	got, ok, err := s.ResolveBuildOutput(ctx, k)
	if err != nil || !ok || got != out {
		t.Errorf("ResolveBuildOutput = (%s, %v, %v), want (%s, true, nil)", got, ok, err, out)
	}

	// Deps: absent until its ref exists.
	if _, ok, err := s.ResolveBuildOutputDeps(ctx, k); err != nil || ok {
		t.Errorf("ResolveBuildOutputDeps before ref = (ok=%v, err=%v), want (false, nil)", ok, err)
	}
	deps := mkOut(true)
	if err := s.PutRef(ctx, "build-output-deps:"+f.String(), deps); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := s.ResolveBuildOutputDeps(ctx, k); err != nil || !ok || got != deps {
		t.Errorf("ResolveBuildOutputDeps = (%s, %v, %v), want (%s, true, nil)", got, ok, err, deps)
	}

	// Artifact = the c/ subtree of the output.
	wantC, found, err := s.TreeSubdir(ctx, out, "c")
	if err != nil || !found {
		t.Fatalf("TreeSubdir c: found=%v err=%v", found, err)
	}
	ak, ok, err := s.ResolveBuildArtifact(ctx, k)
	if err != nil || !ok || ak != wantC {
		t.Errorf("ResolveBuildArtifact = (%s, %v, %v), want (%s, true, nil)", ak, ok, err, wantC)
	}

	// Absent bridge: ok=false, no error — a sync race, not a failure.
	k2 := mustFileKey(t, "definition-2")
	if _, ok, err := s.ResolveBuildOutput(ctx, k2); err != nil || ok {
		t.Errorf("ResolveBuildOutput absent = (ok=%v, err=%v), want (false, nil)", ok, err)
	}
	if _, ok, err := s.ResolveBuildArtifact(ctx, k2); err != nil || ok {
		t.Errorf("ResolveBuildArtifact absent = (ok=%v, err=%v), want (false, nil)", ok, err)
	}

	// build-output:K fallback for local builds without the bridge.
	k3 := mustFileKey(t, "definition-3")
	out3 := mkOut(true)
	if err := s.PutRef(ctx, "build-output:"+k3.String(), out3); err != nil {
		t.Fatal(err)
	}
	wantC3, _, err := s.TreeSubdir(ctx, out3, "c")
	if err != nil {
		t.Fatal(err)
	}
	if ak, ok, err := s.ResolveBuildArtifact(ctx, k3); err != nil || !ok || ak != wantC3 {
		t.Errorf("fallback ResolveBuildArtifact = (%s, %v, %v), want (%s, true, nil)", ak, ok, err, wantC3)
	}

	// Output without c/: permanent, errors.Is-able failure.
	k4 := mustFileKey(t, "definition-4")
	f4 := mustFileKey(t, "from-tree-4")
	if err := s.PutRef(ctx, "build-from:"+k4.String(), f4); err != nil {
		t.Fatal(err)
	}
	if err := s.PutRef(ctx, "build-output:"+f4.String(), mkOut(false)); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.ResolveBuildArtifact(ctx, k4); ok || !errors.Is(err, amber.ErrNoArtifact) {
		t.Errorf("no-c/ ResolveBuildArtifact = (ok=%v, err=%v), want (false, ErrNoArtifact)", ok, err)
	}
}

type fakeGuard struct {
	prepared []key.Key
	commits  int
	aborts   int
	err      error
}

func (g *fakeGuard) PrepareRef(root key.Key) (func(), func(), error) {
	g.prepared = append(g.prepared, root)
	if g.err != nil {
		return nil, nil, g.err
	}
	return func() { g.commits++ }, func() { g.aborts++ }, nil
}

func TestObserverFiresOnGetRef(t *testing.T) {
	ctx := context.Background()
	s, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	k, err := s.IngestFile(ctx, []byte("observed"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutRef(ctx, "obs", k); err != nil {
		t.Fatal(err)
	}

	var touched []string
	s.SetObserver(func(name string) { touched = append(touched, name) })

	if _, _, err := s.GetKey(ctx, "obs"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GetKey(ctx, "absent"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListRefs(ctx); err != nil {
		t.Fatal(err)
	}
	// Exactly one touch: the successful GetKey→GetRef. Absent names and
	// listings are not accesses.
	if len(touched) != 1 || touched[0] != "obs" {
		t.Fatalf("touched = %v, want [obs]", touched)
	}
}

func TestRefGuardCommitAndAbort(t *testing.T) {
	ctx := context.Background()
	s, err := amber.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	k, err := s.IngestFile(ctx, []byte("guarded"))
	if err != nil {
		t.Fatal(err)
	}

	g := &fakeGuard{}
	s.SetRefGuard(g)
	if err := s.PutRef(ctx, "guarded", k); err != nil {
		t.Fatal(err)
	}
	if len(g.prepared) != 1 || g.prepared[0] != k || g.commits != 1 || g.aborts != 0 {
		t.Fatalf("guard = %+v", g)
	}

	g.err = errors.New("closure incomplete")
	if err := s.PutRef(ctx, "refused", k); err == nil {
		t.Fatal("guard error must refuse the put")
	}
	if _, ok, _ := s.GetKey(ctx, "refused"); ok {
		t.Fatal("refused ref must not exist")
	}
}
