package runner

import (
	"context"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/fxamacker/cbor/v2"
	"github.com/jobs-build/jobs-iroh/builddef"
)

// importInput fabricates a distinct pinned import input and returns it with
// its identity K.
func importInput(t *testing.T, name string, n byte) (builddef.PinnedInput, key.Key) {
	t.Helper()
	def := cbor.RawMessage{0x41, n} // one-byte CBOR byte string: distinct per n
	k, err := (builddef.Input{Kind: builddef.KindImport, Definition: def}).Key()
	if err != nil {
		t.Fatal(err)
	}
	return builddef.PinnedInput{Name: name, Kind: builddef.KindImport, Definition: def}, k
}

// A missing import-output ref must surface as a retryable outcome naming the
// ref (the scheduler re-offers later) — pinned across the concurrent-resolution
// change. (jobs also proved the remote pull-on-miss concurrency with a fake
// remote-sync seam; single-store jobs-iroh has no such injectable seam, so
// only the classification half ports.)
func TestCollectStoreMissingRefIsRetryable(t *testing.T) {
	st := openTestStore(t)
	pi, _ := importInput(t, "dep", 1)

	boks, out := collectStore(context.Background(), st, []builddef.PinnedInput{pi}, nil)
	if out == nil {
		t.Fatalf("want a failure outcome, got boks %v", boks)
	}
	if !out.Failed || out.Class != "retryable" || out.Phase != "resolving" {
		t.Fatalf("want retryable resolving outcome, got %+v", out)
	}
}

// collectStore fills jobsDeps with name→/jobs/store/<BOK> for every resolvable
// input and returns each input's BOK.
func TestCollectStoreFillsJobsDeps(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	const n = 4
	inputs := make([]builddef.PinnedInput, 0, n)
	wantDeps := map[string]string{}
	for i := range n {
		pi, k := importInput(t, string(rune('a'+i)), byte(i+1))
		inputs = append(inputs, pi)
		out, err := st.IngestFile(ctx, []byte{byte(0x10 + i)})
		if err != nil {
			t.Fatal(err)
		}
		if err := st.PutRef(ctx, "import-output:"+k.String(), out); err != nil {
			t.Fatal(err)
		}
		wantDeps[pi.Name] = storePath(out)
	}

	jobsDeps := map[string]string{}
	boks, out := collectStore(ctx, st, inputs, jobsDeps)
	if out != nil {
		t.Fatalf("collectStore failed: %+v", out)
	}
	if len(boks) != n {
		t.Fatalf("got %d boks, want %d", len(boks), n)
	}
	for name, want := range wantDeps {
		if jobsDeps[name] != want {
			t.Fatalf("jobsDeps[%q] = %q, want %q", name, jobsDeps[name], want)
		}
	}
}
