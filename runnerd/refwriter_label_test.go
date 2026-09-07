package runnerd

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/runner"
)

func TestCollectRefWriterForwardsLabels(t *testing.T) {
	st, err := amber.Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	k, err := st.IngestFile(context.Background(), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	w := newCollectRefWriter(st)
	if _, err := w.WriteRefs(context.Background(), []runner.Ref{
		{Name: "build-pinned:abc", Key: k, Label: "nice name"},
		{Name: "plain:ref", Key: k},
	}); err != nil {
		t.Fatal(err)
	}
	var got map[string]string = map[string]string{}
	for _, rp := range w.refs {
		got[rp.Name] = rp.Label
	}
	if got["build-pinned:abc"] != "nice name" || got["plain:ref"] != "" {
		t.Fatalf("labels not forwarded: %+v", w.refs)
	}
	_ = key.Key{}
}
