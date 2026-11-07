package builddef

import (
	"bytes"
	"testing"

	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/importdef"
)

// importInput builds an import Input for a fetcher+url (test helper).
func importInput(t *testing.T, fetcher, url string) Input {
	t.Helper()
	p, err := importdef.CanonicalParams(map[string]any{"url": url})
	if err != nil {
		t.Fatal(err)
	}
	canon, err := importdef.Definition{Fetcher: fetcher, Params: p}.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	return Input{Kind: KindImport, Definition: canon}
}

func TestInput_Key(t *testing.T) {
	in := importInput(t, "tarball+https", "https://example.com/x.tgz")
	k, err := in.Key()
	if err != nil {
		t.Fatal(err)
	}
	// K must equal amber.FileKey of the embedded definition bytes (build.md §4).
	want, err := amber.FileKey(in.Definition)
	if err != nil {
		t.Fatal(err)
	}
	if k != want {
		t.Fatalf("Input.Key=%s want %s", k, want)
	}
}

func TestDefinition_CanonicalRoundTrip(t *testing.T) {
	src := importInput(t, "github", "https://github.com/o/r")
	p, err := importdef.CanonicalParams(map[string]any{"opt": 1})
	if err != nil {
		t.Fatal(err)
	}
	d := Definition{Source: src, Dir: "sub", Platform: "linux/amd64", Params: p, BuildJobs: []byte("def build(): pass\n")}
	c, err := d.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDefinition(c)
	if err != nil {
		t.Fatal(err)
	}
	if got.Platform != "linux/amd64" || got.Dir != "sub" || string(got.BuildJobs) != "def build(): pass\n" {
		t.Fatalf("decoded mismatch: %+v", got)
	}
}

func TestDefinition_EmptyOverrideOmitted(t *testing.T) {
	src := importInput(t, "github", "https://github.com/o/r")
	p, err := importdef.CanonicalParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	withEmpty := Definition{Source: src, Dir: "", Platform: "linux/amd64", Params: p, BuildJobs: []byte{}}
	withNil := Definition{Source: src, Platform: "linux/amd64", Params: p}
	a, err := withEmpty.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	b, err := withNil.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("empty override must encode like nil:\n a=%x\n b=%x", a, b)
	}
}

func TestDefinition_BuildFileRoundTrip(t *testing.T) {
	src := importInput(t, "github", "https://github.com/o/r")
	p, err := importdef.CanonicalParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	d := Definition{Source: src, Platform: "linux/amd64", Params: p, BuildFile: "app-a.jobs"}
	c, err := d.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDefinition(c)
	if err != nil {
		t.Fatal(err)
	}
	if got.BuildFile != "app-a.jobs" {
		t.Fatalf("decoded BuildFile=%q want %q", got.BuildFile, "app-a.jobs")
	}
}

// An empty BuildFile must encode identically to a definition without the field
// (omitempty), so pre-existing builds keep their K after this field is added.
func TestDefinition_EmptyBuildFileOmitted(t *testing.T) {
	src := importInput(t, "github", "https://github.com/o/r")
	p, err := importdef.CanonicalParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	withEmpty := Definition{Source: src, Platform: "linux/amd64", Params: p, BuildFile: ""}
	without := Definition{Source: src, Platform: "linux/amd64", Params: p}
	a, err := withEmpty.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	b, err := without.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("empty BuildFile must encode identically to absent:\n a=%x\n b=%x", a, b)
	}
}

// Two builds that differ only in BuildFile are different definitions → different K.
func TestDefinition_BuildFilePerturbsKey(t *testing.T) {
	src := importInput(t, "github", "https://github.com/o/r")
	p, err := importdef.CanonicalParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	base := Definition{Source: src, Platform: "linux/amd64", Params: p}
	a := base
	a.BuildFile = "a.jobs"
	b := base
	b.BuildFile = "b.jobs"
	ka, err := a.Key()
	if err != nil {
		t.Fatal(err)
	}
	kb, err := b.Key()
	if err != nil {
		t.Fatal(err)
	}
	if ka == kb {
		t.Fatalf("different BuildFile must yield different K, both %s", ka)
	}
}

func TestDefinition_ParamsValue(t *testing.T) {
	src := importInput(t, "github", "https://github.com/o/r")
	p, err := importdef.CanonicalParams(map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	d := Definition{Source: src, Platform: "linux/amd64", Params: p}
	v, err := d.ParamsValue()
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(map[string]any)
	if !ok || m["k"] != "v" {
		t.Fatalf("ParamsValue=%#v", v)
	}
}

func TestDefinition_TestFlagChangesKAndRoundTrips(t *testing.T) {
	src := importInput(t, "github", "https://github.com/o/r")
	p, err := importdef.CanonicalParams(map[string]any{"opt": 1})
	if err != nil {
		t.Fatal(err)
	}
	build := Definition{Source: src, Dir: "sub", Platform: "linux/amd64", Params: p, Ctx: CtxWidened}
	test := build
	test.Test = true
	bc, err := build.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	tc, err := test.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(bc, tc) {
		t.Fatal("test definition encodes identically to the build definition")
	}
	// A build definition never carries the field: its bytes (and K) are
	// unchanged by the field's existence.
	if bytes.Contains(bc, []byte("test")) {
		t.Fatalf("build definition carries a test field: %x", bc)
	}
	got, err := DecodeDefinition(tc)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Test || got.Dir != "sub" || got.Ctx != CtxWidened {
		t.Fatalf("decoded mismatch: %+v", got)
	}
	// Re-canonicalizing a decoded test def is a byte-identical fixpoint —
	// the submit canonicality check depends on Canonical copying the field.
	again, err := got.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again, tc) {
		t.Fatal("Canonical() strips the test field")
	}
}

func TestDefinition_RecipeFile(t *testing.T) {
	cases := []struct {
		def  Definition
		want string
	}{
		{Definition{}, "BUILD.jobs"},
		{Definition{Test: true}, "TESTS.jobs"},
		{Definition{BuildFile: "x/other.jobs"}, "x/other.jobs"},
		{Definition{Test: true, BuildFile: "smoke.jobs"}, "smoke.jobs"},
	}
	for _, c := range cases {
		if got := c.def.RecipeFile(); got != c.want {
			t.Errorf("RecipeFile(%+v) = %q, want %q", c.def, got, c.want)
		}
	}
}
