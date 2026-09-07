package recipe

import (
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"go.starlark.net/starlark"
)

func TestSubbuild_ConstructsTreeSourcedBuild(t *testing.T) {
	root, err := amber.FileKey([]byte("build-root"))
	if err != nil {
		t.Fatal(err)
	}
	sb := makeSubbuild("linux/amd64", root, key.Key{})
	v := callBuiltin(t, sb, []starlark.Tuple{{starlark.String("dir"), starlark.String("rust")}})
	in, ok := v.(*Input)
	if !ok {
		t.Fatalf("subbuild returned %T", v)
	}
	if in.In.Kind != builddef.KindBuild {
		t.Fatalf("kind=%q want build", in.In.Kind)
	}
	def, err := builddef.DecodeDefinition(in.In.Definition)
	if err != nil {
		t.Fatal(err)
	}
	if def.Dir != "rust" {
		t.Fatalf("dir=%q want rust", def.Dir)
	}
	if def.Platform != "linux/amd64" {
		t.Fatalf("platform=%q want linux/amd64", def.Platform)
	}
	if def.Source.Kind != builddef.KindTree {
		t.Fatalf("source kind=%q want tree", def.Source.Kind)
	}
	gotKey, err := builddef.DecodeTreeKey(def.Source.Definition)
	if err != nil {
		t.Fatal(err)
	}
	if gotKey != root {
		t.Fatalf("tree source key=%s want %s", gotKey, root)
	}
}

func TestSubbuild_RejectsNonDescendant(t *testing.T) {
	root, _ := amber.FileKey([]byte("r"))
	sb := makeSubbuild("linux/amd64", root, key.Key{})
	for _, bad := range []string{"", ".", "..", "/abs", "a/../b", "a//b", "a/."} {
		_, err := starlark.Call(newThread(), sb, nil, []starlark.Tuple{{starlark.String("dir"), starlark.String(bad)}})
		if err == nil {
			t.Fatalf("subbuild(%q) must error", bad)
		}
	}
}

func TestSubbuild_UnavailableWhenNoSourceKey(t *testing.T) {
	sb := makeSubbuild("linux/amd64", key.Key{}, key.Key{})
	_, err := starlark.Call(newThread(), sb, nil, []starlark.Tuple{{starlark.String("dir"), starlark.String("rust")}})
	if err == nil {
		t.Fatal("subbuild must error when source content key is unset")
	}
}

func TestSubbuild_BuildJobsOverride(t *testing.T) {
	root, _ := amber.FileKey([]byte("r"))
	sb := makeSubbuild("linux/amd64", root, key.Key{})
	v := callBuiltin(t, sb, []starlark.Tuple{
		{starlark.String("dir"), starlark.String("rust")},
		{starlark.String("build_jobs"), starlark.String("def build(): pass\n")},
	})
	def, err := builddef.DecodeDefinition(v.(*Input).In.Definition)
	if err != nil {
		t.Fatal(err)
	}
	if string(def.BuildJobs) != "def build(): pass\n" {
		t.Fatalf("BuildJobs=%q", def.BuildJobs)
	}
}

func TestSubbuild_buildFile(t *testing.T) {
	root, _ := amber.FileKey([]byte("r"))
	sb := makeSubbuild("linux/amd64", root, key.Key{})
	v := callBuiltin(t, sb, []starlark.Tuple{
		{starlark.String("dir"), starlark.String("rust")},
		{starlark.String("build_file"), starlark.String("server.jobs")},
	})
	def, err := builddef.DecodeDefinition(v.(*Input).In.Definition)
	if err != nil {
		t.Fatal(err)
	}
	if def.BuildFile != "server.jobs" {
		t.Fatalf("BuildFile=%q want server.jobs", def.BuildFile)
	}
}

// subbuild is wired into BOTH stages (plugins() and build()) via basePredeclared.
func TestSubbuild_AvailableInBuildStage(t *testing.T) {
	root, _ := amber.FileKey([]byte("r"))
	cfg := EvalConfig{Platform: "linux/amd64", Source: MapSource{}, SourceContentKey: root}
	recipeSrc := []byte("def build():\n    return struct(inputs={\"s\": subbuild(\"sub\")}, env={}, script=\"\", runtime_deps=[])\n")
	res, err := EvalBuild(cfg, recipeSrc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Inputs) != 1 || res.Inputs[0].In.Kind != builddef.KindBuild {
		t.Fatalf("inputs=%+v", res.Inputs)
	}
}

func TestSubbuild_AvailableInPluginsStage(t *testing.T) {
	root, _ := amber.FileKey([]byte("r"))
	cfg := EvalConfig{Platform: "linux/amd64", Source: MapSource{}, SourceContentKey: root}
	recipeSrc := []byte("def plugins():\n    return {\"p\": subbuild(\"sub\")}\n")
	res, err := EvalPlugins(cfg, recipeSrc)
	if err != nil {
		t.Fatal(err)
	}
	if res.Plugins["p"].Kind != builddef.KindBuild {
		t.Fatalf("plugins[p]=%+v", res.Plugins["p"])
	}
}
