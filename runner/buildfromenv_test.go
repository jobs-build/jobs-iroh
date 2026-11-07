package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jobs-build/jobs-iroh/importdef"
)

func TestLoadBuildFromEnv_SourceContentKey(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "BUILD.jobs"), []byte("def build(): pass\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	envKey, err := st.IngestDir(ctx, srcDir)
	if err != nil {
		t.Fatal(err)
	}
	params, err := importdef.CanonicalParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	f, err := st.BuildFromTree(ctx, envKey, "", params, "linux/amd64", nil)
	if err != nil {
		t.Fatal(err)
	}

	env, o := loadBuildFromEnv(ctx, st, f)
	if o != nil {
		t.Fatalf("loadBuildFromEnv: %+v", *o)
	}
	defer env.cleanup()

	if env.SourceContentKey != envKey {
		t.Fatalf("SourceContentKey=%s want env subtree key %s", env.SourceContentKey, envKey)
	}
}

func TestLoadBuildFromEnv_TestFTree(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	srcDir := t.TempDir()
	// Both recipes present: the F-tree's TESTS.jobs entry decides, never the
	// build root's BUILD.jobs.
	if err := os.WriteFile(filepath.Join(srcDir, "BUILD.jobs"), []byte("def build(): pass\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "TESTS.jobs"), []byte("def test(): pass\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	envKey, err := st.IngestDir(ctx, srcDir)
	if err != nil {
		t.Fatal(err)
	}
	params, err := importdef.CanonicalParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	f, err := st.TestFromTree(ctx, envKey, "", params, "linux/amd64", []byte("def test(): pass\n"))
	if err != nil {
		t.Fatal(err)
	}
	env, o := loadBuildFromEnv(ctx, st, f)
	if o != nil {
		t.Fatalf("loadBuildFromEnv: %+v", *o)
	}
	defer env.cleanup()
	if !env.Test || string(env.Recipe) != "def test(): pass\n" {
		t.Fatalf("Test=%v Recipe=%q, want a test env with the spliced recipe", env.Test, env.Recipe)
	}

	// A build F-tree over the same env is NOT a test, even though the build
	// root happens to contain a TESTS.jobs.
	fb, err := st.BuildFromTree(ctx, envKey, "", params, "linux/amd64", nil)
	if err != nil {
		t.Fatal(err)
	}
	benv, o := loadBuildFromEnv(ctx, st, fb)
	if o != nil {
		t.Fatalf("loadBuildFromEnv(build): %+v", *o)
	}
	defer benv.cleanup()
	if benv.Test || string(benv.Recipe) != "def build(): pass\n" {
		t.Fatalf("Test=%v Recipe=%q, want the build env", benv.Test, benv.Recipe)
	}
}
