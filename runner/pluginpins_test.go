package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
)

func ingestPluginArtifact(t *testing.T, ctx context.Context, st *amber.Store, files map[string]string) key.Key {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	kk, err := st.IngestDir(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	return kk
}

func TestReadPluginPins(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	withBundle := ingestPluginArtifact(t, ctx, st, map[string]string{
		"plugin": "#!/bin/sh\n",
		"fetchers.toml": `[[fetcher]]
name   = "gomod"
url    = "https://e/fg.tar.gz"
sha256 = "aa"
`,
	})
	pins, err := readPluginPins(ctx, st, withBundle)
	if err != nil {
		t.Fatal(err)
	}
	if pins == nil || pins.Entries["gomod"].URL != "https://e/fg.tar.gz" {
		t.Fatalf("pins: %+v", pins)
	}

	without := ingestPluginArtifact(t, ctx, st, map[string]string{"plugin": "#!/bin/sh\n"})
	pins, err = readPluginPins(ctx, st, without)
	if err != nil || pins != nil {
		t.Fatalf("absent bundle: pins=%+v err=%v", pins, err)
	}

	malformed := ingestPluginArtifact(t, ctx, st, map[string]string{
		"plugin":        "#!/bin/sh\n",
		"fetchers.toml": "[[fetcher]]\nname = \"x\"\n",
	})
	_, err = readPluginPins(ctx, st, malformed)
	if err == nil {
		t.Fatal("malformed bundle must error")
	}
	// A malformed bundle is the plugin artifact's fault — classified HARD via
	// pluginKeyError; store I/O errors stay plain (retryable at the caller).
	var pke pluginKeyError
	if !errors.As(err, &pke) {
		t.Fatalf("malformed bundle must be a pluginKeyError (hard), got %T: %v", err, err)
	}
}
