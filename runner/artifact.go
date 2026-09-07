package runner

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
)

// resolvedArtifact is everything `run` and `image` need to consume an already-built
// output by its build key K: the artifact's content key (the c/ BOK), the shell
// artifact, the transitive runtime-closure dep BOKs, and the decoded
// JOBS.entrypoint. Resolving it is shared so the two commands cannot drift.
type resolvedArtifact struct {
	bokSelf  key.Key
	shellKey key.Key
	depBOKs  []key.Key
	ep       Entrypoint
}

// resolveByKeyArtifact resolves a build output (→ the c/ artifact subtree and its
// JOBS.entrypoint), the shell ref, and the runtime-closure dep BOKs. It serves two
// callers with different ref structures:
//
//   - SERVER-built artifacts (k = a definition K): output is at build-output:F via
//     the build-from:K → F bridge (two-hop).
//   - LOCAL builds (k = F, written directly by driveFStages → RunBuild): F has no
//     build-from:K bridge, so ResolveBuildOutput returns ok=false and the direct
//     build-output:F / build-output-deps:F refs (written by RunBuild) are used.
//
// The function tries two-hop first, then falls back to direct lookup for the local
// build case. It only reads amber trees (no sandbox), so it is cross-platform.
// platform and shellRef must already be defaulted by the caller when empty.
// needShell=false skips the shell lookup entirely (shellKey stays zero) — a
// --no-shell image never bakes it, so a store with no shell:<platform> ref
// (e.g. populated only by a remote-build pull) must still resolve.
func resolveByKeyArtifact(ctx context.Context, st *amber.Store, k key.Key, platform, shellRef string, needShell bool) (resolvedArtifact, error) {
	if platform == "" {
		platform = Platform()
	}
	if shellRef == "" {
		shellRef = "shell:" + platform
	}

	var shellKey key.Key
	if needShell {
		var ok bool
		var err error
		shellKey, ok, err = st.GetKey(ctx, shellRef)
		if err != nil {
			return resolvedArtifact{}, fmt.Errorf("resolve %s: %w", shellRef, err)
		}
		if !ok {
			return resolvedArtifact{}, fmt.Errorf("shell artifact %q not found (seed missing — restart to re-seed)", shellRef)
		}
	}

	// A build output is { c/ (the artifact) }; the artifact BOK is the c/
	// subtree's content key (build.md §4, §9). ResolveBuildArtifact does the
	// two-hop resolve (with the direct build-output:K fallback for local
	// builds) plus the c/ descent.
	bokSelf, ok, err := st.ResolveBuildArtifact(ctx, k)
	if err != nil {
		return resolvedArtifact{}, fmt.Errorf("resolve artifact for %s: %w", k.String(), err)
	}
	if !ok {
		return resolvedArtifact{}, fmt.Errorf("build-output for %s not found — build it first", k.String())
	}

	epBytes, err := readTreeFile(ctx, st, bokSelf, entrypointFile)
	if err != nil {
		return resolvedArtifact{}, fmt.Errorf("output is not runnable: %w", err)
	}
	ep, err := decodeEntrypoint(epBytes)
	if err != nil {
		return resolvedArtifact{}, err
	}

	depsKey, ok, err := st.ResolveBuildOutputDeps(ctx, k) // server artifact: build-from:K → F → build-output-deps:F
	if err != nil {
		return resolvedArtifact{}, fmt.Errorf("resolve build-output-deps for %s: %w", k.String(), err)
	}
	if !ok { // local build: fall back to direct build-output-deps:F lookup
		depsKey, ok, err = st.GetKey(ctx, "build-output-deps:"+k.String())
		if err != nil {
			return resolvedArtifact{}, fmt.Errorf("resolve build-output-deps:%s: %w", k.String(), err)
		}
	}
	if !ok {
		return resolvedArtifact{}, fmt.Errorf("build-output-deps for %s not found", k.String())
	}
	ents, err := st.Ls(ctx, depsKey, "")
	if err != nil {
		return resolvedArtifact{}, fmt.Errorf("list runtime closure: %w", err)
	}
	depBOKs := make([]key.Key, 0, len(ents))
	for _, e := range ents {
		bok, err := parseStoreEntryKey(e.Name)
		if err != nil {
			return resolvedArtifact{}, fmt.Errorf("runtime closure entry %q: %w", e.Name, err)
		}
		depBOKs = append(depBOKs, bok)
	}

	return resolvedArtifact{bokSelf: bokSelf, shellKey: shellKey, depBOKs: depBOKs, ep: ep}, nil
}

// resolveSubdirKey returns the content key of the immediate child directory
// named name within the amber tree rooted at root. Pure tree navigation, so it
// works on every platform.
func resolveSubdirKey(ctx context.Context, st *amber.Store, root key.Key, name string) (key.Key, error) {
	ents, err := st.Ls(ctx, root, "")
	if err != nil {
		return key.Key{}, err
	}
	for _, e := range ents {
		if e.Name == name {
			if e.Key == (key.Key{}) {
				return key.Key{}, fmt.Errorf("entry %q has no content key", name)
			}
			return e.Key, nil
		}
	}
	return key.Key{}, fmt.Errorf("no %q entry", name)
}

// readTreeFile reads the file at name (slash path) from the amber tree rooted at
// root by streaming the tree as a tar and matching the entry. Used to read
// c/JOBS.entrypoint out of an already-built output.
func readTreeFile(ctx context.Context, st *amber.Store, root key.Key, name string) ([]byte, error) {
	rc, err := st.Tar(ctx, root, "")
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	want := strings.TrimPrefix(name, "/")
	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("no %s at output root", name)
		}
		if err != nil {
			return nil, err
		}
		if strings.TrimPrefix(strings.TrimPrefix(hdr.Name, "./"), "/") == want {
			return io.ReadAll(tr)
		}
	}
}
