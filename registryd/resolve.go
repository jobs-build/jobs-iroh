package registryd

import (
	"context"
	"errors"
	"fmt"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"

	"github.com/jobs-build/jobs-iroh/amberclient"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/runner"
)

// errNotFound marks a build (or one of its refs) the jobs-server does not
// have — the HTTP layer maps it to 404. errUpstream marks a failure to reach
// or sync from the jobs-server — mapped to 503.
var (
	errNotFound = errors.New("not found")
	errUpstream = errors.New("jobs-server unavailable")
)

// resolvedBuild is everything image assembly needs for one build key K.
type resolvedBuild struct {
	f        key.Key            // build identity: build-from:K's value, or K itself for direct outputs
	artifact key.Key            // the c/ subtree BOK (the build layer)
	deps     []key.Key          // runtime-closure BOKs (the deps layer)
	shell    key.Key            // shell artifact for /bin/sh + /jobs/shell; zero = none baked
	ep       *runner.Entrypoint // nil when the output has no (valid) JOBS.entrypoint
	platform string             // os/arch for the image config
}

// resolveBuild syncs build K from the jobs-server on demand and resolves the
// pieces of its image. fHint short-circuits the K→F lookup when a prior
// assembly recorded F (reassembly after cache expiry then needs no server ref
// listing — with the content still local it needs no server at all).
func (r *registry) resolveBuild(ctx context.Context, k key.Key, fHint key.Key) (resolvedBuild, error) {
	f := fHint
	if f == (key.Key{}) {
		var err error
		if f, err = r.resolveF(ctx, k); err != nil {
			return resolvedBuild{}, err
		}
	}

	outRoot, ok, err := r.ensureRef(ctx, "build-output:"+f.String())
	if err != nil {
		return resolvedBuild{}, err
	}
	if !ok {
		return resolvedBuild{}, fmt.Errorf("build-output:%s missing on jobs-server: %w", f.String(), errNotFound)
	}

	artifact, ok, err := r.st.TreeSubdir(ctx, outRoot, "c")
	if err != nil {
		return resolvedBuild{}, fmt.Errorf("descend into output of %s: %w", k.String(), err)
	}
	if !ok {
		return resolvedBuild{}, fmt.Errorf("build output for %s has no artifact (c/): %w", k.String(), errNotFound)
	}

	deps, err := r.resolveDeps(ctx, f)
	if err != nil {
		return resolvedBuild{}, err
	}

	platform := r.buildPlatform(ctx, k)
	shell, err := r.resolveShell(ctx, platform)
	if err != nil {
		return resolvedBuild{}, err
	}

	return resolvedBuild{
		f:        f,
		artifact: artifact,
		deps:     deps,
		shell:    shell,
		ep:       r.buildEntrypoint(ctx, artifact),
		platform: platform,
	}, nil
}

// resolveShell syncs the platform's shell artifact — script entrypoints carry
// #!/bin/sh or #!/jobs/shell/bin/bash shebangs, so registry images bake the
// shell like `run` and `jobs-client image` do. A server that has no shell for
// the platform is a stable state and yields a shell-less image (zero key,
// like --no-shell); a transport failure is an error — it must not silently
// change what the image contains.
func (r *registry) resolveShell(ctx context.Context, platform string) (key.Key, error) {
	if r.noShell {
		return key.Key{}, nil
	}
	shellRef := "shell:" + platform
	shell, ok, err := r.ensureRef(ctx, shellRef)
	if err != nil {
		return key.Key{}, err
	}
	if !ok {
		r.log.Warn("no shell on jobs-server; baking image without /bin/sh", "ref", shellRef)
		return key.Key{}, nil
	}
	return shell, nil
}

// resolveF maps K to the build identity F by looking refs up by name:
// build-from:K's VALUE is F, so a lookup avoids pulling the build-from closure
// (which reaches the whole source env). A bare build-output:K ref (a local
// build's F pushed directly) resolves to itself, mirroring
// runner.BuildImageByKey's fallback. Two point lookups whatever the server
// holds (issue #22: listing every ref to find one stopped working past
// 131,072 refs); a server predating TRefGet falls back to the listing.
func (r *registry) resolveF(ctx context.Context, k key.Key) (key.Key, error) {
	from, err := r.sync.GetRef(ctx, "build-from:"+k.String())
	switch {
	case err == nil:
		return from.Key, nil
	case errors.Is(err, amberclient.ErrUnsupported):
		return r.resolveFByListing(ctx, k)
	case !errors.Is(err, amberclient.ErrRefNotFound):
		return key.Key{}, fmt.Errorf("%w: look up build-from:%s: %v", errUpstream, k.String(), err)
	}
	if _, err := r.sync.GetRef(ctx, "build-output:"+k.String()); err == nil {
		return k, nil
	} else if !errors.Is(err, amberclient.ErrRefNotFound) {
		return key.Key{}, fmt.Errorf("%w: look up build-output:%s: %v", errUpstream, k.String(), err)
	}
	return key.Key{}, fmt.Errorf("build %s not on jobs-server: %w", k.String(), errNotFound)
}

// resolveFByListing is resolveF for a server without TRefGet: the two names
// are found by prefix-filtered listing (one unfiltered frame from such a
// server — the pre-#22 behaviour, bounded by that server's own limits).
func (r *registry) resolveFByListing(ctx context.Context, k key.Key) (key.Key, error) {
	fromName, outName := "build-from:"+k.String(), "build-output:"+k.String()
	var f key.Key
	var haveFrom, haveDirect bool
	err := r.sync.EachRef(ctx, "build-", func(ref amberclient.RefInfo) error {
		switch ref.Name {
		case fromName:
			f, haveFrom = ref.Key, true
		case outName:
			haveDirect = true
		}
		return nil
	})
	switch {
	case err != nil:
		return key.Key{}, fmt.Errorf("%w: list refs: %v", errUpstream, err)
	case haveFrom:
		return f, nil
	case haveDirect:
		return k, nil
	}
	return key.Key{}, fmt.Errorf("build %s not on jobs-server: %w", k.String(), errNotFound)
}

// resolveDeps lists the runtime closure recorded in build-output-deps:F. An
// absent deps ref (closure-free build) yields no deps, like pullHome.
func (r *registry) resolveDeps(ctx context.Context, f key.Key) ([]key.Key, error) {
	depsRoot, ok, err := r.ensureRef(ctx, "build-output-deps:"+f.String())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	ents, err := r.st.Ls(ctx, depsRoot, "")
	if err != nil {
		return nil, fmt.Errorf("list runtime closure: %w", err)
	}
	deps := make([]key.Key, 0, len(ents))
	for _, e := range ents {
		bok, ok := parseHexKey(e.Name)
		if !ok {
			return nil, fmt.Errorf("runtime closure entry %q is not a store key", e.Name)
		}
		deps = append(deps, bok)
	}
	return deps, nil
}

// ensureRef makes the named ref and its complete closure local: a complete
// local copy short-circuits, otherwise the ref is pulled from the server
// (verified objects, ref written after — see amberclient.Pull), with one
// paranoid re-pull if the closure still reads incomplete (the runnerd
// pattern). ok=false means the server does not have the ref.
func (r *registry) ensureRef(ctx context.Context, name string) (key.Key, bool, error) {
	if k, ok, err := r.st.GetKey(ctx, name); err != nil {
		return key.Key{}, false, err
	} else if ok && r.checkComplete(k) == nil {
		return k, true, nil
	}
	root, err := r.sync.Pull(ctx, name)
	if errors.Is(err, amberclient.ErrRefNotFound) {
		return key.Key{}, false, nil
	}
	if err != nil {
		return key.Key{}, false, fmt.Errorf("%w: pull %s: %v", errUpstream, name, err)
	}
	if r.checkComplete(root) != nil {
		if root, err = r.sync.Pull(ctx, name); err != nil {
			return key.Key{}, false, fmt.Errorf("%w: re-pull %s: %v", errUpstream, name, err)
		}
		if cerr := r.checkComplete(root); cerr != nil {
			return key.Key{}, false, fmt.Errorf("pull %s left store incomplete: %w", name, cerr)
		}
	}
	return root, true, nil
}

// checkComplete verifies the full object closure below root exists locally —
// presence of a parent proves nothing about its children.
func (r *registry) checkComplete(root key.Key) error {
	obj := r.st.Objects()
	_, err := fstree.CheckComplete(root, obj.Get, obj.Has, 0)
	return err
}

// buildPlatform learns K's os/arch from its stored definition (the tiny
// build:K ref written at submit). Any miss — an unsubmitted K, a direct F, a
// decode failure — falls back to the configured default; the image config
// must state SOME platform and the artifact bytes are unaffected.
func (r *registry) buildPlatform(ctx context.Context, k key.Key) string {
	root, ok, err := r.ensureRef(ctx, "build:"+k.String())
	if err != nil || !ok {
		if err != nil {
			r.log.Debug("platform lookup failed", "k", k.String(), "error", err)
		}
		return r.defaultPlatform
	}
	canon, err := r.st.ReadFile(ctx, root)
	if err != nil {
		r.log.Warn("read definition", "k", k.String(), "error", err)
		return r.defaultPlatform
	}
	def, err := builddef.DecodeDefinition(canon)
	if err != nil || def.Platform == "" {
		r.log.Warn("decode definition", "k", k.String(), "error", err)
		return r.defaultPlatform
	}
	return def.Platform
}

// buildEntrypoint reads the artifact's JOBS.entrypoint. Unlike `jobs-client
// run`/`image`, a missing or invalid entrypoint is tolerated (nil): the
// registry distributes the build either way and `docker run` can name a
// command.
func (r *registry) buildEntrypoint(ctx context.Context, artifact key.Key) *runner.Entrypoint {
	ents, err := r.st.Ls(ctx, artifact, "")
	if err != nil {
		r.log.Warn("list artifact root", "error", err)
		return nil
	}
	for _, e := range ents {
		if e.Name != runner.EntrypointFile || e.Key == (key.Key{}) {
			continue
		}
		b, err := r.st.ReadFile(ctx, e.Key)
		if err != nil {
			r.log.Warn("read "+runner.EntrypointFile, "error", err)
			return nil
		}
		ep, err := runner.DecodeEntrypoint(b)
		if err != nil {
			r.log.Warn("image gets no entrypoint", "error", err)
			return nil
		}
		return &ep
	}
	return nil
}
