package runner

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/amber-store/core/key"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/jobs-build/jobs-iroh/amber"
)

// epoch is the fixed timestamp stamped on every layer entry and on the image
// config so that identical inputs produce a byte-identical, reproducible image.
var epoch = time.Unix(0, 0).UTC()

// imageEntrypointArgv resolves the entrypoint command against the image root "/"
// (a relative "bin/app" becomes "/bin/app"; a leading-"/" command is verbatim)
// and appends its args — the image analogue of run's cmdPath rule, rooted at "/".
func imageEntrypointArgv(ep Entrypoint) []string {
	cmdPath := path.Join("/", ep.Command)
	argv := make([]string, 0, 1+len(ep.Args))
	argv = append(argv, cmdPath)
	argv = append(argv, ep.Args...)
	return argv
}

// imageEnv builds the image config's Env: a structural base (PATH = /bin, plus
// the shell's bin when included; HOME=/tmp) overlaid with the entrypoint's env
// (which wins), sorted so the resulting config is deterministic.
func imageEnv(epEnv map[string]string, shellBOK key.Key, includeShell bool) []string {
	pathVal := "/bin"
	if includeShell {
		pathVal += ":" + storePath(shellBOK) + "/bin"
	}
	merged := map[string]string{
		"PATH": pathVal,
		"HOME": "/tmp",
	}
	for k, v := range epEnv {
		merged[k] = v
	}
	out := make([]string, 0, len(merged))
	for k, v := range merged {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// ImageOptions configures the image-building commands. Tag is the image
// reference recorded in the tarball's manifest (so `docker load` names it);
// IncludeShell bakes the vendored shell + /bin/sh + /jobs/shell into the image;
// Output receives the docker-load-able tarball.
type ImageOptions struct {
	Tag          string
	IncludeShell bool
	Output       io.Writer
}

// BuildImageByKey resolves an already-built output (build-output:K) and its
// runtime closure (build-output-deps:K) from the store and writes a
// docker-load-able image tarball to opts.Output, reusing the artifact's
// JOBS.entrypoint exactly as `jobs-client run` does. No building or sandbox is
// involved — it only reads amber trees and writes a tar, so it is
// cross-platform. resolveByKeyArtifact does the two-hop resolve with the
// direct build-output:K fallback, so both server-built keys (K) and local
// build identities (F) work.
func BuildImageByKey(ctx context.Context, st *amber.Store, k key.Key, platform, shellRef string, opts ImageOptions) error {
	if platform == "" {
		platform = Platform()
	}
	art, err := resolveByKeyArtifact(ctx, st, k, platform, shellRef, opts.IncludeShell)
	if err != nil {
		return err
	}
	img, err := assembleImage(ctx, st, art.bokSelf, art.depBOKs, art.shellKey, art.ep, platform, opts.IncludeShell)
	if err != nil {
		return err
	}
	tag := opts.Tag
	if tag == "" {
		tag = defaultTag(k.String())
	}
	return writeImageTar(img, tag, opts.Output)
}

// defaultTag derives an image reference from an identifier (a build key or a
// source dir basename) when the user gives no --tag.
func defaultTag(ident string) string {
	if len(ident) > 12 {
		ident = ident[:12]
	}
	return "jobs-" + ident + ":latest"
}

// writeImageTar writes img as a docker-load-able tarball to w, tagging it so the
// archive's manifest names the image (RepoTags). An empty default registry keeps
// a bare "name:tag" verbatim instead of normalising it to docker.io/library/….
func writeImageTar(img v1.Image, tag string, w io.Writer) error {
	if w == nil {
		return fmt.Errorf("no image output writer")
	}
	ref, err := name.NewTag(tag, name.WithDefaultRegistry(""))
	if err != nil {
		return fmt.Errorf("parse tag %q: %w", tag, err)
	}
	if err := tarball.Write(ref, img, w); err != nil {
		return fmt.Errorf("write image tar: %w", err)
	}
	return nil
}

// assembleImage builds an OCI image from a build artifact and its runtime
// closure: the artifact (selfBOK) lands at the image root, each transitive dep
// (depBOKs) is content-addressed under /jobs/store/<BOK>, and — when includeShell
// is set — the shell artifact is added under /jobs/store with /bin/sh and
// /jobs/shell compat symlinks. The config's Entrypoint/Env/WorkingDir come from
// the (run-shared) Entrypoint; OS/Architecture come from platform.
func assembleImage(ctx context.Context, st *amber.Store, selfBOK key.Key, depBOKs []key.Key, shellBOK key.Key, ep Entrypoint, platform string, includeShell bool) (v1.Image, error) {
	osName, arch, ok := strings.Cut(platform, "/")
	if !ok || osName == "" || arch == "" {
		return nil, fmt.Errorf("invalid platform %q (want os/arch, e.g. linux/amd64)", platform)
	}

	layer, err := storeLayer(ctx, st, selfBOK, depBOKs, shellBOK, includeShell)
	if err != nil {
		return nil, fmt.Errorf("build image layer: %w", err)
	}

	img, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		return nil, fmt.Errorf("append layer: %w", err)
	}

	cf, err := img.ConfigFile()
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cf = cf.DeepCopy()
	cf.OS = osName
	cf.Architecture = arch
	cf.Created = v1.Time{Time: epoch}
	cf.Config.Entrypoint = imageEntrypointArgv(ep)
	cf.Config.Cmd = nil
	cf.Config.Env = imageEnv(ep.Env, shellBOK, includeShell)
	cf.Config.WorkingDir = "/"

	img, err = mutate.ConfigFile(img, cf)
	if err != nil {
		return nil, fmt.Errorf("set config: %w", err)
	}
	return img, nil
}

// AssembleOCIImage builds the two-layer OCI-mediatype image that jobs-registry
// serves: the first layer is the runtime closure (each dep content-addressed
// under /jobs/store/<BOK>, plus the shell with its /bin/sh and /jobs/shell
// compat symlinks when shell is non-zero — script entrypoints carry fixed
// #!/bin/sh or #!/jobs/shell/bin/bash shebangs, exactly like `run` and the
// single-layer image), the second the build artifact at the image root plus a
// writable /tmp. Keeping the closure in its own layer means every image
// sharing a dep set (and shell) shares that layer blob, so registry clients
// re-pull only the artifact layer. A zero shell key bakes no shell (the
// --no-shell shape). ep may be nil for outputs without a JOBS.entrypoint; the
// config then carries no Entrypoint. Both layer streams are deterministic
// (epoch mtimes, sorted store entries), so blob digests are reproducible.
//
// Layers are UNCOMPRESSED (application/vnd.oci.image.layer.v1.tar): each
// layer's digest is the digest of its tar. Assembly therefore streams the
// content exactly once (to measure it), and the returned ImageLayers carry the
// spec that re-streams each layer's bytes — see LayerSpec.
func AssembleOCIImage(ctx context.Context, st *amber.Store, artifact key.Key, deps []key.Key, shell key.Key, ep *Entrypoint, platform string) (v1.Image, []ImageLayer, error) {
	osName, arch, ok := strings.Cut(platform, "/")
	if !ok || osName == "" || arch == "" {
		return nil, nil, fmt.Errorf("invalid platform %q (want os/arch, e.g. linux/amd64)", platform)
	}

	specs := []LayerSpec{
		{Kind: LayerDeps, Deps: deps, Shell: shell},
		{Kind: LayerArtifact, Artifact: artifact},
	}
	layers := make([]v1.Layer, 0, len(specs))
	assembled := make([]ImageLayer, 0, len(specs))
	for _, spec := range specs {
		l, err := newTarLayer(ctx, LayerTarOpener(st, spec))
		if err != nil {
			return nil, nil, fmt.Errorf("build %s layer: %w", spec.Kind, err)
		}
		layers = append(layers, l)
		assembled = append(assembled, ImageLayer{Digest: l.digest, Size: l.size, Spec: spec})
	}

	base := mutate.MediaType(empty.Image, types.OCIManifestSchema1)
	base = mutate.ConfigMediaType(base, types.OCIConfigJSON)
	img, err := mutate.AppendLayers(base, layers...)
	if err != nil {
		return nil, nil, fmt.Errorf("append layers: %w", err)
	}

	cf, err := img.ConfigFile()
	if err != nil {
		return nil, nil, fmt.Errorf("read config: %w", err)
	}
	cf = cf.DeepCopy()
	cf.OS = osName
	cf.Architecture = arch
	cf.Created = v1.Time{Time: epoch}
	var epEnv map[string]string
	if ep != nil {
		cf.Config.Entrypoint = imageEntrypointArgv(*ep)
		epEnv = ep.Env
	}
	cf.Config.Cmd = nil
	cf.Config.Env = imageEnv(epEnv, shell, shell != key.Key{})
	cf.Config.WorkingDir = "/"

	img, err = mutate.ConfigFile(img, cf)
	if err != nil {
		return nil, nil, fmt.Errorf("set config: %w", err)
	}
	return img, assembled, nil
}

// writeDepsTar writes the runtime-closure layer: /jobs/store scaffolding, each
// dep tree (and the shell, when non-zero) under /jobs/store/<BOK>, and — with
// the shell — the /bin/sh and /jobs/shell compat symlinks. The scaffolding
// dirs are written even with zero deps so the layer is never an empty tar.
// The stream is a pure function of the sorted dep set plus the shell, so
// images sharing a closure share the blob.
func writeDepsTar(ctx context.Context, st *amber.Store, w io.Writer, deps []key.Key, shell key.Key) error {
	includeShell := shell != (key.Key{})
	tw := tar.NewWriter(w)
	if err := writeDir(tw, "jobs/"); err != nil {
		return err
	}
	if err := writeDir(tw, "jobs/store/"); err != nil {
		return err
	}
	storeBOKs := append([]key.Key{}, deps...)
	if includeShell {
		storeBOKs = append(storeBOKs, shell)
	}
	for _, bok := range sortedDedupKeys(storeBOKs) {
		prefix := "jobs/store/" + bok.String() + "/"
		if err := writeDir(tw, prefix); err != nil {
			return err
		}
		if err := copyTreeInto(ctx, st, tw, bok, prefix); err != nil {
			return err
		}
	}
	if includeShell {
		shellRoot := storePath(shell)
		if err := writeSymlink(tw, "bin/sh", shellRoot+"/bin/sh"); err != nil {
			return err
		}
		if err := writeSymlink(tw, "jobs/shell", shellRoot); err != nil {
			return err
		}
	}
	return tw.Close()
}

// writeArtifactTar writes the build layer: the artifact tree at the image root
// and a writable /tmp (HOME).
func writeArtifactTar(ctx context.Context, st *amber.Store, w io.Writer, artifact key.Key) error {
	tw := tar.NewWriter(w)
	if err := copyTreeInto(ctx, st, tw, artifact, ""); err != nil {
		return err
	}
	if err := writeDir(tw, "tmp/"); err != nil {
		return err
	}
	return tw.Close()
}

// storeLayer wraps the runtime-closure tar (writeStoreTar) as a single OCI
// layer. The opener re-streams the tar from the store on each call
// (go-containerregistry reads it once for the diffID and once for the
// compressed blob); the stream is deterministic, so the layer digest is
// reproducible.
func storeLayer(ctx context.Context, st *amber.Store, selfBOK key.Key, depBOKs []key.Key, shellBOK key.Key, includeShell bool) (v1.Layer, error) {
	opener := func() (io.ReadCloser, error) {
		pr, pw := io.Pipe()
		go func() {
			pw.CloseWithError(writeStoreTar(ctx, st, pw, selfBOK, depBOKs, shellBOK, includeShell))
		}()
		return pr, nil
	}
	return tarball.LayerFromOpener(opener)
}

// writeStoreTar writes the image's single filesystem layer: the build target at
// the root, each transitive dependency (and the shell, when included) under
// /jobs/store/<BOK>, an empty /tmp, and — with the shell — the /bin/sh and
// /jobs/shell compat symlinks. Entries are normalised (uid/gid 0, fixed mtime)
// for reproducibility.
func writeStoreTar(ctx context.Context, st *amber.Store, w io.Writer, selfBOK key.Key, depBOKs []key.Key, shellBOK key.Key, includeShell bool) error {
	tw := tar.NewWriter(w)

	// 1. The build target's tree at the image root.
	if err := copyTreeInto(ctx, st, tw, selfBOK, ""); err != nil {
		return err
	}

	// 2. The content-addressed store: each dep (and the shell) at /jobs/store/<BOK>.
	storeBOKs := append([]key.Key{}, depBOKs...)
	if includeShell {
		storeBOKs = append(storeBOKs, shellBOK)
	}
	storeBOKs = sortedDedupKeys(storeBOKs)
	if len(storeBOKs) > 0 {
		if err := writeDir(tw, "jobs/"); err != nil {
			return err
		}
		if err := writeDir(tw, "jobs/store/"); err != nil {
			return err
		}
		for _, bok := range storeBOKs {
			prefix := "jobs/store/" + bok.String() + "/"
			if err := writeDir(tw, prefix); err != nil {
				return err
			}
			if err := copyTreeInto(ctx, st, tw, bok, prefix); err != nil {
				return err
			}
		}
	}

	// 3. A writable /tmp (HOME).
	if err := writeDir(tw, "tmp/"); err != nil {
		return err
	}

	// 4. The shell compat symlinks (absolute → preserved by image extraction).
	if includeShell {
		shellRoot := storePath(shellBOK)
		if err := writeSymlink(tw, "bin/sh", shellRoot+"/bin/sh"); err != nil {
			return err
		}
		if err := writeSymlink(tw, "jobs/shell", shellRoot); err != nil {
			return err
		}
	}

	return tw.Close()
}

// copyTreeInto streams the amber tree rooted at bok as a tar and re-emits every
// entry under prefix (prefix "" = the image root), normalising ownership and
// mtime. Symlinks keep their (relative) link targets, so a tree's internal
// symlinks (e.g. the shell's bin/sh → busybox) stay valid.
func copyTreeInto(ctx context.Context, st *amber.Store, tw *tar.Writer, bok key.Key, prefix string) error {
	rc, err := st.Tar(ctx, bok, "")
	if err != nil {
		return fmt.Errorf("stream %s: %w", bok.String(), err)
	}
	defer rc.Close()
	tr := tar.NewReader(rc)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read %s tar: %w", bok.String(), err)
		}
		name := strings.TrimPrefix(strings.TrimPrefix(h.Name, "./"), "/")
		if name == "" || name == "." {
			continue // tree root
		}
		nh := &tar.Header{
			Name:     prefix + name,
			Mode:     h.Mode,
			Size:     h.Size,
			Typeflag: h.Typeflag,
			Linkname: h.Linkname,
			ModTime:  epoch,
		}
		if h.Typeflag == tar.TypeDir && !strings.HasSuffix(nh.Name, "/") {
			nh.Name += "/"
		}
		if err := tw.WriteHeader(nh); err != nil {
			return err
		}
		if h.Typeflag == tar.TypeReg {
			if _, err := io.Copy(tw, tr); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeDir(tw *tar.Writer, name string) error {
	return tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: 0o755, ModTime: epoch})
}

func writeSymlink(tw *tar.Writer, name, target string) error {
	return tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeSymlink, Linkname: target, Mode: 0o777, ModTime: epoch})
}

// sortedDedupKeys returns the keys sorted by hex string with duplicates removed,
// so the layer's store entries are emitted in a deterministic order.
func sortedDedupKeys(keys []key.Key) []key.Key {
	seen := make(map[key.Key]bool, len(keys))
	out := make([]key.Key, 0, len(keys))
	for _, k := range keys {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
