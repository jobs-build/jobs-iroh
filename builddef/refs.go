package builddef

import (
	"fmt"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

// PluginResolved is the build-plugin-resolved:F ref content: the resolved
// plugins by name (build-from design §7) plus the recipe-declared resolution
// deps (resolution-deps design §4) — inputs materialized for the pin stage.
// Self-contained — each value embeds a complete import/build definition
// (build.md §4). The build definition is no longer carried here: the pin stage
// re-reads everything from the F-tree. omitempty keeps a dep-less resolve
// byte-identical to the pre-deps encoding.
type PluginResolved struct {
	Plugins map[string]Input `cbor:"plugins"`
	Deps    map[string]Input `cbor:"deps,omitempty"`
}

// ValidateDepName enforces that a resolution-dep name is a safe single path
// segment (it becomes /jobs/deps/<name> in the plugin sandbox): non-empty,
// bounded (a name over NAME_MAX would make the materialize mkdir fail forever),
// no separator or control bytes, not a dot dir (resolution-deps design §3.1).
func ValidateDepName(name string) error {
	if name == "" {
		return fmt.Errorf("resolution dep name must be non-empty")
	}
	if len(name) > 128 {
		return fmt.Errorf("resolution dep name %q exceeds 128 bytes", name[:32]+"…")
	}
	if strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("resolution dep name %q must not contain path separators", name)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("resolution dep name %q contains control characters", name)
		}
	}
	if name == "." || name == ".." {
		return fmt.Errorf("resolution dep name %q is reserved", name)
	}
	return nil
}

func EncodePluginResolved(p PluginResolved) ([]byte, error) { return canonEnc.Marshal(p) }

func DecodePluginResolved(b []byte) (PluginResolved, error) {
	var p PluginResolved
	err := cbor.Unmarshal(b, &p)
	return p, err
}

// PinnedInput is one input of a pinned build: a complete input definition plus
// the recipe-chosen name under which the build script finds it in JOBS_DEPS
// (build.md §7). The mount path is no longer pin-time-derivable — it is the
// dependency's content key (BOK), known only after the dep is built — so the
// runner injects name→/jobs/store/<BOK> at build time instead.
type PinnedInput struct {
	Name       string          `cbor:"name"`
	Kind       string          `cbor:"kind"`
	Definition cbor.RawMessage `cbor:"definition"`
}

// Pinned is the build-pinned:K ref content — the resolved job description
// (build.md §7). RuntimeDeps holds the 32-byte K of each runtime dependency, a
// subset of Inputs. Caches are the build's persistent cache declarations
// (build-cache design §4); omitempty keeps a cache-less Pinned byte-identical
// to the pre-cache encoding.
//
// Sources/Dir/Generated carry the covered source closure of a widened-context
// build (sibling-sources design §3.3): Sources is the sorted list of
// root-relative covered paths, Dir the build dir within the context (sandbox
// CWD), Generated the pin-synthesized files overlaid onto the covered tree
// (root-relative path → content, ≤ GeneratedMaxBytes total). All omitempty so
// legacy pins stay byte-identical.
type Pinned struct {
	Inputs      []PinnedInput     `cbor:"inputs"`
	Env         map[string]string `cbor:"env"`
	Script      string            `cbor:"script"`
	RuntimeDeps [][]byte          `cbor:"runtimeDeps"`
	Caches      []PinnedCache     `cbor:"caches,omitempty"`
	Resources   *PinnedResources  `cbor:"resources,omitempty"`
	// TimeoutSec is the recipe-declared wall-clock limit of a TEST run, in
	// seconds (tests design §6.1); 0 = the runner's default. build() never
	// sets it, so build pins stay byte-identical. Like Resources it rides
	// build-pinned and is deliberately NOT part of KT — raising a passed
	// test's limit must not re-run it.
	TimeoutSec int64    `cbor:"timeout_sec,omitempty"`
	Sources    []string `cbor:"sources,omitempty"`
	// Closure, when non-empty, is a COMPLETE cover of the source context
	// (source-closure design §4): the build dir is NOT auto-seeded and the
	// covered tree is exactly this expanded list. Mutually exclusive with
	// Sources — producers enforce it at eval, cover.Derive re-checks.
	Closure   []string          `cbor:"closure,omitempty"`
	Dir       string            `cbor:"dir,omitempty"`
	Generated map[string][]byte `cbor:"generated,omitempty"`
}

// GeneratedMaxBytes caps the total inline size of Pinned.Generated (the
// contract of the sibling-sources arc, design §7): pin hard-fails above it.
const GeneratedMaxBytes = 1 << 20

// PinnedResources is the recipe-declared CPU/RAM requirement carried in Pinned
// (multi-job-runner design). Deterministic from F (it rides build-pinned:F).
// Note: since the sibling-sources arc, the pinned bytes are part of KP — a
// resource change re-keys the buildrun memo (it always re-keyed F via the
// recipe text, so this is no behavioral change). A nil pointer (the common
// case) encodes to nothing, keeping resource-less pins byte-identical to the
// pre-resources encoding.
type PinnedResources struct {
	CPUMilli int64 `cbor:"cpu_milli,omitempty"`
	MemBytes int64 `cbor:"mem_bytes,omitempty"`
}

func EncodePinned(p Pinned) ([]byte, error) { return canonEnc.Marshal(p) }

func DecodePinned(b []byte) (Pinned, error) {
	var p Pinned
	err := cbor.Unmarshal(b, &p)
	return p, err
}

// ResolvedInput is one pinned input resolved to CONTENT (tests design
// §4.2): Out is the tree the sandbox mounts at /jobs/store/<BOK> — the c/
// artifact of a build, the whole output tree of an import — and Deps the
// build's materialized runtime-closure tree (build-output-deps), absent for
// imports. Definitions deliberately do not appear: a test's identity is what
// it runs against, not how that was described.
type ResolvedInput struct {
	Name string `cbor:"name"`
	Kind string `cbor:"kind"`
	Out  []byte `cbor:"out"`
	Deps []byte `cbor:"deps,omitempty"`
}

// TestJob is the job.cbor of a KT tree (tests design §4.2): the pinned
// test's script, env, sandbox dir and cache declarations, with its inputs
// resolved to content keys. Resources are deliberately absent — raising a
// passed test's memory must not re-run it. Canonical CBOR, so the same
// resolved job yields the same bytes everywhere.
type TestJob struct {
	Inputs []ResolvedInput   `cbor:"inputs"`
	Env    map[string]string `cbor:"env"`
	Script string            `cbor:"script"`
	Caches []PinnedCache     `cbor:"caches,omitempty"`
	Dir    string            `cbor:"dir,omitempty"`
}

func EncodeTestJob(j TestJob) ([]byte, error) { return canonEnc.Marshal(j) }

func DecodeTestJob(b []byte) (TestJob, error) {
	var j TestJob
	err := cbor.Unmarshal(b, &j)
	return j, err
}
