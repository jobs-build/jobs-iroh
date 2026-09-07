package sched

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"

	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/wire"
)

// gateError marks a ref batch the gate rejected — a broken (or hostile)
// runner, so the node hard-fails instead of retrying.
type gateError struct{ err error }

func (e *gateError) Error() string { return "gate: " + e.err.Error() }
func (e *gateError) Unwrap() error { return e.err }

// gateAllowed reports whether every entry in refs is a name the node
// (kind, key) may legitimately produce — a behavior-identical port of the
// jobs grain scheduler's allow-table (jobs sched/gate/gate.go), itself
// derived from the runner drivers' actual write sites:
//
//	import        (key K) -> exactly import-output:K
//	buildfrom     (key K) -> build-from:K; build-from-tree:<f> where <f> is the
//	                          hex of the Key VALUE reported for build-from:K in
//	                          the SAME batch (never an arbitrary tree)
//	pluginresolve (key F) -> build-plugin-resolved:F; any build-from-tree:<hex>
//	pin           (key F) -> build-pinned:F; any build-from-tree:<hex>
//	buildrun      (key F) -> build-output:F, build-output-deps:F;
//	                          build-cache:<id>:<platform> where <id> is declared
//	                          in build-pinned:F and <platform> is the build's
//	                          placement platform
//	buildvalue             -> nothing, ever (pure orchestrator)
//
// Every name additionally passes reference.ValidateName; every
// build-from-tree: suffix must be the canonical lowercase hex of a valid key
// AND equal that entry's reported Key value. Anything else — including
// import:, build:, fetcher:, shell:, f-tree/ — fails closed.
//
// declared is the build's own cache declaration set, decoded server-side
// from build-pinned:F (never trusted from the runner).
func gateAllowed(kind string, nodeKey key.Key, platform string, refs []wire.RefProposal, declared []builddef.PinnedCache) error {
	K := nodeKey.String()

	// A buildfrom batch may publish build-from-tree:<f> only for the exact F it
	// reports as build-from:K's own value in this same batch.
	var reportedF string
	if kind == wire.KindBuildFrom {
		for _, r := range refs {
			if r.Name != "build-from:"+K {
				continue
			}
			fk, err := key.Parse(r.Key)
			if err != nil {
				return &gateError{fmt.Errorf("ref %q: bad key value: %w", r.Name, err)}
			}
			reportedF = fk.String()
		}
	}

	declaredIDs := make(map[string]bool, len(declared))
	for _, c := range declared {
		declaredIDs[c.ID] = true
	}

	for _, r := range refs {
		if err := reference.ValidateName(r.Name); err != nil {
			return &gateError{fmt.Errorf("ref %q: %w", r.Name, err)}
		}
		if err := checkRefName(kind, K, reportedF, platform, declaredIDs, r); err != nil {
			return &gateError{err}
		}
	}
	return nil
}

// checkRefName is gateAllowed's per-entry decision, split out for
// testability of the individual branches (same structure as the jobs gate).
func checkRefName(kind, K, reportedF, platform string, declared map[string]bool, r wire.RefProposal) error {
	name := r.Name
	if strings.HasPrefix(name, "build-cache:") {
		if kind != wire.KindBuildRun {
			return fmt.Errorf("ref %q not permitted for %s|%s", name, kind, K)
		}
		id, plat, ok := builddef.ParseCacheRefName(name)
		if !ok {
			return fmt.Errorf("ref %q: malformed build-cache name", name)
		}
		if plat != platform {
			return fmt.Errorf("ref %q: platform %q does not match the build's placement platform %q", name, plat, platform)
		}
		if !declared[id] {
			return fmt.Errorf("ref %q: cache id %q not declared in build-pinned:%s", name, id, K)
		}
		return nil
	}
	if hexPart, ok := strings.CutPrefix(name, "build-from-tree:"); ok {
		switch kind {
		case wire.KindBuildFrom, wire.KindPluginResolve, wire.KindPin:
			parsedKey, err := parseHexKey(hexPart)
			if err != nil {
				return fmt.Errorf("ref %q: %w", name, err)
			}
			// hex.DecodeString accepts any case but key.String() always renders
			// lowercase — require the exact round-trip so only the canonical
			// name is accepted.
			if hexPart != parsedKey.String() {
				return fmt.Errorf("ref %q: non-canonical hex key (want %s)", name, parsedKey.String())
			}
			// Bind the entry's reported Key VALUE to the name's own hex suffix —
			// the legitimate writers always emit name==value for these refs.
			if !bytes.Equal(r.Key, parsedKey[:]) {
				return fmt.Errorf("ref %q: entry key value does not match the name's own key", name)
			}
			if kind == wire.KindBuildFrom && hexPart != reportedF {
				return fmt.Errorf("ref %q: build-from-tree key does not match this batch's build-from:%s value", name, K)
			}
			return nil
		default:
			return fmt.Errorf("ref %q not permitted for %s|%s", name, kind, K)
		}
	}

	var ok bool
	switch kind {
	case wire.KindImport:
		ok = name == "import-output:"+K
	case wire.KindBuildFrom:
		ok = name == "build-from:"+K
	case wire.KindPluginResolve:
		ok = name == "build-plugin-resolved:"+K
	case wire.KindPin:
		ok = name == "build-pinned:"+K
	case wire.KindBuildRun:
		ok = name == "build-output:"+K || name == "build-output-deps:"+K
	}
	if !ok {
		return fmt.Errorf("ref %q not permitted for %s|%s", name, kind, K)
	}
	return nil
}

// parseHexKey decodes a hex-encoded key.Key, as carried in a
// "build-from-tree:<hex>" ref name's suffix.
func parseHexKey(hexStr string) (key.Key, error) {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return key.Key{}, fmt.Errorf("invalid hex: %w", err)
	}
	return key.Parse(b)
}
