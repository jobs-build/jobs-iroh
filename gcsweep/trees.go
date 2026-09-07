package gcsweep

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/amber-store/core/key"
)

// fetcherStaleAfter is how old an abandoned fetcher-* work dir must be
// before the sweep collects it: live ones are torn down by their own
// cleanup within a job's lifetime, so anything past a day is a crash
// leftover.
const fetcherStaleAfter = 24 * time.Hour

// parseTreeKey recovers the store key from a <CacheDir>/trees/ entry name.
// key.Key.String() (amber-store-core/key/key.go) emits the plain lowercase
// hex encoding of the 32-byte key with no type prefix or separator
// (hex.EncodeToString(k[:])) — there is no ParseString/FromString
// counterpart in the key package, only Parse([]byte), so recovering a Key
// from a directory name is hex-decode then Parse (which also validates
// canonical form: reserved bit clear, defined type, minimal length
// encoding). A name that isn't valid hex, isn't exactly 32 bytes, or
// doesn't validate is not a key this package ever wrote — junk to be swept.
//
// stagedBinDir (runner/importexec_linux.go:390-398) writes a companion
// directory at <CacheDir>/trees/<shellKey>.bin — the synthesized /bin farm
// for that shell tree, keyed by the same shellKey and sharing its
// lifetime (both are staged and torn down alongside imports of that
// shell). It carries no independent key of its own, so callers strip a
// single ".bin" suffix before parsing: the remaining hex is the OWNING
// key, and the ".bin" dir lives and dies with it, exactly like the bare
// tree dir.
func parseTreeKey(name string) (key.Key, error) {
	if base, ok := strings.CutSuffix(name, ".bin"); ok {
		name = base
	}
	b, err := hex.DecodeString(name)
	if err != nil {
		return key.Key{}, err
	}
	return key.Parse(b)
}

// sweepTrees collects the cache dir's derived state (design §3): staged
// tree materializations whose store object is gone (the store is the
// truth — a tree in use belongs to refs the retention window shields, so
// its objects survive the cycle and Has holds), and fetcher work dirs
// abandoned by a crash. Best-effort: failures log and retry next sweep.
func (g *Sweeper) sweepTrees() (trees, fetchers int) {
	if g.cacheDir == "" {
		return 0, 0
	}
	cutoff := time.Now().Add(-fetcherStaleAfter)
	treesDir := filepath.Join(g.cacheDir, "trees")
	if entries, err := os.ReadDir(treesDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			// stagedTree and stagedBinDir (runner/importexec_linux.go:366,
			// :402) materialize into a staging-/bin-staging- MkdirTemp dir
			// inside trees/ before the atomic rename that publishes the
			// final <key>[.bin] path. That temp name never parses as a
			// key, so without this guard the junk rule below would delete
			// it mid-write — worst case the rename then wins the race and
			// publishes an incomplete tree at the canonical path, which
			// stagedTree's os.Stat fast-path would then reuse forever.
			// Skip while young (in-flight or a very recent run); collect
			// only once it's older than a crash could plausibly still be
			// running, same rationale and threshold as fetcher-* below.
			if strings.HasPrefix(e.Name(), "staging-") || strings.HasPrefix(e.Name(), "bin-staging-") {
				info, ierr := e.Info()
				if ierr != nil || info.ModTime().After(cutoff) {
					continue // unknown or recent — conservative keep
				}
				p := filepath.Join(treesDir, e.Name())
				if err := os.RemoveAll(p); err != nil {
					g.log.Warn("gc: remove stale staging dir", "dir", p, "error", err)
					continue
				}
				trees++
				g.log.Debug("gc: stale staging dir removed", "dir", p)
				continue
			}
			k, perr := parseTreeKey(e.Name())
			if perr == nil {
				// TOCTOU: between Has returning false and the RemoveAll
				// below, a concurrent re-ingest of k could resurrect this
				// dir via stagedTree's os.Stat fast-path
				// (runner/importexec_linux.go:362) and then lose it
				// mid-use. Bounded the same way as the rest of this
				// package: "wasteful, never wrong" — the job that hit the
				// race just fails and retries — against a microseconds-wide
				// window on an hourly sweep.
				has, herr := g.store.Has(k)
				if herr != nil || has {
					continue // present, or unknown — keep (conservative)
				}
			}
			// Dead key or unparseable name: the materialization is orphaned.
			p := filepath.Join(treesDir, e.Name())
			if err := os.RemoveAll(p); err != nil {
				g.log.Warn("gc: remove orphaned tree", "dir", p, "error", err)
				continue
			}
			trees++
			g.log.Debug("gc: orphaned tree removed", "dir", p)
		}
	}
	if entries, err := os.ReadDir(g.cacheDir); err == nil {
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), "fetcher-") {
				continue
			}
			info, ierr := e.Info()
			if ierr != nil || info.ModTime().After(cutoff) {
				continue
			}
			p := filepath.Join(g.cacheDir, e.Name())
			if err := os.RemoveAll(p); err != nil {
				g.log.Warn("gc: remove stale fetcher dir", "dir", p, "error", err)
				continue
			}
			fetchers++
		}
	}
	return trees, fetchers
}
