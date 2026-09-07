package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
	"github.com/jobs-build/jobs-iroh/builddef"
	"github.com/jobs-build/jobs-iroh/events"
	"github.com/jobs-build/jobs-iroh/importdef"
)

// exTempFail (EX_TEMPFAIL) signals a retryable fetch (import.md §3.3).
const exTempFail = 75

// classControl marks a CONTROL-PLANE verdict (jobs' state.ClassControl): a
// refs-ack timeout says nothing about the job itself, so it must not spend
// the job's retryable budget — the scheduler gives it its own, far more
// generous consecutive cap (issue #153).
const classControl = "control"

// TagSecret is a runner-held, host-scoped credential for a tag (import.md §4).
type TagSecret struct {
	Scope  string          `json:"scope"`
	Secret json.RawMessage `json:"secret"`
}

// Outcome is the result of running an import — what to report to the scheduler.
type Outcome struct {
	OutputKey     key.Key
	Decline       bool
	DeclineReason string
	Cancelled     bool
	Failed        bool
	Class         string // "hard" | "retryable" | classControl
	ExitCode      int
	Phase         string
	Stderr        string
}

func retryable(phase string, err error) Outcome {
	return Outcome{Failed: true, Class: "retryable", Phase: phase, Stderr: err.Error()}
}

// pushOutcome classifies a WriteRefs error (the shared tail of every stage
// driver's publish step). Losing a duplicate-execution race — the server
// declining the batch as "not assigned" — is not a failure: the node was
// re-placed (or already completed) elsewhere and this job is superseded, so
// its outcome is silence, exactly like a cancel (issue #152). Everything else
// stays a retryable push failure.
func pushOutcome(phase string, err error) Outcome {
	if errors.Is(err, errRefsNotAssigned) {
		return Outcome{Cancelled: true}
	}
	// An ack timeout is a CONTROL-PLANE verdict (congested server, bad link):
	// it says nothing about the job, so it must not spend the job's retryable
	// budget — classControl has its own generous cap (issue #153).
	if errors.Is(err, errRefsAckTimeout) {
		return Outcome{Failed: true, Class: classControl, Phase: phase, Stderr: err.Error()}
	}
	return retryable(phase, err)
}

func hard(phase, msg string, exit int) Outcome {
	return Outcome{Failed: true, Class: "hard", Phase: phase, Stderr: msg, ExitCode: exit}
}

// ensureImportDef verifies the import:K bookkeeping ref is readable before the
// raw content read of the definition. Single-store world: there is no closure
// pull (jobs' RemoteSync ReadKey) — the def objects either are local or the
// read below fails retryably — but a ref-store error here must still classify
// as a retryable "pulling" failure, exactly like jobs' transport errors.
// The ref being absent is NOT an error (K's objects may be present without
// the bookkeeping ref, e.g. on the local paths).
func ensureImportDef(ctx context.Context, st *amber.Store, k key.Key) *Outcome {
	if _, _, err := st.GetKey(ctx, "import:"+k.String()); err != nil {
		o := retryable("pulling import def", err)
		return &o
	}
	return nil
}

// RunImport executes one import job (import.md §5): pull the definition, resolve
// and run the fetcher, ingest the output, publish the import-output ref.
// Infrastructure problems are retryable; a fetch failure is hard (exit 75 is
// retryable); a missing named fetcher is hard (nothing provisions those
// anymore). ev (nil-safe) receives exec.phase transitions and the fetcher's
// output (build-events §9).
func RunImport(ctx context.Context, st *amber.Store, rw RefWriter, ex Executor, cacheDir string, k key.Key, secrets map[string]TagSecret, ev *events.Job) Outcome {
	// 1. pull the definition.
	ev.Phase("pulling")
	if o := ensureImportDef(ctx, st, k); o != nil {
		return *o
	}
	rc, err := st.File(ctx, k)
	if err != nil {
		return retryable("pulling", err)
	}
	defBytes, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return retryable("pulling", err)
	}
	def, err := importdef.Decode(defBytes)
	if err != nil {
		return hard("pulling", "decode definition: "+err.Error(), 0)
	}

	// 2. resolve the fetcher (cleanup releases it once the fetcher has run).
	// A def carrying FetcherDef (a recipe-declared fetcher) resolves the fetcher
	// build's c/ artifact by content — the scheduler gated this import on that
	// build, so a miss here is a sync race, not a scheduling error (design §6).
	// A NAMED fetcher resolves fetcher:<name>:<platform>; def.Platform selects
	// it — since the import-platform-pinning design every new def carries the
	// build's platform. Empty is legacy-read only (defs frozen in pre-change
	// Pinned trees): resolve for this runner's own platform, exactly as before.
	ev.Phase("resolving")
	platform := def.Platform
	if platform == "" {
		platform = Platform()
	}
	var fdir string
	var cleanup func()
	if len(def.FetcherDef) > 0 {
		fk, kerr := (builddef.Input{Kind: builddef.KindBuild, Definition: def.FetcherDef}).Key()
		if kerr != nil {
			return hard("resolving", "fetcher build def key: "+kerr.Error(), 0)
		}
		ak, ok, aerr := st.ResolveBuildArtifact(ctx, fk)
		if aerr != nil {
			if errors.Is(aerr, amber.ErrNoArtifact) {
				// The fetcher build produced an output with no c/ — malformed
				// forever for this output; retrying cannot fix it.
				return hard("resolving", aerr.Error(), 0)
			}
			return retryable("resolving", aerr)
		}
		if !ok {
			return retryable("resolving", fmt.Errorf("fetcher build %s output not resolvable yet", fk))
		}
		fdir, cleanup, err = ResolveFetcherArtifact(ctx, st, cacheDir, ak, def.Fetcher)
		if err != nil {
			return retryable("resolving", err)
		}
	} else {
		var found bool
		fdir, cleanup, found, err = ResolveFetcher(ctx, st, cacheDir, def.Fetcher, platform)
		if err != nil {
			return retryable("resolving", err)
		}
		if !found {
			// Nothing will ever provision a missing named fetcher (seeds are
			// self-seeded; the provisioning machinery is gone) — fail, don't
			// decline (recipe-declared-fetchers design §5).
			return hard("resolving", "no fetcher "+def.Fetcher+" for "+platform+" (not a seed; recipe-declared fetchers carry their build)", 0)
		}
	}
	defer cleanup()

	// The hermetic executor's root: the shell artifact (the static userland
	// every fetch script runs under) and, for a recipe-declared fetcher, its
	// build's runtime closure — mounted at /jobs/store/<key> like a build's
	// deps. Both are pulled ahead by the scheduler (PullRefs); the Subprocess
	// executor ignores them. A missing shell is a sync race, not a fatal
	// condition here: the executor reports it and the import retries.
	shellKey, _, err := st.GetKey(ctx, "shell:"+platform)
	if err != nil {
		return retryable("resolving", err)
	}
	var closureKey key.Key
	if len(def.FetcherDef) > 0 {
		fk, _ := (builddef.Input{Kind: builddef.KindBuild, Definition: def.FetcherDef}).Key()
		ck, ok, cerr := st.ResolveBuildOutputDeps(ctx, fk)
		if cerr != nil {
			return retryable("resolving", cerr)
		}
		if !ok {
			// Local build: no build-from:K bridge — the same direct fallback
			// ResolveBuildArtifact applies to build-output above. Without it a
			// locally driven fetcher build's runtime closure (e.g. gomod's
			// toolchain, baked into env.sh as /jobs/store paths) never mounts.
			ck, ok, cerr = st.GetKey(ctx, "build-output-deps:"+fk.String())
			if cerr != nil {
				return retryable("resolving", cerr)
			}
		}
		if ok {
			closureKey = ck
		}
	}

	// 3. prepare a network-capable sandbox
	ev.Phase("fetching")
	work, err := os.MkdirTemp("", "jobs-import")
	if err != nil {
		return retryable("fetching", err)
	}
	defer os.RemoveAll(work)
	outDir := filepath.Join(work, "output")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return retryable("fetching", err)
	}
	paramsJSON, err := def.ParamsJSON()
	if err != nil {
		return hard("fetching", "render params: "+err.Error(), 0)
	}
	env := map[string]string{
		"JOBS_FETCH_PARAMS": string(paramsJSON),
		"JOBS_OUTPUT_DIR":   outDir,
	}
	spec := ExecSpec{FetcherDir: fdir, OutputDir: outDir, Env: env, Events: ev,
		Node:  "import|" + k.String(),
		Store: st, ShellKey: shellKey, ClosureKey: closureKey, CacheDir: cacheDir}
	if len(def.RequiredTags) > 0 {
		secFile := filepath.Join(work, "secrets.json")
		if err := writeSecrets(secFile, def.RequiredTags, secrets); err != nil {
			return retryable("fetching", err)
		}
		env["JOBS_SECRETS_FILE"] = secFile
		spec.SecretsFile = secFile
	}

	// 4. run the fetcher, capturing its full output as events (build-events §9)
	if ow := ev.Output("stdout", "fetching"); ow != nil {
		spec.StdoutSink = ow
		defer ow.Close()
	}
	if ow := ev.Output("stderr", "fetching"); ow != nil {
		spec.StderrSink = ow
		defer ow.Close()
	}
	res, err := ex.Run(ctx, spec)
	if err != nil {
		if ctx.Err() != nil {
			return Outcome{Cancelled: true}
		}
		return retryable("fetching", err)
	}
	switch {
	case res.ExitCode == 0:
		// success
	case res.ExitCode == exTempFail:
		return Outcome{Failed: true, Class: "retryable", ExitCode: res.ExitCode, Phase: "fetching", Stderr: res.StderrTail}
	default:
		return Outcome{Failed: true, Class: "hard", ExitCode: res.ExitCode, Phase: "fetching", Stderr: res.StderrTail}
	}

	// 5. ingest the output tree (honoring any .amberignore the fetcher emitted)
	ev.Phase("ingesting")
	r, ing, err := st.IngestSourceDirStats(ctx, outDir)
	if err != nil {
		return retryable("ingesting", err)
	}

	// 6. publish the result ref (objects already present from ingest)
	ev.Phase("pushing")
	pt, err := rw.WriteRefs(ctx, []Ref{{Name: "import-output:" + k.String(), Key: r}})
	if err != nil {
		return pushOutcome("pushing", err)
	}
	emitCAS(ev, ing, pt)
	return Outcome{OutputKey: r}
}

// writeSecrets assembles the tag→{scope,secret} JSON for the job's required tags
// and writes it 0600 (import.md §4). Secrets never enter CAS or argv/env.
func writeSecrets(path string, required []string, secrets map[string]TagSecret) error {
	m := make(map[string]TagSecret, len(required))
	for _, tag := range required {
		s, ok := secrets[tag]
		if !ok {
			return fmt.Errorf("runner missing secret for required tag %q", tag)
		}
		m[tag] = s
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}
