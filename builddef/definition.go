// Package builddef defines the JOBS build job definition and the unifying Input
// type, with their canonical CBOR encoding. The amber file key of a canonical
// definition is the job identity K (architecture/build.md §2, §4). An Input
// embeds the COMPLETE inner definition (import or build), so a build's K
// content-addresses its entire transitive input set.
package builddef

import (
	"fmt"
	"reflect"

	"github.com/amber-store/core/key"
	"github.com/fxamacker/cbor/v2"
	"github.com/jobs-build/jobs-iroh/amber"
)

// Input kinds (build.md §4).
const (
	KindImport = "import"
	KindBuild  = "build"
)

// CtxWidened marks a definition with widened-context semantics
// (sibling-sources design §3.2): F captures the WHOLE source context (env/ =
// context root) and carries dir as an F-tree entry, instead of narrowing env/
// to the dir subtree. Absent (0) is the legacy dir-narrowed encoding. Every
// def constructor sets CtxWidened whenever Dir != ""; root builds omit it and
// keep their pre-field K. Drivers must reject values they do not implement
// via ValidateCtx — fxamacker's default decode silently drops unknown fields,
// so an unrecognized ctx reaching an old driver would otherwise be built with
// the WRONG (narrow) semantics under the new K.
const CtxWidened = 2

// canonEnc encodes deterministically (sorted map keys, shortest ints) so equal
// values produce equal bytes — the property that makes K a content address.
var canonEnc = func() cbor.EncMode {
	m, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		panic(err)
	}
	return m
}()

// stringMapDec decodes CBOR maps with string keys so params/specs surface to
// Starlark and JSON as map[string]any.
var stringMapDec = func() cbor.DecMode {
	m, err := cbor.DecOptions{DefaultMapType: reflect.TypeOf(map[string]any(nil))}.DecMode()
	if err != nil {
		panic(err)
	}
	return m
}()

// Input is a complete import or build definition, tagged by kind (build.md §4).
// Definition holds the canonical CBOR of the inner import-def or build-def.
type Input struct {
	Kind       string          `cbor:"kind"`
	Definition cbor.RawMessage `cbor:"definition"`
}

// Key derives the input's identity K = amber.FileKey(definition) (build.md §4).
func (in Input) Key() (key.Key, error) {
	return amber.FileKey(in.Definition)
}

// Definition is the build job definition (build.md §2; superseded by the
// build-from design §5). Its canonical CBOR is content-hashed to produce the
// platform-SPECIFIC job identity K.
type Definition struct {
	Source    Input           `cbor:"source"`
	Dir       string          `cbor:"dir,omitempty"`
	Platform  string          `cbor:"platform"`
	Params    cbor.RawMessage `cbor:"params"`
	BuildJobs []byte          `cbor:"buildJobs,omitempty"` // optional override recipe (design §6)
	BuildFile string          `cbor:"buildFile,omitempty"` // optional recipe path relative to Dir (default BUILD.jobs)
	Ctx       int             `cbor:"ctx,omitempty"`       // context-semantics version (sibling-sources design §3.2)
}

// Canonical returns the canonical CBOR of the definition. BuildJobs is an
// optional override recipe and BuildFile an optional alternative recipe path
// (relative to Dir); both are omitted when empty (omitempty) so a build that
// uses neither encodes identically to before these fields existed. The
// explicit field copy is load-bearing beyond style: the submit path's
// canonicality check re-encodes through this function and byte-compares, so a
// field missing here strips silently and rejects every def that carries it —
// every new Definition field MUST be added to this copy.
func (d Definition) Canonical() ([]byte, error) {
	out := Definition{
		Source:    d.Source,
		Dir:       d.Dir,
		Platform:  d.Platform,
		Params:    d.Params,
		BuildJobs: d.BuildJobs,
		BuildFile: d.BuildFile,
		Ctx:       d.Ctx,
	}
	return canonEnc.Marshal(out)
}

// ValidateCtx rejects context-semantics versions this binary does not
// implement. Drivers (buildfrom, pin, local pipeline) call it right after
// decoding a definition: building a newer-ctx def with older semantics would
// silently produce a wrong F under the def's K (sibling-sources design §3.2).
func ValidateCtx(ctx int) error {
	switch ctx {
	case 0, CtxWidened:
		return nil
	}
	return fmt.Errorf("definition ctx %d is not supported by this binary (upgrade jobs)", ctx)
}

// Key derives the build's identity K from its canonical CBOR (build.md §2).
func (d Definition) Key() (key.Key, error) {
	canon, err := d.Canonical()
	if err != nil {
		return key.Key{}, err
	}
	return amber.FileKey(canon)
}

// DecodeDefinition parses canonical CBOR back into a Definition.
func DecodeDefinition(b []byte) (Definition, error) {
	var d Definition
	if err := cbor.Unmarshal(b, &d); err != nil {
		return Definition{}, err
	}
	return d, nil
}

// ParamsValue decodes the params CBOR to a string-keyed Go value, for surfacing
// to the recipe as the `params` Starlark global (build.md §3.1).
func (d Definition) ParamsValue() (any, error) {
	if len(d.Params) == 0 {
		return nil, nil
	}
	var v any
	if err := stringMapDec.Unmarshal(d.Params, &v); err != nil {
		return nil, err
	}
	return v, nil
}
