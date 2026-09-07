package builddef

import (
	"fmt"

	"github.com/amber-store/core/key"
	"github.com/fxamacker/cbor/v2"
)

// KindTree is an Input whose value is a fixed, already-present amber content
// tree, addressed by its content key. Unlike import/build it names no work: the
// content already exists. It is constructed only by the recipe subbuild()
// builtin (sub-builds of a build's own descendant directories) and resolved
// directly by the build-from stage. See
// docs/superpowers/specs/2026-06-25-subbuild-descendant-inputs-design.md.
const KindTree = "tree"

// treeRef is the canonical-CBOR body of a tree Input's definition: a single
// content key. Its amber file key is the tree Input's identity K.
type treeRef struct {
	Key []byte `cbor:"key"`
}

// TreeInput constructs a tree Input referencing the content tree at k.
func TreeInput(k key.Key) (Input, error) {
	body, err := canonEnc.Marshal(treeRef{Key: k[:]})
	if err != nil {
		return Input{}, fmt.Errorf("encode tree ref: %w", err)
	}
	return Input{Kind: KindTree, Definition: body}, nil
}

// DecodeTreeKey recovers the content key from a tree Input's definition.
func DecodeTreeKey(def []byte) (key.Key, error) {
	var tr treeRef
	if err := cbor.Unmarshal(def, &tr); err != nil {
		return key.Key{}, fmt.Errorf("decode tree ref: %w", err)
	}
	return key.Parse(tr.Key)
}
