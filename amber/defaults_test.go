package amber

import (
	"testing"

	"github.com/amber-store/core/chunkers"
)

// The byte-chunker sizes are identity-critical: they must be the library
// defaults so content dedups across jobs-iroh, the amber CLI and every other
// core-based store.
func TestByteOptsAreTheLibraryDefaults(t *testing.T) {
	if defByteOpts.MinSize != chunkers.DefaultMinSize || defByteOpts.NormalSize != chunkers.DefaultNormalSize || defByteOpts.MaxSize != chunkers.DefaultMaxSize {
		t.Fatalf("defByteOpts = %d/%d/%d, want the library defaults %d/%d/%d",
			defByteOpts.MinSize, defByteOpts.NormalSize, defByteOpts.MaxSize,
			chunkers.DefaultMinSize, chunkers.DefaultNormalSize, chunkers.DefaultMaxSize)
	}
}
