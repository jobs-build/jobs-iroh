//go:build !linux

package runner

import (
	"context"
	"io"

	"github.com/amber-store/core/key"
	"github.com/jobs-build/jobs-iroh/amber"
)

// RunIO wires the run child's standard streams (see the Linux implementation).
type RunIO struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// BuildFromSource is unsupported off Linux (the hermetic build sandbox is
// Linux-only).
func BuildFromSource(ctx context.Context, st *amber.Store, cfg DevelopConfig, p *Progress) (key.Key, error) {
	return key.Key{}, ErrUnsupported
}

// RunFromSource is unsupported off Linux (the run sandbox is Linux-only).
func RunFromSource(ctx context.Context, st *amber.Store, cfg DevelopConfig, extraArgs []string, rio RunIO, p *Progress) (int, error) {
	return -1, ErrUnsupported
}

// RunByKey is unsupported off Linux (the run sandbox is Linux-only).
func RunByKey(ctx context.Context, st *amber.Store, k key.Key, platform, shellRef string, extraArgs []string, rio RunIO) (int, error) {
	return -1, ErrUnsupported
}
