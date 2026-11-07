//go:build !linux

package runner

import (
	"context"

	"github.com/jobs-build/jobs-iroh/amber"
)

// DevelopConfig configures a `jobs develop` / local `jobs run --source` run
// (see the Linux implementation). Declared off Linux too so the cross-platform
// run/develop entry points share one config type; the interactive develop
// shell itself (RunDevelop) is Linux-only.
type DevelopConfig struct {
	SourceDir string
	Dir       string
	BuildFile string
	Platform  string
	Params    []byte
	ShellRef  string
	CacheDir  string
	Secrets   map[string]TagSecret
	Test      bool
}

// RunDevelop is unsupported off Linux (the hermetic sandbox is Linux-only).
func RunDevelop(ctx context.Context, st *amber.Store, cfg DevelopConfig) error {
	return ErrUnsupported
}
