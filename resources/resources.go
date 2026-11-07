// Package resources defines the CPU/RAM scheduling value type used by the
// engine (admission control), the runner (advertised capacity), and the wire
// protocol (resolved per-node requirement). Integer units keep it canonical-CBOR
// safe and float-free. See
// docs/superpowers/specs/2026-07-07-multi-job-runner-resource-scheduling-design.md.
package resources

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	MiB = 1 << 20
	GiB = 1 << 30
)

// Resources is a CPU (millicpu) + memory (bytes) requirement or capacity.
type Resources struct {
	CPUMilli int64 `cbor:"cpu_milli,omitempty"`
	MemBytes int64 `cbor:"mem_bytes,omitempty"`
}

// Kind strings (mirror state.Kind values; kept as plain strings so this package
// has no dependency on state — everything upstream depends on resources).
const (
	KindImport             = "import"
	KindBuildFrom          = "build-from"
	KindBuildPluginResolve = "build-plugin-resolve"
	KindBuildPin           = "build-pin"
	KindBuild              = "build"
	KindTest               = "test"
)

// Per-kind default requirements (multi-job-runner design). Imports and the three
// light build stages get a small floor; the heavy build run stage gets more.
var (
	DefaultImport     = Resources{CPUMilli: 200, MemBytes: 512 * MiB}
	DefaultLightBuild = Resources{CPUMilli: 200, MemBytes: 512 * MiB}
	DefaultBuild      = Resources{CPUMilli: 1000, MemBytes: 1 * GiB}
)

// DefaultFor returns the per-kind default requirement. An unknown kind falls
// back to the light-build default (the conservative small floor).
func DefaultFor(kind string) Resources {
	switch kind {
	case KindImport:
		return DefaultImport
	case KindBuild, KindTest:
		// A test run is a build run that keeps nothing: same heavy floor.
		return DefaultBuild
	case KindBuildFrom, KindBuildPluginResolve, KindBuildPin:
		return DefaultLightBuild
	default:
		return DefaultLightBuild
	}
}

// ParseCPU parses "200m" (millicpu), or "0.2"/"1"/"2" (cores) into millicpu.
func ParseCPU(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty cpu")
	}
	if strings.HasSuffix(s, "m") {
		v, err := strconv.ParseInt(strings.TrimSuffix(s, "m"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cpu %q: %w", s, err)
		}
		return v, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("cpu %q: %w", s, err)
	}
	return int64(math.Round(f * 1000)), nil
}

// memSuffixes maps a unit suffix to its byte multiplier. Binary (power-of-two)
// suffixes are checked before decimal so "Mi" is not mistaken for "M".
var memSuffixes = []struct {
	suffix string
	mult   int64
}{
	{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40},
	{"K", 1_000}, {"M", 1_000_000}, {"G", 1_000_000_000}, {"T", 1_000_000_000_000},
}

// ParseMem parses "512Mi", "1Gi", "10M", "1G", or a bare byte count.
func ParseMem(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty mem")
	}
	mult := int64(1)
	num := s
	for _, ms := range memSuffixes {
		if strings.HasSuffix(s, ms.suffix) {
			mult = ms.mult
			num = strings.TrimSuffix(s, ms.suffix)
			break
		}
	}
	v, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("mem %q: %w", s, err)
	}
	return v * mult, nil
}

// Max returns the element-wise maximum of r and o.
func (r Resources) Max(o Resources) Resources {
	if o.CPUMilli > r.CPUMilli {
		r.CPUMilli = o.CPUMilli
	}
	if o.MemBytes > r.MemBytes {
		r.MemBytes = o.MemBytes
	}
	return r
}

// Add returns the element-wise sum.
func (r Resources) Add(o Resources) Resources {
	return Resources{r.CPUMilli + o.CPUMilli, r.MemBytes + o.MemBytes}
}

// Sub returns the element-wise difference (may go negative — callers guard).
func (r Resources) Sub(o Resources) Resources {
	return Resources{r.CPUMilli - o.CPUMilli, r.MemBytes - o.MemBytes}
}

// Fits reports whether r fits within remaining in both dimensions.
func (r Resources) Fits(remaining Resources) bool {
	return r.CPUMilli <= remaining.CPUMilli && r.MemBytes <= remaining.MemBytes
}

// IsZero reports whether both dimensions are zero.
func (r Resources) IsZero() bool { return r.CPUMilli == 0 && r.MemBytes == 0 }

func (r Resources) String() string {
	return fmt.Sprintf("cpu=%dm mem=%dMi", r.CPUMilli, r.MemBytes/MiB)
}
