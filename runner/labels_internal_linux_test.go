package runner

import (
	"testing"

	"github.com/amber-store/core/key"
)

func TestBuildLabelPrefersName(t *testing.T) {
	var k key.Key
	k[0] = 0x22
	if got := buildLabel(k, "apk_acl_libs"); got != "apk_acl_libs" {
		t.Fatalf("named: %q", got)
	}
	if got := buildLabel(k, ""); got != k.String() {
		t.Fatalf("unnamed fallback: %q", got)
	}
}
