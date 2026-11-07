package wire

import (
	"strings"
	"testing"

	"github.com/amber-store/core/key"
)

func TestParseNodeNameKinds(t *testing.T) {
	k, err := key.New(key.Blob, 3, []byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{KindImport, KindBuildFrom, KindPluginResolve, KindPin, KindBuildRun, KindBuildValue, KindTestRun, KindTestValue} {
		name := NodeName(kind, k)
		gotKind, gotKey, err := ParseNodeName(name)
		if err != nil {
			t.Fatalf("ParseNodeName(%s): %v", name, err)
		}
		if gotKind != kind || gotKey != k {
			t.Fatalf("ParseNodeName(%s) = %s/%s", name, gotKind, gotKey)
		}
	}
	// Unknown kinds and non-canonical hex fail closed.
	if _, _, err := ParseNodeName("bogus_" + k.String()); err == nil {
		t.Fatal("unknown kind accepted")
	}
	if _, _, err := ParseNodeName(KindTestRun + "_" + strings.ToUpper(k.String())); err == nil {
		t.Fatal("non-canonical hex accepted")
	}
}
