package registryd

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/amber-store/transport-iroh/amberiroh"
)

func TestPinAsserterCoalesces(t *testing.T) {
	p := newPinAsserter()
	now := time.Now()

	due := p.due([]string{"a", "b"}, now)
	slices.Sort(due)
	if !slices.Equal(due, []string{"a", "b"}) {
		t.Fatalf("first due = %v", due)
	}
	if got := p.due([]string{"a", "b"}, now.Add(time.Minute)); len(got) != 0 {
		t.Fatalf("within the hour must coalesce, got %v", got)
	}
	if got := p.due([]string{"a"}, now.Add(2*time.Hour)); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("after an hour must re-assert, got %v", got)
	}
}

func TestPinAsserterRetryAndDisable(t *testing.T) {
	p := newPinAsserter()
	now := time.Now()
	_ = p.due([]string{"a"}, now)

	// A transport failure re-arms the names for the next serve.
	p.retry([]string{"a"})
	if got := p.due([]string{"a"}, now.Add(time.Second)); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("retry must re-arm, got %v", got)
	}

	// An old server disables asserting for good.
	p.disable()
	if got := p.due([]string{"a", "b"}, now.Add(3*time.Hour)); len(got) != 0 {
		t.Fatalf("disabled asserter returned %v", got)
	}
}

func TestShouldDisable(t *testing.T) {
	// The old-server signal (unrecognized TPin op): disable.
	if !shouldDisable(&amberiroh.RemoteError{Code: amberiroh.CodeBadRequest, Text: "unknown operation"}) {
		t.Fatalf("bad-request RemoteError must disable")
	}
	// Any other RemoteError code (e.g. a transient server-side store
	// error) must re-arm via retry, not disable permanently.
	if shouldDisable(&amberiroh.RemoteError{Code: amberiroh.CodeInternal, Text: "store error"}) {
		t.Fatalf("non-bad-request RemoteError must not disable")
	}
	// A non-RemoteError (e.g. a transport failure) must not disable.
	if shouldDisable(errors.New("dial timeout")) {
		t.Fatalf("non-RemoteError must not disable")
	}
	// Wrapped RemoteError (as amberclient.Pin actually returns it, via
	// fmt.Errorf("...: %w", ...)) must still be detected through errors.As.
	wrapped := fmt.Errorf("amberclient: pin: %w", &amberiroh.RemoteError{Code: amberiroh.CodeBadRequest})
	if !shouldDisable(wrapped) {
		t.Fatalf("wrapped bad-request RemoteError must disable")
	}
}

func TestPinNames(t *testing.T) {
	rec := imageRecord{K: "aa", F: "bb", Platform: "linux/amd64"}
	want := []string{"build-from:aa", "build:aa", "build-output:bb",
		"build-output-deps:bb", "shell:linux/amd64"}
	got := pinNames(rec)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("pinNames = %v, want %v", got, want)
	}
}
