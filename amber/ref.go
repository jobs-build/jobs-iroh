package amber

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/core/refstore"
)

// RefInfo is one reference: a name pointing at a store key, with its
// creation time.
type RefInfo struct {
	Name      string
	Key       key.Key
	CreatedAt time.Time
}

// ErrRefNotFound is returned (errors.Is-able) by GetRef and DeleteRef for an
// absent name.
var ErrRefNotFound = errors.New("ref not found")

// PutRef stores the reference name → k, overwriting unconditionally
// (last-write-wins; the refstore has no compare-and-swap). The record is an
// UNSIGNED reference.Reference — Name, Key and CreatedAt only, with
// User/Signature/PublicKey empty, exactly what amber-store-iroh's own server
// writes — so records stay sync-protocol-compatible and round-trip
// reference.Decode's canonicality check. Names must satisfy
// reference.ValidateName (1–1024 bytes of UTF-8, no '@', no control chars).
func (s *Store) PutRef(ctx context.Context, name string, k key.Key) error {
	if err := reference.ValidateName(name); err != nil {
		return err
	}
	rec := reference.Reference{
		Name:      name,
		Key:       k[:],
		CreatedAt: time.Now().UnixNano(),
	}
	b, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("encode ref %q: %w", name, err)
	}
	if s.guard != nil {
		commit, abort, gerr := s.guard.PrepareRef(k)
		if gerr != nil {
			return fmt.Errorf("gc guard for ref %q: %w", name, gerr)
		}
		if err := s.refs.Put(name, b); err != nil {
			abort()
			return err
		}
		commit()
		return nil
	}
	return s.refs.Put(name, b)
}

// GetKey resolves a ref name to its key; an absent name is (zero, false, nil).
// A present-but-undecodable record is an error — corruption must not read as
// absence, or callers would re-run work that already completed.
func (s *Store) GetKey(ctx context.Context, name string) (key.Key, bool, error) {
	ri, err := s.GetRef(ctx, name)
	if errors.Is(err, ErrRefNotFound) {
		return key.Key{}, false, nil
	}
	if err != nil {
		return key.Key{}, false, err
	}
	return ri.Key, true, nil
}

// GetRef returns the reference stored under name, or an error satisfying
// errors.Is(err, ErrRefNotFound) when absent.
func (s *Store) GetRef(ctx context.Context, name string) (RefInfo, error) {
	b, err := s.refs.Get(name)
	if errors.Is(err, refstore.ErrNotFound) {
		return RefInfo{}, fmt.Errorf("%q: %w", name, ErrRefNotFound)
	}
	if err != nil {
		return RefInfo{}, err
	}
	ri, err := decodeRef(name, b)
	if err == nil && s.observe != nil {
		s.observe(name)
	}
	return ri, err
}

// ListRefs returns every reference in lexicographic name order. A record that
// fails to decode fails the listing (see GetKey on why decode errors are
// never soft).
func (s *Store) ListRefs(ctx context.Context) ([]RefInfo, error) {
	recs, err := s.refs.All()
	if err != nil {
		return nil, err
	}
	out := make([]RefInfo, 0, len(recs))
	for _, r := range recs {
		ri, err := decodeRef(r.Name, r.Data)
		if err != nil {
			return nil, err
		}
		out = append(out, ri)
	}
	return out, nil
}

// DeleteRef removes name, or returns an error satisfying
// errors.Is(err, ErrRefNotFound) when it does not exist.
func (s *Store) DeleteRef(ctx context.Context, name string) error {
	err := s.refs.Delete(name)
	if errors.Is(err, refstore.ErrNotFound) {
		return fmt.Errorf("%q: %w", name, ErrRefNotFound)
	}
	return err
}

// decodeRef parses and validates the record bytes stored under name.
// reference.Decode enforces canonical encoding, so anything another
// jobs-iroh component (or amber-store-iroh's server) wrote round-trips.
func decodeRef(name string, b []byte) (RefInfo, error) {
	rec, err := reference.Decode(b)
	if err != nil {
		return RefInfo{}, fmt.Errorf("ref %q: %w", name, err)
	}
	k, err := key.Parse(rec.Key)
	if err != nil {
		return RefInfo{}, fmt.Errorf("ref %q: key: %w", name, err)
	}
	return RefInfo{Name: rec.Name, Key: k, CreatedAt: time.Unix(0, rec.CreatedAt)}, nil
}
