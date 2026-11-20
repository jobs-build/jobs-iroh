package amber

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

// RefPage is one page of a prefix-filtered ref listing (RefsPage).
type RefPage struct {
	Refs    []RefInfo // in name order, at most the requested limit
	More    bool      // matching refs remain past the last one in Refs
	Matched int       // every ref matching the prefix, across all pages
}

// RefsPage lists the refs whose name starts with prefix and sorts strictly
// after after, in name order, at most limit of them (limit ≤ 0 = no cap).
// Only the returned page's records are decoded, so a page of a store with
// hundreds of thousands of refs costs one key scan, not a full decode —
// amber-store-core's refstore has no ranged iterator yet, so the scan
// itself still walks every name.
func (s *Store) RefsPage(ctx context.Context, prefix, after string, limit int) (RefPage, error) {
	recs, err := s.refs.All()
	if err != nil {
		return RefPage{}, err
	}
	var page RefPage
	for _, r := range recs {
		if !strings.HasPrefix(r.Name, prefix) {
			continue
		}
		page.Matched++
		if after != "" && r.Name <= after {
			continue
		}
		if limit > 0 && len(page.Refs) == limit {
			page.More = true
			continue // keep counting Matched
		}
		ri, err := decodeRef(r.Name, r.Data)
		if err != nil {
			return RefPage{}, err
		}
		page.Refs = append(page.Refs, ri)
	}
	return page, nil
}

// RefNames returns every ref name in lexicographic order without decoding
// the records — what reconciliation needs.
func (s *Store) RefNames(ctx context.Context) ([]string, error) {
	recs, err := s.refs.All()
	if err != nil {
		return nil, err
	}
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.Name
	}
	return out, nil
}

// CountRefs reports how many refs the store holds, without decoding them.
func (s *Store) CountRefs(ctx context.Context) (int, error) {
	recs, err := s.refs.All()
	return len(recs), err
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
