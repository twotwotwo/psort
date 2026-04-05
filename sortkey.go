package psort

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"runtime"
	"slices"
	"strings"
)

// SortKey sorts x using a key function, distributing work across
// available CPUs. The sort is not stable.
//
// For string keys, SortKey uses an abbreviated-key optimization: it
// packs the leading bytes of each key into a uint64 for fast
// comparison, falling back to full key comparison on ties. In
// benchmarks with random 20-byte strings, this is roughly 1.5–2x
// faster than [Sort] at 1M–10M elements.
//
// The optimization allocates a temporary array of approximately
// (8 + sizeof(element)) bytes per element. For a []string of 1M
// items, this is about 25 MB. Use [SortFunc] to avoid the allocation.
//
// For non-string ordered keys (int, float, etc.), SortKey delegates
// to [SortFunc] with no extra allocation, since those types are
// already cheap to compare.
//
// The key function should be cheap to call (e.g., returning a struct
// field), as it may be called more than once per element during
// sorting. Key functions that allocate or do expensive computation
// will hurt performance.
//
// Elements with equal keys may appear in any order.
func SortKey[S ~[]E, E any, K cmp.Ordered](x S, key func(E) K) {
	// Type-switch on K to detect string keys for abbreviated-key path.
	var zeroK K
	switch any(zeroK).(type) {
	case string:
		sortKeyString(x, any(key).(func(E) string), nil)
		return
	}

	// For all other ordered types (int, float, etc.), the key itself
	// is cheap to compare — no abbreviated-key trick needed.
	SortFunc(x, sortKeyCmp(key, nil))
}

// sortKeyThen is like SortKey but calls tiebreaker to order
// elements whose keys are equal.
func sortKeyThen[S ~[]E, E any, K cmp.Ordered](x S, key func(E) K, tiebreaker func(a, b E) int) {
	var zeroK K
	switch any(zeroK).(type) {
	case string:
		sortKeyString(x, any(key).(func(E) string), tiebreaker)
		return
	}

	SortFunc(x, sortKeyCmp(key, tiebreaker))
}

// SortKeyBytes sorts x using a []byte key function, distributing work
// across available CPUs. The sort is not stable.
//
// Like [SortKey] with string keys, this uses an abbreviated-key
// optimization and requires a temporary allocation proportional to
// len(x). See [SortKey] for performance characteristics and caveats.
//
// The key function should be cheap to call (e.g., returning a struct
// field or sub-slice), as it may be called more than once per element
// during sorting. Key functions that allocate (e.g., via append or
// copying) will hurt performance.
//
// Elements with equal keys may appear in any order.
func SortKeyBytes[S ~[]E, E any](x S, key func(E) []byte) {
	sortKeyBytesImpl(x, key, nil)
}

// sortKeyBytesThen is like SortKeyBytes but calls tiebreaker to
// order elements whose keys are equal.
func sortKeyBytesThen[S ~[]E, E any](x S, key func(E) []byte, tiebreaker func(a, b E) int) {
	sortKeyBytesImpl(x, key, tiebreaker)
}

// SortBytes sorts a [][]byte slice in lexicographic order, distributing
// work across available CPUs. The sort is not stable.
//
// Like [SortKey] with string keys, this uses an abbreviated-key
// optimization and requires a temporary allocation proportional to
// len(x). See [SortKey] for performance characteristics and caveats.
func SortBytes[S ~[]E, E ~[]byte](x S) {
	sortKeyBytesImpl(x, func(e E) []byte { return []byte(e) }, nil)
}

// sortKeyCmp builds a comparison function from a key function and
// optional tiebreaker.
func sortKeyCmp[E any, K cmp.Ordered](key func(E) K, tiebreaker func(a, b E) int) func(a, b E) int {
	if tiebreaker != nil {
		return func(a, b E) int {
			if d := cmp.Compare(key(a), key(b)); d != 0 {
				return d
			}
			return tiebreaker(a, b)
		}
	}
	return func(a, b E) int {
		return cmp.Compare(key(a), key(b))
	}
}

// --- Abbreviated-key internals ---

// abbrevElem holds an abbreviated key alongside the original element.
// The key function is called again on tie rather than caching the full
// key, keeping the struct small and swap-friendly.
type abbrevElem[E any] struct {
	abbrev uint64
	elem   E
}

// abbreviateString packs up to the first 8 bytes of s into a uint64
// in big-endian order, so that uint64 comparison matches lexicographic
// string comparison for any prefix.
func abbreviateString(s string) uint64 {
	if len(s) >= 8 {
		return binary.BigEndian.Uint64([]byte(s[:8]))
	}
	var buf [8]byte
	copy(buf[:], s)
	return binary.BigEndian.Uint64(buf[:])
}

// abbreviateBytes is the same for []byte.
func abbreviateBytes(b []byte) uint64 {
	if len(b) >= 8 {
		return binary.BigEndian.Uint64(b[:8])
	}
	var buf [8]byte
	copy(buf[:], b)
	return binary.BigEndian.Uint64(buf[:])
}

// --- String key path ---

func sortKeyString[S ~[]E, E any](x S, key func(E) string, tiebreaker func(a, b E) int) {
	n := len(x)

	keyed := make([]abbrevElem[E], n)
	for i := range x {
		keyed[i] = abbrevElem[E]{
			abbrev: abbreviateString(key(x[i])),
			elem:   x[i],
		}
	}

	var cmpFn func(a, b abbrevElem[E]) int
	if tiebreaker != nil {
		cmpFn = func(a, b abbrevElem[E]) int {
			if d := cmp.Compare(a.abbrev, b.abbrev); d != 0 {
				return d
			}
			if d := strings.Compare(key(a.elem), key(b.elem)); d != 0 {
				return d
			}
			return tiebreaker(a.elem, b.elem)
		}
	} else {
		cmpFn = func(a, b abbrevElem[E]) int {
			if d := cmp.Compare(a.abbrev, b.abbrev); d != 0 {
				return d
			}
			return strings.Compare(key(a.elem), key(b.elem))
		}
	}

	sortAbbrev(keyed, cmpFn)

	for i := range keyed {
		x[i] = keyed[i].elem
	}
}

// --- []byte key path ---

func sortKeyBytesImpl[S ~[]E, E any](x S, key func(E) []byte, tiebreaker func(a, b E) int) {
	n := len(x)

	keyed := make([]abbrevElem[E], n)
	for i := range x {
		keyed[i] = abbrevElem[E]{
			abbrev: abbreviateBytes(key(x[i])),
			elem:   x[i],
		}
	}

	var cmpFn func(a, b abbrevElem[E]) int
	if tiebreaker != nil {
		cmpFn = func(a, b abbrevElem[E]) int {
			if d := cmp.Compare(a.abbrev, b.abbrev); d != 0 {
				return d
			}
			if d := bytes.Compare(key(a.elem), key(b.elem)); d != 0 {
				return d
			}
			return tiebreaker(a.elem, b.elem)
		}
	} else {
		cmpFn = func(a, b abbrevElem[E]) int {
			if d := cmp.Compare(a.abbrev, b.abbrev); d != 0 {
				return d
			}
			return bytes.Compare(key(a.elem), key(b.elem))
		}
	}

	sortAbbrev(keyed, cmpFn)

	for i := range keyed {
		x[i] = keyed[i].elem
	}
}

// --- In-place MSD radix sort on abbreviated keys ---

// radixSortThreshold is the bucket size at which MSD radix sort stops
// recursing and falls back to slices.SortFunc (pdqsort).
var radixSortThreshold = 128

// sortAbbrev sorts keyed using parallel partitioning followed by
// per-partition MSD radix sort on the abbrev field.
func sortAbbrev[E any](keyed []abbrevElem[E], cmpFn func(a, b abbrevElem[E]) int) {
	if len(keyed) < minParallel {
		radixSortAbbrev(keyed, 0, cmpFn)
		return
	}

	nproc := runtime.GOMAXPROCS(0)
	parts, sorted := partitionCmpFunc(keyed, nproc, cmpFn)
	if sorted {
		return
	}
	sortPartitions(parts, nproc, func(lo, hi int) {
		radixSortAbbrev(keyed[lo:hi], 0, cmpFn)
	})
}

// radixSortAbbrev performs an in-place MSD radix sort on data, keyed
// by byte byteIdx (0 = MSB) of the abbrev field. When a bucket is
// small enough or all 8 abbrev bytes are exhausted, it falls back to
// cmpFn (which does a full key comparison for ties).
func radixSortAbbrev[E any](data []abbrevElem[E], byteIdx int, cmpFn func(a, b abbrevElem[E]) int) {
	if len(data) <= radixSortThreshold || byteIdx >= 8 {
		slices.SortFunc(data, cmpFn)
		return
	}

	shift := uint(56 - 8*byteIdx)

	// Count occurrences of each byte value.
	var count [256]int
	for i := range data {
		count[uint8(data[i].abbrev>>shift)]++
	}

	// Prefix sum to get bucket start positions.
	var bucketStart [256]int
	var offset [256]int
	pos := 0
	for b := 0; b < 256; b++ {
		bucketStart[b] = pos
		offset[b] = pos
		pos += count[b]
	}

	// In-place permutation: for each bucket, swap elements into their
	// target bucket until every position holds the right byte value.
	for b := 0; b < 256; b++ {
		end := bucketStart[b] + count[b]
		for offset[b] < end {
			// Looping over the bucket looks like more work, but avoids
			// a dependency between one swap and the next. The CPU can
			// ask for the item it will swap bucket[1] with even before
			// it knows what the new bucket[0] is after the previous
			// swap.
			for i := offset[b]; i < end; i++ {
				target := int(uint8(data[i].abbrev >> shift))
				data[i], data[offset[target]] = data[offset[target]], data[i]
				offset[target]++  // target may be b!
			}
		}
	}

	// Recurse into each bucket with more than one element.
	for b := 0; b < 256; b++ {
		if count[b] > 1 {
			radixSortAbbrev(data[bucketStart[b]:bucketStart[b]+count[b]], byteIdx+1, cmpFn)
		}
	}
}
