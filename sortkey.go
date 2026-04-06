package psort

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"runtime"
	"slices"
	"strings"
	"sync"
)

// SortKey sorts x using a key function, distributing work across
// available CPUs. The sort is not stable.
//
// For string keys, SortKey uses an abbreviated-key optimization: it
// packs the leading bytes of each key into a uint64, sorts a parallel
// []uint64 alongside the input (swapping items in place), and falls
// back to full key comparison on ties. This is the same optimization
// that [Sort] uses for []string. In benchmarks with random 20-byte
// strings, it is roughly 1.5–3x faster than [SortFunc] or
// [SortInPlace] at 1M–10M elements.
//
// The optimization allocates a temporary []uint64 of 8 bytes per
// element. For a []string of 1M items, this is about 8 MB. Use
// [SortFunc] to avoid the allocation.
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
// optimization and requires a temporary []uint64 allocation of 8
// bytes per element. See [SortKey] for performance characteristics
// and caveats.
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
// optimization and requires a temporary []uint64 allocation of 8
// bytes per element. See [SortKey] for performance characteristics
// and caveats.
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

// abbrevPool recycles the []uint64 scratch buffer used to hold
// abbreviated keys. External-sort-style workloads run many sorts
// back-to-back, and this keeps a warm buffer alive between them
// instead of allocating (and garbage-collecting) fresh ones.
//
// Caveat: sync.Pool keeps whatever size we hand back. A single huge
// sort will leave a correspondingly huge buffer in the pool until
// the next GC cycle evicts it, so workloads with wildly varying
// input sizes may occasionally hold onto more memory than necessary.
var abbrevPool sync.Pool

// getAbbrevs returns a *[]uint64 whose slice has length n. The
// underlying array may be reused from a previous sort.
//
// On a pool miss (or when the pooled buffer is too small), we
// overallocate by 25% — external-sort workloads tend to produce
// chunks whose sizes drift within a band as records pack to
// approximate byte targets, and without slack every upward drift
// would throw away the pooled buffer and allocate a fresh one.
// The slack is one-time per grow, and GC will evict an idle
// oversized buffer from the pool on its own.
func getAbbrevs(n int) *[]uint64 {
	if v := abbrevPool.Get(); v != nil {
		p := v.(*[]uint64)
		if cap(*p) >= n {
			*p = (*p)[:n]
			return p
		}
		// Too small — drop it and allocate fresh.
	}
	s := make([]uint64, n, n+n>>2)
	return &s
}

// putAbbrevs returns p to the pool for reuse.
func putAbbrevs(p *[]uint64) {
	abbrevPool.Put(p)
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

// sortKeyString sorts x by a string key using a parallel []uint64 of
// abbreviated keys alongside the input. The items array is permuted
// in place, mirroring swaps on the abbrevs array.
func sortKeyString[S ~[]E, E any](x S, key func(E) string, tiebreaker func(a, b E) int) {
	n := len(x)
	if n < 2 {
		return
	}

	var elemCmp func(a, b E) int
	if tiebreaker != nil {
		elemCmp = func(a, b E) int {
			if d := strings.Compare(key(a), key(b)); d != 0 {
				return d
			}
			return tiebreaker(a, b)
		}
	} else {
		elemCmp = func(a, b E) int {
			return strings.Compare(key(a), key(b))
		}
	}

	// For inputs big enough to go through the parallel partition path,
	// sniff a handful of sample abbreviations first. If they mostly
	// collide (long shared prefix, e.g. "https://..."), the abbrev trick
	// will just add overhead; fall back to a plain comparison sort
	// without ever allocating the full abbrevs array.
	if n >= minParallel && !hasAbbrevDiversity(x, func(e E) uint64 { return abbreviateString(key(e)) }) {
		SortFunc(x, elemCmp)
		return
	}

	pabbrevs := getAbbrevs(n)
	abbrevs := *pabbrevs
	for i := range x {
		abbrevs[i] = abbreviateString(key(x[i]))
	}
	sortSplitAbbrev(abbrevs, x, elemCmp)
	putAbbrevs(pabbrevs)
}

// --- []byte key path ---

// sortKeyBytesImpl sorts x by a []byte key using a parallel []uint64
// of abbreviated keys alongside the input.
func sortKeyBytesImpl[S ~[]E, E any](x S, key func(E) []byte, tiebreaker func(a, b E) int) {
	n := len(x)
	if n < 2 {
		return
	}

	var elemCmp func(a, b E) int
	if tiebreaker != nil {
		elemCmp = func(a, b E) int {
			if d := bytes.Compare(key(a), key(b)); d != 0 {
				return d
			}
			return tiebreaker(a, b)
		}
	} else {
		elemCmp = func(a, b E) int {
			return bytes.Compare(key(a), key(b))
		}
	}

	if n >= minParallel && !hasAbbrevDiversity(x, func(e E) uint64 { return abbreviateBytes(key(e)) }) {
		SortFunc(x, elemCmp)
		return
	}

	pabbrevs := getAbbrevs(n)
	abbrevs := *pabbrevs
	for i := range x {
		abbrevs[i] = abbreviateBytes(key(x[i]))
	}
	sortSplitAbbrev(abbrevs, x, elemCmp)
	putAbbrevs(pabbrevs)
}

// hasAbbrevDiversity samples a small number of elements from x, computes
// their abbreviated keys, and returns whether the distinct-abbrev count
// is at least half the sample size. It uses the same sample layout that
// partitionSplit would use, so the decision is representative of the
// pivots the partitioning step would pick.
func hasAbbrevDiversity[S ~[]E, E any](x S, abbrevFn func(E) uint64) bool {
	nproc := runtime.GOMAXPROCS(0)
	maxDepth, _ := partitionLayout(nproc)
	numSamples := 1 << maxDepth
	n := len(x)
	stride := n / numSamples
	if stride >= 2 && stride&(stride-1) == 0 {
		stride--
	}
	samples := make([]uint64, numSamples)
	for i := range samples {
		samples[i] = abbrevFn(x[i*stride+stride/2])
	}
	slices.Sort(samples)
	distinct := 1
	for i := 1; i < len(samples); i++ {
		if samples[i] != samples[i-1] {
			distinct++
		}
	}
	return distinct*2 >= numSamples
}

// --- Split-array sort: parallel partition + per-partition MSD radix ---

// sortSplitAbbrev sorts items using parallel partitioning followed by
// per-partition MSD radix sort, keeping keys and items in separate arrays.
// Callers are responsible for deciding the abbreviated-key path is worth
// taking (see hasAbbrevDiversity).
func sortSplitAbbrev[S ~[]E, E any](abbrevs []uint64, items S, elemCmp func(a, b E) int) {
	n := len(items)
	if n < minParallel {
		radixSortSplit(abbrevs, items, 0, elemCmp)
		return
	}

	nproc := runtime.GOMAXPROCS(0)
	parts, sorted := partitionSplit(abbrevs, items, nproc, elemCmp)
	if sorted {
		return
	}
	sortPartitions(parts, nproc, func(lo, hi int) {
		radixSortSplit(abbrevs[lo:hi], items[lo:hi], 0, elemCmp)
	})
}

// splitSample holds a sampled pivot for partitionSplit: both the
// abbreviated key and the original item, so partitioning can break
// ties correctly.
type splitSample[E any] struct {
	abbrev uint64
	item   E
}

// partitionSplit partitions both abbrevs and items arrays in sync using
// Hoare partitioning on the full (abbreviated key, element) order.
func partitionSplit[S ~[]E, E any](abbrevs []uint64, items S, nproc int, elemCmp func(a, b E) int) ([]span, bool) {
	n := len(abbrevs)
	maxDepth, parDepth := partitionLayout(nproc)
	numLeaves := 1 << maxDepth
	parts := make([]span, numLeaves)

	stride := n / numLeaves
	if stride >= 2 && stride&(stride-1) == 0 {
		stride--
	}
	samples := make([]splitSample[E], numLeaves)
	for i := range samples {
		idx := i*stride + stride/2
		samples[i] = splitSample[E]{abbrevs[idx], items[idx]}
	}

	sampleCmp := func(a, b splitSample[E]) int {
		if a.abbrev != b.abbrev {
			if a.abbrev < b.abbrev {
				return -1
			}
			return 1
		}
		return elemCmp(a.item, b.item)
	}

	if slices.IsSortedFunc(samples, sampleCmp) && slices.IsSorted(abbrevs) && slices.IsSortedFunc(items, elemCmp) {
		return nil, true
	}
	slices.SortFunc(samples, sampleCmp)

	var rec func(lo, hi, depth, leafStart int)
	rec = func(lo, hi, depth, leafStart int) {
		if depth >= maxDepth || hi-lo <= 1 {
			parts[leafStart] = span{lo, hi}
			return
		}

		halfLeaves := (1 << (maxDepth - depth)) / 2
		pivot := samples[leafStart+halfLeaves]
		split := hoarePartitionSplit(abbrevs, items, lo, hi, pivot.abbrev, pivot.item, elemCmp)

		if depth < parDepth {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() { rec(lo, split, depth+1, leafStart); wg.Done() }()
			rec(split, hi, depth+1, leafStart+halfLeaves)
			wg.Wait()
		} else {
			rec(lo, split, depth+1, leafStart)
			rec(split, hi, depth+1, leafStart+halfLeaves)
		}
	}

	rec(0, n, 0, 0)
	return parts, false
}

// splitLess reports whether (abbrevs[idx], items[idx]) is strictly less
// than (pivotAbbrev, pivotItem) using the abbreviated key first, then
// the full element comparison as tiebreaker.
func splitLess[S ~[]E, E any](abbrevs []uint64, items S, idx int, pivotAbbrev uint64, pivotItem E, elemCmp func(a, b E) int) bool {
	if abbrevs[idx] != pivotAbbrev {
		return abbrevs[idx] < pivotAbbrev
	}
	return elemCmp(items[idx], pivotItem) < 0
}

// splitGreater reports whether (abbrevs[idx], items[idx]) is strictly
// greater than (pivotAbbrev, pivotItem).
func splitGreater[S ~[]E, E any](abbrevs []uint64, items S, idx int, pivotAbbrev uint64, pivotItem E, elemCmp func(a, b E) int) bool {
	if abbrevs[idx] != pivotAbbrev {
		return abbrevs[idx] > pivotAbbrev
	}
	return elemCmp(items[idx], pivotItem) > 0
}

func hoarePartitionSplit[S ~[]E, E any](abbrevs []uint64, items S, lo, hi int, pivotAbbrev uint64, pivotItem E, elemCmp func(a, b E) int) int {
	i := lo - 1
	j := hi
	for {
		i++
		for i < hi && splitLess(abbrevs, items, i, pivotAbbrev, pivotItem, elemCmp) {
			i++
		}
		j--
		for j >= lo && splitGreater(abbrevs, items, j, pivotAbbrev, pivotItem, elemCmp) {
			j--
		}
		if i >= j {
			return j + 1
		}
		abbrevs[i], abbrevs[j] = abbrevs[j], abbrevs[i]
		items[i], items[j] = items[j], items[i]
	}
}

const splitInsertionThreshold = 32

// radixSortSplit performs MSD radix sort on the abbrevs array, mirroring
// all swaps to the items array. Falls back to insertion sort at small sizes,
// then resolves ties with elemCmp via slices.SortFunc.
func radixSortSplit[S ~[]E, E any](abbrevs []uint64, items S, byteIdx int, elemCmp func(a, b E) int) {
	n := len(abbrevs)
	if byteIdx >= 8 {
		// All 8 abbrev bytes are exhausted, so every element in this range
		// has the same abbreviated key. Sort items directly by elemCmp.
		slices.SortFunc(items, elemCmp)
		return
	}
	if n <= splitInsertionThreshold {
		// Insertion sort on abbrevs, mirroring to items.
		for i := 1; i < n; i++ {
			ak := abbrevs[i]
			ae := items[i]
			j := i - 1
			for j >= 0 && abbrevs[j] > ak {
				abbrevs[j+1] = abbrevs[j]
				items[j+1] = items[j]
				j--
			}
			abbrevs[j+1] = ak
			items[j+1] = ae
		}
		// Now resolve equal-key runs with elemCmp.
		i := 0
		for i < n {
			j := i + 1
			for j < n && abbrevs[j] == abbrevs[i] {
				j++
			}
			if j-i > 1 {
				slices.SortFunc(items[i:j], elemCmp)
			}
			i = j
		}
		return
	}

	shift := uint(56 - 8*byteIdx)

	var count [256]int
	for i := range abbrevs {
		count[uint8(abbrevs[i]>>shift)]++
	}

	var bucketStart [256]int
	var offset [256]int
	pos := 0
	for b := 0; b < 256; b++ {
		bucketStart[b] = pos
		offset[b] = pos
		pos += count[b]
	}

	for b := 0; b < 256; b++ {
		end := bucketStart[b] + count[b]
		for offset[b] < end {
			for i := offset[b]; i < end; i++ {
				target := int(uint8(abbrevs[i] >> shift))
				abbrevs[i], abbrevs[offset[target]] = abbrevs[offset[target]], abbrevs[i]
				items[i], items[offset[target]] = items[offset[target]], items[i]
				offset[target]++
			}
		}
	}

	for b := 0; b < 256; b++ {
		if count[b] > 1 {
			lo := bucketStart[b]
			hi := lo + count[b]
			radixSortSplit(abbrevs[lo:hi], items[lo:hi], byteIdx+1, elemCmp)
		}
	}
}
