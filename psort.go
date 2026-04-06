// Package psort provides parallel sorting for slices.
//
// [Sort] sorts ordered types using multiple goroutines. For []string,
// it automatically uses an abbreviated-key optimization (packing
// leading bytes into a uint64 for faster comparison) at the cost of a
// temporary []uint64 allocation. For all other ordered types, it is a
// purely in-place comparison sort.
//
// [SortKey] and [SortKeyBytes] sort by a key extracted from each
// element, using the same abbreviated-key optimization for string and
// []byte keys. See their documentation for details.
//
// [SortInPlace] and [SortFunc] are purely in-place comparison sorts
// with minimal allocation. Use [SortInPlace] to sort strings without
// the extra allocation, or [SortFunc] for a custom comparison.
//
// All functions partition the data into ordered spans using a
// quicksort-style partition pass, then sort each span independently
// and in parallel. For small slices, they fall back to the
// corresponding [slices] function directly.
package psort

import (
	"cmp"
	"math/bits"
	"runtime"
	"slices"
	"sync"
)

// minParallel is the smallest slice we'll try to sort in parallel.
// Below this, the goroutine overhead isn't worth it.
var minParallel = 1 << 13 // 8192

// Sort sorts the slice x in ascending order, distributing work across
// available CPUs. The sort is not stable.
//
// For []string, Sort uses an abbreviated-key optimization that is
// faster than a pure comparison sort for large slices, at the cost of
// a temporary []uint64 allocation of 8 bytes per element. For all
// other ordered types, Sort is an in-place comparison sort with
// minimal allocation.
//
// Use [SortInPlace] if you need a guaranteed in-place sort for strings,
// or [SortFunc] if you need a custom comparison function.
func Sort[S ~[]E, E cmp.Ordered](x S) {
	var zero E
	switch any(zero).(type) {
	case string:
		// If the input has long shared prefixes (e.g. URLs all starting
		// with "https://"), the abbreviated-key trick is wasted work.
		// Cheap sniff via sampled abbrevs decides whether to take it.
		// When bailing, SortInPlace is preferable to sortKeyString's
		// internal fallback because it uses the specialized slices.Sort
		// on cmp.Ordered types, avoiding a per-compare type assertion.
		if len(x) >= minParallel && !hasAbbrevDiversity(x, func(e E) uint64 { return abbreviateString(any(e).(string)) }) {
			SortInPlace(x)
			return
		}
		sortKeyString(x, func(e E) string { return any(e).(string) }, nil)
		return
	}
	SortInPlace(x)
}

// SortInPlace sorts the slice x in ascending order, distributing work
// across available CPUs. The sort is not stable.
//
// SortInPlace is a purely in-place comparison sort with minimal
// allocation. For []string, [Sort] uses an abbreviated-key
// optimization that is faster but allocates; use SortInPlace when you
// need to avoid that allocation.
func SortInPlace[S ~[]E, E cmp.Ordered](x S) {
	if len(x) < minParallel {
		slices.Sort(x)
		return
	}

	nproc := runtime.GOMAXPROCS(0)
	parts, sorted := partitionOrdered(x, nproc)
	if sorted {
		return
	}
	sortPartitions(parts, nproc, func(lo, hi int) {
		slices.Sort(x[lo:hi])
	})
}

// SortFunc sorts the slice x using the provided comparison function,
// distributing work across available CPUs. The sort is not stable.
// SortFunc will always be an in-place comparison sort that allocates
// very little memory.
//
// cmp(a, b) should return a negative number when a < b, a positive
// number when a > b, and zero when a == b or when a and b are
// incomparable in the sense of a strict weak ordering.
func SortFunc[S ~[]E, E any](x S, cmp func(a, b E) int) {
	if len(x) < minParallel {
		slices.SortFunc(x, cmp)
		return
	}

	nproc := runtime.GOMAXPROCS(0)
	parts, sorted := partitionCmpFunc(x, nproc, cmp)
	if sorted {
		return
	}
	sortPartitions(parts, nproc, func(lo, hi int) {
		slices.SortFunc(x[lo:hi], cmp)
	})
}

// sortPartitions sorts the given spans in parallel using a worker pool.
func sortPartitions(parts []span, nproc int, sortRange func(lo, hi int)) {
	workCh := make(chan span, len(parts))
	for _, p := range parts {
		workCh <- p
	}
	close(workCh)

	var wg sync.WaitGroup
	for w := 0; w < nproc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range workCh {
				sortRange(p.lo, p.hi)
			}
		}()
	}
	wg.Wait()
}

// span is a half-open range [lo, hi) of indices.
type span struct {
	lo, hi int
}

// --- Partition layout helpers (shared by both code paths) ---

// partitionLayout computes maxDepth and parDepth for a given nproc.
func partitionLayout(nproc int) (maxDepth, parDepth int) {
	// The multiplier ensures target >= 4 for any valid nproc (>= 1).
	// bits.Len requires target >= 2 to produce maxDepth >= 1.
	target := 4 * nproc
	maxDepth = bits.Len(uint(target - 1)) // ceil(log2(target))

	parDepth = bits.Len(uint(nproc - 1))
	if parDepth > maxDepth {
		parDepth = maxDepth
	}
	return
}

// --- cmp.Ordered code path (used by Sort) ---

func partitionOrdered[S ~[]E, E cmp.Ordered](data S, nproc int) ([]span, bool) {
	n := len(data)
	maxDepth, parDepth := partitionLayout(nproc)
	numLeaves := 1 << maxDepth
	parts := make([]span, numLeaves)

	// Sample numLeaves evenly-spaced elements and sort them to derive
	// all pivots up front. samples[leafStart+halfLeaves] at each
	// recursion level gives the appropriate quantile pivot.
	// If the stride is a power of 2, decrement it to avoid alignment
	// with power-of-2 data patterns.
	stride := n / numLeaves
	if stride >= 2 && stride&(stride-1) == 0 {
		stride--
	}
	samples := make([]E, numLeaves)
	for i := range samples {
		samples[i] = data[i*stride+stride/2]
	}
	if slices.IsSorted(samples) && slices.IsSorted(data) {
		return nil, true
	}
	slices.Sort(samples)

	var rec func(lo, hi, depth, leafStart int)
	rec = func(lo, hi, depth, leafStart int) {
		if depth >= maxDepth || hi-lo <= 1 {
			parts[leafStart] = span{lo, hi}
			return
		}

		halfLeaves := (1 << (maxDepth - depth)) / 2
		pivot := samples[leafStart+halfLeaves]
		split := hoarePartitionOrdered(data, lo, hi, pivot)

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

func hoarePartitionOrdered[S ~[]E, E cmp.Ordered](data S, lo, hi int, pivot E) int {
	i := lo - 1
	j := hi
	for {
		i++
		for i < hi && cmp.Less(data[i], pivot) {
			i++
		}
		j--
		for j >= lo && cmp.Less(pivot, data[j]) {
			j--
		}
		if i >= j {
			return j + 1
		}
		data[i], data[j] = data[j], data[i]
	}
}

// --- cmp func code path (used by SortFunc) ---

func partitionCmpFunc[S ~[]E, E any](data S, nproc int, cmpFn func(a, b E) int) ([]span, bool) {
	n := len(data)
	maxDepth, parDepth := partitionLayout(nproc)
	numLeaves := 1 << maxDepth
	parts := make([]span, numLeaves)

	stride := n / numLeaves
	if stride >= 2 && stride&(stride-1) == 0 {
		stride--
	}
	samples := make([]E, numLeaves)
	for i := range samples {
		samples[i] = data[i*stride+stride/2]
	}
	if slices.IsSortedFunc(samples, cmpFn) && slices.IsSortedFunc(data, cmpFn) {
		return nil, true
	}
	slices.SortFunc(samples, cmpFn)

	var rec func(lo, hi, depth, leafStart int)
	rec = func(lo, hi, depth, leafStart int) {
		if depth >= maxDepth || hi-lo <= 1 {
			parts[leafStart] = span{lo, hi}
			return
		}

		halfLeaves := (1 << (maxDepth - depth)) / 2
		pivot := samples[leafStart+halfLeaves]
		split := hoarePartitionCmpFunc(data, lo, hi, pivot, cmpFn)

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

func hoarePartitionCmpFunc[S ~[]E, E any](data S, lo, hi int, pivot E, cmpFn func(a, b E) int) int {
	i := lo - 1
	j := hi
	for {
		i++
		for i < hi && cmpFn(data[i], pivot) < 0 {
			i++
		}
		j--
		for j >= lo && cmpFn(data[j], pivot) > 0 {
			j--
		}
		if i >= j {
			return j + 1
		}
		data[i], data[j] = data[j], data[i]
	}
}
