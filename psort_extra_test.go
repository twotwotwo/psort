package psort

// Bonus tests -- some are a bit goofy but as long as they don't prevent
// changes I figure the more the better

import (
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"testing"
)

// --- Partition layout unit tests ---

func TestPartitionLayout(t *testing.T) {
	cases := []struct {
		nproc    int
		wantMax  int
		wantPar  int
	}{
		{1, 2, 0},
		{2, 3, 1},
		{4, 4, 2},
		{8, 5, 3},
		{16, 6, 4},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("nproc=%d", tc.nproc), func(t *testing.T) {
			maxD, parD := partitionLayout(tc.nproc)
			if maxD != tc.wantMax {
				t.Errorf("maxDepth: got %d, want %d", maxD, tc.wantMax)
			}
			if parD != tc.wantPar {
				t.Errorf("parDepth: got %d, want %d", parD, tc.wantPar)
			}
		})
	}
}

// --- Recursive partitioning: verify spans tile [0, n) ---

func TestPartitionSpansCoverRange(t *testing.T) {
	for _, nproc := range []int{1, 2, 4, 8} {
		for _, n := range []int{minParallel, minParallel * 2, 10000} {
			t.Run(fmt.Sprintf("nproc=%d/n=%d", nproc, n), func(t *testing.T) {
				data := make([]int, n)
				for i := range data {
					data[i] = rand.Intn(n)
				}

				old := runtime.GOMAXPROCS(nproc)
				parts, sorted := partitionOrdered(data, nproc)
				runtime.GOMAXPROCS(old)

				if sorted {
					return // data happened to be sorted, skip
				}

				// Collect non-empty spans and verify they tile [0, n).
				var nonEmpty []span
				for _, p := range parts {
					if p.hi > p.lo {
						nonEmpty = append(nonEmpty, p)
					}
				}
				slices.SortFunc(nonEmpty, func(a, b span) int {
					return a.lo - b.lo
				})

				if len(nonEmpty) == 0 {
					t.Fatalf("no non-empty spans")
				}
				if nonEmpty[0].lo != 0 {
					t.Errorf("first span starts at %d, not 0", nonEmpty[0].lo)
				}
				for i := 1; i < len(nonEmpty); i++ {
					if nonEmpty[i].lo != nonEmpty[i-1].hi {
						t.Errorf("gap or overlap between span %d [%d,%d) and span %d [%d,%d)",
							i-1, nonEmpty[i-1].lo, nonEmpty[i-1].hi,
							i, nonEmpty[i].lo, nonEmpty[i].hi)
					}
				}
				if nonEmpty[len(nonEmpty)-1].hi != n {
					t.Errorf("last span ends at %d, not %d", nonEmpty[len(nonEmpty)-1].hi, n)
				}
			})
		}
	}
}

// TestPartitionOrderInvariant checks that after partitionOrdered, for each
// pair of adjacent spans i and i+1, max(span_i) <= min(span_{i+1}).
// This is the KEY correctness property — if this holds, sorting each span
// independently produces a fully sorted result.
func TestPartitionOrderInvariant(t *testing.T) {
	for _, nproc := range []int{1, 2, 4, 8} {
		for _, n := range []int{minParallel, 1000, 10000} {
			for _, pattern := range []string{"random", "few_unique", "sorted", "reversed", "organ_pipe"} {
				t.Run(fmt.Sprintf("nproc=%d/n=%d/%s", nproc, n, pattern), func(t *testing.T) {
					data := make([]int, n)
					switch pattern {
					case "random":
						for i := range data {
							data[i] = rand.Intn(n)
						}
					case "few_unique":
						for i := range data {
							data[i] = rand.Intn(5)
						}
					case "sorted":
						for i := range data {
							data[i] = i
						}
					case "reversed":
						for i := range data {
							data[i] = n - i
						}
					case "organ_pipe":
						for i := range data {
							if i < n/2 {
								data[i] = i
							} else {
								data[i] = n - i
							}
						}
					}

					old := runtime.GOMAXPROCS(nproc)
					parts, sorted := partitionOrdered(data, nproc)
					runtime.GOMAXPROCS(old)

					if sorted {
						return
					}

					// For each pair of adjacent non-empty spans, check ordering.
					var nonEmpty []span
					for _, p := range parts {
						if p.hi > p.lo {
							nonEmpty = append(nonEmpty, p)
						}
					}

					for i := 0; i+1 < len(nonEmpty); i++ {
						maxLeft := data[nonEmpty[i].lo]
						for j := nonEmpty[i].lo; j < nonEmpty[i].hi; j++ {
							if data[j] > maxLeft {
								maxLeft = data[j]
							}
						}
						minRight := data[nonEmpty[i+1].lo]
						for j := nonEmpty[i+1].lo; j < nonEmpty[i+1].hi; j++ {
							if data[j] < minRight {
								minRight = data[j]
							}
						}
						if maxLeft > minRight {
							t.Errorf("span %d max=%d > span %d min=%d", i, maxLeft, i+1, minRight)
						}
					}
				})
			}
		}
	}
}

// --- Adversarial inputs ---

// TestSortPipeOrgan — classic quicksort killer pattern.
func TestSortPipeOrgan(t *testing.T) {
	for _, n := range []int{minParallel, 10000, 100000} {
		for _, s := range intSorters {
			t.Run(fmt.Sprintf("%s/n=%d", s.name, n), func(t *testing.T) {
				data := make([]int, n)
				for i := 0; i < n/2; i++ {
					data[i] = i
				}
				for i := n / 2; i < n; i++ {
					data[i] = n - i
				}
				s.sort(data)
				if !slices.IsSorted(data) {
					t.Errorf("pipe organ not sorted")
				}
			})
		}
	}
}

// TestSortReversed — fully descending.
func TestSortReversed(t *testing.T) {
	for _, n := range []int{minParallel, 100000} {
		for _, s := range intSorters {
			t.Run(fmt.Sprintf("%s/n=%d", s.name, n), func(t *testing.T) {
				data := make([]int, n)
				for i := range data {
					data[i] = n - i
				}
				s.sort(data)
				if !slices.IsSorted(data) {
					t.Errorf("reversed not sorted")
				}
			})
		}
	}
}

// TestSortPreservesMultiset — verify no elements lost or duplicated.
func TestSortPreservesMultiset(t *testing.T) {
	for _, n := range []int{minParallel, 50000} {
		for _, s := range intSorters {
			t.Run(fmt.Sprintf("%s/n=%d", s.name, n), func(t *testing.T) {
				data := make([]int, n)
				for i := range data {
					data[i] = rand.Intn(n / 3)
				}
				counts := make(map[int]int)
				for _, v := range data {
					counts[v]++
				}
				s.sort(data)
				got := make(map[int]int)
				for _, v := range data {
					got[v]++
				}
				for k, v := range counts {
					if got[k] != v {
						t.Errorf("value %d: had %d, now %d", k, v, got[k])
					}
				}
			})
		}
	}
}

// TestSortAlreadySorted — verify the early-exit optimization.
func TestSortAlreadySorted(t *testing.T) {
	for _, n := range []int{0, 1, minParallel, 100000} {
		for _, s := range intSorters {
			t.Run(fmt.Sprintf("%s/n=%d", s.name, n), func(t *testing.T) {
				data := make([]int, n)
				for i := range data {
					data[i] = i
				}
				s.sort(data)
				if !slices.IsSorted(data) {
					t.Errorf("already-sorted became unsorted")
				}
			})
		}
	}
}

// TestSortAlmostSorted — one swap away from sorted.
func TestSortAlmostSorted(t *testing.T) {
	for _, n := range []int{minParallel, 100000} {
		for _, s := range intSorters {
			t.Run(fmt.Sprintf("%s/n=%d", s.name, n), func(t *testing.T) {
				data := make([]int, n)
				for i := range data {
					data[i] = i
				}
				// Swap two random elements to break sortedness.
				i, j := rand.Intn(n), rand.Intn(n)
				data[i], data[j] = data[j], data[i]
				s.sort(data)
				if !slices.IsSorted(data) {
					t.Errorf("almost-sorted not correctly sorted")
				}
			})
		}
	}
}
