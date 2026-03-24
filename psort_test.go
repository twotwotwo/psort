// Tests adapted from Go's sort/slices packages and twotwotwo/sorts.

package psort

import (
	"cmp"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// prodMinParallel is the production value of minParallel, saved before
// tests lower it so benchmarks can restore it.
var prodMinParallel = minParallel

func TestMain(m *testing.M) {
	minParallel = 64
	os.Exit(m.Run())
}

func restoreMinParallel(b *testing.B) {
	b.Helper()
	minParallel = prodMinParallel
	b.Cleanup(func() { minParallel = 64 })
}

// intSorter abstracts over Sort and SortFunc for test reuse.
type intSorter struct {
	name string
	sort func([]int)
}

var intSorters = []intSorter{
	{"Sort", Sort[[]int]},
	{"SortInPlace", SortInPlace[[]int]},
	{"SortFunc", func(x []int) { SortFunc(x, cmp.Compare) }},
}

// stringSorter abstracts over Sort and SortFunc for string test reuse.
type stringSorter struct {
	name string
	sort func([]string)
}

var stringSorters = []stringSorter{
	{"Sort", Sort[[]string]},
	{"SortInPlace", SortInPlace[[]string]},
	{"SortFunc", func(x []string) { SortFunc(x, strings.Compare) }},
}

// --- Basic correctness ---

var ints = [...]int{74, 59, 238, -784, 9845, 959, 905, 0, 0, 42, 7586, -5467984, 7586}
var strs = [...]string{"", "Hello", "foo", "bar", "foo", "f00", "%*&^*&^&", "***"}

func TestSortInts(t *testing.T) {
	for _, s := range intSorters {
		t.Run(s.name, func(t *testing.T) {
			data := slices.Clone(ints[:])
			s.sort(data)
			if !slices.IsSorted(data) {
				t.Errorf("sorted %v\n   got %v", ints, data)
			}
		})
	}
}

func TestSortStrings(t *testing.T) {
	for _, s := range stringSorters {
		t.Run(s.name, func(t *testing.T) {
			data := slices.Clone(strs[:])
			s.sort(data)
			if !slices.IsSorted(data) {
				t.Errorf("sorted %v\n   got %v", strs, data)
			}
		})
	}
}

// --- Edge cases (from twotwotwo/sorts) ---

func TestEmpty(t *testing.T) {
	for _, s := range intSorters {
		t.Run(s.name, func(t *testing.T) {
			s.sort([]int(nil))
			s.sort([]int{})
		})
	}
}

func TestSingle(t *testing.T) {
	for _, s := range intSorters {
		t.Run(s.name, func(t *testing.T) {
			data := []int{42}
			s.sort(data)
			if data[0] != 42 {
				t.Errorf("single element changed")
			}
		})
	}
}

func TestTwo(t *testing.T) {
	for _, s := range intSorters {
		t.Run(s.name, func(t *testing.T) {
			data := []int{2, 1}
			s.sort(data)
			if !slices.IsSorted(data) {
				t.Errorf("two elements not sorted: %v", data)
			}
		})
	}
}

func TestAllEqual(t *testing.T) {
	for _, s := range intSorters {
		t.Run(s.name, func(t *testing.T) {
			data := make([]int, 10000)
			for i := range data {
				data[i] = 42
			}
			s.sort(data)
			for _, v := range data {
				if v != 42 {
					t.Fatalf("equal elements corrupted")
				}
			}
		})
	}
}

// --- Large random (from stdlib and twotwotwo/sorts) ---

func TestSortLargeRandom(t *testing.T) {
	n := 1_000_000
	if testing.Short() {
		n /= 100
	}
	for _, s := range intSorters {
		t.Run(s.name, func(t *testing.T) {
			data := make([]int, n)
			for i := range data {
				data[i] = rand.Intn(100)
			}
			if slices.IsSorted(data) {
				t.Fatalf("terrible rand")
			}
			s.sort(data)
			if !slices.IsSorted(data) {
				t.Errorf("sort didn't sort - %d ints", n)
			}
		})
	}
}

func TestSortLargeRandomStrings(t *testing.T) {
	n := 500_000
	if testing.Short() {
		n /= 100
	}
	for _, s := range stringSorters {
		t.Run(s.name, func(t *testing.T) {
			data := make([]string, n)
			for i := range data {
				data[i] = randomString(rand.Intn(50) + 1)
			}
			s.sort(data)
			if !slices.IsSorted(data) {
				t.Errorf("sort didn't sort %d strings", n)
			}
		})
	}
}

func randomString(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}

// --- Bentley-McIlroy torture test (from Go stdlib / twotwotwo/sorts) ---

const (
	bmSawtooth = iota
	bmRand
	bmStagger
	bmPlateau
	bmShuffle
	bmNDist
)

const (
	bmCopy = iota
	bmReverse
	bmReverseFirstHalf
	bmReverseSecondHalf
	bmSorted
	bmDither
	bmNMode
)

func TestBentleyMcIlroy(t *testing.T) {
	sizes := []int{100, 1023, 1024, 1025, 10000}
	if testing.Short() {
		sizes = []int{100, 127, 128, 129}
	}
	dists := []string{"sawtooth", "rand", "stagger", "plateau", "shuffle"}
	modes := []string{"copy", "reverse", "reverse1", "reverse2", "sort", "dither"}

	for _, s := range intSorters {
		t.Run(s.name, func(t *testing.T) {
			runBentleyMcIlroy(t, s.sort, sizes, dists, modes)
		})
	}
}

func runBentleyMcIlroy(t *testing.T, sortFn func([]int), sizes []int, dists, modes []string) {
	var tmp1, tmp2 [10001]int
	for _, n := range sizes {
		for m := 1; m < 2*n; m *= 2 {
			for dist := 0; dist < bmNDist; dist++ {
				j := 0
				k := 1
				data := tmp1[0:n]
				for i := 0; i < n; i++ {
					switch dist {
					case bmSawtooth:
						data[i] = i % m
					case bmRand:
						data[i] = rand.Intn(m)
					case bmStagger:
						data[i] = (i*m + i) % n
					case bmPlateau:
						data[i] = min(i, m)
					case bmShuffle:
						if rand.Intn(m) != 0 {
							j += 2
							data[i] = j
						} else {
							k += 2
							data[i] = k
						}
					}
				}

				mdata := tmp2[0:n]
				for mode := 0; mode < bmNMode; mode++ {
					switch mode {
					case bmCopy:
						copy(mdata, data)
					case bmReverse:
						for i := 0; i < n; i++ {
							mdata[i] = data[n-i-1]
						}
					case bmReverseFirstHalf:
						for i := 0; i < n/2; i++ {
							mdata[i] = data[n/2-i-1]
						}
						for i := n / 2; i < n; i++ {
							mdata[i] = data[i]
						}
					case bmReverseSecondHalf:
						for i := 0; i < n/2; i++ {
							mdata[i] = data[i]
						}
						for i := n / 2; i < n; i++ {
							mdata[i] = data[n-(i-n/2)-1]
						}
					case bmSorted:
						copy(mdata, data)
						slices.Sort(mdata)
					case bmDither:
						for i := 0; i < n; i++ {
							mdata[i] = data[i] + i%5
						}
					}

					desc := fmt.Sprintf("n=%d/m=%d/%s/%s", n, m, dists[dist], modes[mode])
					t.Run(desc, func(t *testing.T) {
						work := slices.Clone(mdata)
						sortFn(work)
						if !slices.IsSorted(work) {
							t.Errorf("ints not sorted")
						}
					})
				}
			}
		}
	}
}

// --- Test that we match stdlib exactly (permutation check) ---

func TestMatchesStdlib(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 10, 100, 1000, 50000} {
		data1 := make([]int, n)
		for i := range data1 {
			data1[i] = rand.Intn(n/5 + 1)
		}

		for _, s := range intSorters {
			t.Run(fmt.Sprintf("%s/n=%d", s.name, n), func(t *testing.T) {
				got := slices.Clone(data1)
				want := slices.Clone(data1)
				s.sort(got)
				slices.Sort(want)
				if !slices.Equal(got, want) {
					t.Errorf("result differs from slices.Sort")
				}
			})
		}
	}
}

// --- Test with custom comparator / struct sorting (SortFunc only) ---

type record struct {
	key   string
	value int
}

func TestSortFuncStructs(t *testing.T) {
	n := 100_000
	if testing.Short() {
		n /= 100
	}
	data := make([]record, n)
	for i := range data {
		data[i] = record{
			key:   randomString(rand.Intn(20) + 1),
			value: i,
		}
	}

	cmpFn := func(a, b record) int {
		return strings.Compare(a.key, b.key)
	}

	SortFunc(data, cmpFn)
	if !slices.IsSortedFunc(data, cmpFn) {
		t.Errorf("struct sort failed")
	}
}

// --- Test forced serial path (small input) ---

func TestSmallInputSerial(t *testing.T) {
	for _, s := range intSorters {
		t.Run(s.name, func(t *testing.T) {
			for _, n := range []int{0, 1, 2, 10, 100, 1000, minParallel - 1} {
				data := make([]int, n)
				for i := range data {
					data[i] = rand.Intn(max(n, 1))
				}
				s.sort(data)
				if !slices.IsSorted(data) {
					t.Errorf("serial path failed for n=%d", n)
				}
			}
		})
	}
}

// --- Low-cardinality pivot coverage ---
//
// With sample-derived pivots, a pivot value may not exist in every
// subrange after partitioning. This test exercises skewed
// low-cardinality data where that situation arises. It checks both
// sort order and element counts to catch corruption.

func TestLowCardinalityPivots(t *testing.T) {
	for _, nproc := range []int{1, 2, 4, 8, 16} {
		for _, n := range []int{minParallel, minParallel * 4, 1 << 20} {
			for _, s := range intSorters {
				t.Run(fmt.Sprintf("%s/nproc=%d/n=%d", s.name, nproc, n), func(t *testing.T) {
					data := make([]int, n)
					// 87.5% zeros, 12.5% ones at the end.
					ones := n / 8
					for i := n - ones; i < n; i++ {
						data[i] = 1
					}

					old := runtime.GOMAXPROCS(nproc)
					s.sort(data)
					runtime.GOMAXPROCS(old)

					if !slices.IsSorted(data) {
						t.Fatalf("not sorted")
					}
					got := 0
					for _, v := range data {
						got += v
					}
					if got != ones {
						t.Fatalf("element count changed: want %d ones, got %d", ones, got)
					}
				})
			}
		}
	}
}

// --- Power-of-2 stride boundary ---

// TestSortPowerOf2Stride — when n/numLeaves is a power of 2, stride is
// decremented. Verify correctness at these boundaries.
func TestSortPowerOf2Stride(t *testing.T) {
	// For nproc=1, numLeaves=4, so stride = n/4.
	// stride is pow2 when n/4 is pow2, e.g. n = 4*2 = 8, 4*4 = 16, etc.
	// But minParallel limits us. With test minParallel=64, try n where
	// stride = n/numLeaves hits powers of 2.
	for _, nproc := range []int{1, 2, 4} {
		maxD, _ := partitionLayout(nproc)
		numLeaves := 1 << maxD
		for _, mult := range []int{2, 4, 8, 16, 32} {
			n := numLeaves * mult
			if n < minParallel {
				continue
			}
			for _, s := range intSorters {
				t.Run(fmt.Sprintf("%s/nproc=%d/n=%d", s.name, nproc, n), func(t *testing.T) {
					data := make([]int, n)
					for i := range data {
						data[i] = rand.Intn(n)
					}
					want := slices.Clone(data)
					slices.Sort(want)
					old := runtime.GOMAXPROCS(nproc)
					s.sort(data)
					runtime.GOMAXPROCS(old)
					if !slices.Equal(data, want) {
						t.Errorf("mismatch at pow2 stride boundary")
					}
				})
			}
		}
	}
}

// --- Hoare partition unit tests ---

// TestHoarePartitionInvariant verifies that after partitioning, all elements
// in [lo, split) are <= pivot and all elements in [split, hi) are >= pivot.
func TestHoarePartitionInvariant(t *testing.T) {
	rng := rand.New(rand.NewSource(12345))

	cases := []struct {
		name  string
		build func(n int) ([]int, int) // returns (data, pivot)
	}{
		{"random_pivot_in_data", func(n int) ([]int, int) {
			data := make([]int, n)
			for i := range data {
				data[i] = rng.Intn(100)
			}
			return data, data[rng.Intn(n)]
		}},
		{"random_pivot_absent", func(n int) ([]int, int) {
			// Pivot may not be in the data — only even values in data, odd pivot.
			data := make([]int, n)
			for i := range data {
				data[i] = rng.Intn(50) * 2
			}
			return data, rng.Intn(50)*2 + 1
		}},
		{"all_equal", func(n int) ([]int, int) {
			data := make([]int, n)
			for i := range data {
				data[i] = 42
			}
			return data, 42
		}},
		{"pivot_smaller_than_all", func(n int) ([]int, int) {
			data := make([]int, n)
			for i := range data {
				data[i] = rng.Intn(100) + 100
			}
			return data, 0 // pivot < everything
		}},
		{"pivot_larger_than_all", func(n int) ([]int, int) {
			data := make([]int, n)
			for i := range data {
				data[i] = rng.Intn(100)
			}
			return data, 999 // pivot > everything
		}},
		{"two_values", func(n int) ([]int, int) {
			data := make([]int, n)
			for i := range data {
				if rng.Intn(2) == 0 {
					data[i] = 0
				} else {
					data[i] = 1
				}
			}
			return data, data[0]
		}},
		{"sorted_ascending", func(n int) ([]int, int) {
			data := make([]int, n)
			for i := range data {
				data[i] = i
			}
			return data, data[n/2]
		}},
		{"sorted_descending", func(n int) ([]int, int) {
			data := make([]int, n)
			for i := range data {
				data[i] = n - i
			}
			return data, data[n/2]
		}},
	}

	for _, tc := range cases {
		for _, n := range []int{2, 3, 5, 10, 100, 1000} {
			t.Run(fmt.Sprintf("%s/n=%d", tc.name, n), func(t *testing.T) {
				data, pivot := tc.build(n)
				orig := slices.Clone(data)

				split := hoarePartitionOrdered(data, 0, n, pivot)

				if split < 0 || split > n {
					t.Fatalf("split %d out of range [0, %d]", split, n)
				}

				for i := 0; i < split; i++ {
					if cmp.Less(pivot, data[i]) {
						t.Errorf("left side: data[%d]=%d > pivot=%d", i, data[i], pivot)
					}
				}
				for i := split; i < n; i++ {
					if cmp.Less(data[i], pivot) {
						t.Errorf("right side: data[%d]=%d < pivot=%d", i, data[i], pivot)
					}
				}

				// Check no elements were lost or created.
				slices.Sort(data)
				slices.Sort(orig)
				if !slices.Equal(data, orig) {
					t.Errorf("element multiset changed")
				}
			})
		}
	}
}

// TestHoarePartitionCmpFuncInvariant — same for the CmpFunc path.
func TestHoarePartitionCmpFuncInvariant(t *testing.T) {
	rng := rand.New(rand.NewSource(11111))

	for _, n := range []int{2, 3, 10, 100, 1000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			data := make([]int, n)
			for i := range data {
				data[i] = rng.Intn(50)
			}
			pivot := data[rng.Intn(n)]
			orig := slices.Clone(data)

			split := hoarePartitionCmpFunc(data, 0, n, pivot, cmp.Compare)

			if split < 0 || split > n {
				t.Fatalf("split %d out of range", split)
			}
			for i := 0; i < split; i++ {
				if data[i] > pivot {
					t.Errorf("left: data[%d]=%d > pivot=%d", i, data[i], pivot)
				}
			}
			for i := split; i < n; i++ {
				if data[i] < pivot {
					t.Errorf("right: data[%d]=%d < pivot=%d", i, data[i], pivot)
				}
			}
			slices.Sort(data)
			slices.Sort(orig)
			if !slices.Equal(data, orig) {
				t.Errorf("element multiset changed")
			}
		})
	}
}

// --- Few-unique and GOMAXPROCS variation ---

// TestSortFewUnique — low cardinality exercises equal-pivot paths.
func TestSortFewUnique(t *testing.T) {
	for _, k := range []int{1, 2, 3, 5, 10} {
		for _, n := range []int{minParallel, 100000} {
			for _, s := range intSorters {
				t.Run(fmt.Sprintf("%s/k=%d/n=%d", s.name, k, n), func(t *testing.T) {
					data := make([]int, n)
					for i := range data {
						data[i] = rand.Intn(k)
					}
					want := slices.Clone(data)
					slices.Sort(want)
					s.sort(data)
					if !slices.Equal(data, want) {
						t.Errorf("few-unique result differs from stdlib")
					}
				})
			}
		}
	}
}

// TestSortWithDifferentGOMAXPROCS — exercise various worker counts.
func TestSortWithDifferentGOMAXPROCS(t *testing.T) {
	n := 100000
	for _, nproc := range []int{1, 2, 3, 4, 7, 8, 16, 32, 64, 256} {
		for _, s := range intSorters {
			t.Run(fmt.Sprintf("%s/nproc=%d", s.name, nproc), func(t *testing.T) {
				data := make([]int, n)
				for i := range data {
					data[i] = rand.Intn(1000)
				}
				want := slices.Clone(data)
				slices.Sort(want)
				old := runtime.GOMAXPROCS(nproc)
				s.sort(data)
				runtime.GOMAXPROCS(old)
				if !slices.Equal(data, want) {
					t.Errorf("mismatch with GOMAXPROCS=%d", nproc)
				}
			})
		}
	}
}

// --- Benchmarks ---

func BenchmarkSort(b *testing.B) {
	restoreMinParallel(b)
	for _, size := range []int{1 << 10, 1 << 16, 1 << 20} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			unsorted := make([]int, size)
			for i := range unsorted {
				unsorted[i] = i ^ 0x2cc
			}
			data := make([]int, size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(data, unsorted)
				Sort(data)
			}
		})
	}
}

func BenchmarkSortFunc(b *testing.B) {
	restoreMinParallel(b)
	for _, size := range []int{1 << 10, 1 << 16, 1 << 20} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			unsorted := make([]int, size)
			for i := range unsorted {
				unsorted[i] = i ^ 0x2cc
			}
			data := make([]int, size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(data, unsorted)
				SortFunc(data, cmp.Compare)
			}
		})
	}
}

func BenchmarkSortStrings(b *testing.B) {
	restoreMinParallel(b)
	for _, size := range []int{1 << 10, 1 << 16, 1 << 20} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			unsorted := make([]string, size)
			for i := range unsorted {
				unsorted[i] = randomString(20)
			}
			data := make([]string, size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(data, unsorted)
				Sort(data)
			}
		})
	}
}

func BenchmarkSortInPlaceStrings(b *testing.B) {
	restoreMinParallel(b)
	for _, size := range []int{1 << 10, 1 << 16, 1 << 20} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			unsorted := make([]string, size)
			for i := range unsorted {
				unsorted[i] = randomString(20)
			}
			data := make([]string, size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(data, unsorted)
				SortInPlace(data)
			}
		})
	}
}

func BenchmarkSortFuncStrings(b *testing.B) {
	restoreMinParallel(b)
	for _, size := range []int{1 << 10, 1 << 16, 1 << 20} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			unsorted := make([]string, size)
			for i := range unsorted {
				unsorted[i] = randomString(20)
			}
			data := make([]string, size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(data, unsorted)
				SortFunc(data, strings.Compare)
			}
		})
	}
}

// Baseline: stdlib for comparison.
func BenchmarkStdlibSort(b *testing.B) {
	restoreMinParallel(b)
	for _, size := range []int{1 << 10, 1 << 16, 1 << 20} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			unsorted := make([]int, size)
			for i := range unsorted {
				unsorted[i] = i ^ 0x2cc
			}
			data := make([]int, size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(data, unsorted)
				slices.Sort(data)
			}
		})
	}
}

func BenchmarkStdlibSortStrings(b *testing.B) {
	restoreMinParallel(b)
	for _, size := range []int{1 << 10, 1 << 16, 1 << 20} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			unsorted := make([]string, size)
			for i := range unsorted {
				unsorted[i] = randomString(20)
			}
			data := make([]string, size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(data, unsorted)
				slices.Sort(data)
			}
		})
	}
}

// Benchmark various input patterns at 1M.
func BenchmarkSortPatterns(b *testing.B) {
	restoreMinParallel(b)
	const size = 1 << 20
	patterns := map[string]func([]int){
		"random": func(data []int) {
			for i := range data {
				data[i] = rand.Intn(size)
			}
		},
		"sorted": func(data []int) {
			for i := range data {
				data[i] = i
			}
		},
		"reversed": func(data []int) {
			for i := range data {
				data[i] = size - i
			}
		},
		"mod8": func(data []int) {
			for i := range data {
				data[i] = i % 8
			}
		},
		"allEqual": func(data []int) {
			for i := range data {
				data[i] = 1
			}
		},
	}
	for name, gen := range patterns {
		for _, s := range intSorters {
			b.Run(name+"/"+s.name, func(b *testing.B) {
				unsorted := make([]int, size)
				gen(unsorted)
				data := make([]int, size)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					copy(data, unsorted)
					s.sort(data)
				}
			})
		}
	}
}

func sizeLabel(n int) string {
	switch {
	case n >= 1<<20:
		return "1M"
	case n >= 1<<16:
		return "64K"
	case n >= 1<<10:
		return "1K"
	default:
		return "small"
	}
}
