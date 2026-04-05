package psort

import (
	"math/rand"
	"slices"
	"strings"
	"testing"
)

func TestSortStringsVariousSizes(t *testing.T) {
	for _, n := range []int{0, 1, 2, 10, 100, 1000, 50000, 200000} {
		data := make([]string, n)
		for i := range data {
			data[i] = randomString(rand.Intn(30) + 1)
		}
		want := slices.Clone(data)
		slices.Sort(want)
		Sort(data)
		if !slices.Equal(data, want) {
			t.Errorf("n=%d: mismatch", n)
		}
	}
}

func TestRadixSortSplitSharedPrefixSmall(t *testing.T) {
	// Test radixSortSplit directly with equal abbreviated keys.
	prefix := "aaaaaaaaa" // 9 chars, first 8 identical -> same abbrev
	data := []string{prefix + "z", prefix + "a", prefix + "m", prefix + "b"}
	abbrevs := make([]uint64, len(data))
	for i := range data {
		abbrevs[i] = abbreviateString(data[i])
	}
	t.Logf("abbrevs: %v", abbrevs)
	t.Logf("before: %v", data)

	elemCmp := func(a, b string) int {
		return strings.Compare(a, b)
	}
	radixSortSplit(abbrevs, data, 0, elemCmp)
	t.Logf("after: %v", data)

	want := []string{prefix + "a", prefix + "b", prefix + "m", prefix + "z"}
	if !slices.Equal(data, want) {
		t.Errorf("got %v, want %v", data, want)
	}

	// Larger case: above insertion threshold (32)
	for _, n := range []int{33, 50, 100, 500} {
		data := make([]string, n)
		for i := range data {
			data[i] = prefix + randomString(rand.Intn(10)+1)
		}
		want := slices.Clone(data)
		slices.Sort(want)
		abbrevs := make([]uint64, n)
		for i := range data {
			abbrevs[i] = abbreviateString(data[i])
		}
		radixSortSplit(abbrevs, data, 0, elemCmp)
		if !slices.Equal(data, want) {
			t.Errorf("n=%d: mismatch", n)
			for i := range data {
				if data[i] != want[i] {
					t.Logf("first mismatch at %d: got %q want %q", i, data[i], want[i])
					break
				}
			}
		}
	}
}

func TestSortKeyStringSharedPrefixDirect(t *testing.T) {
	prefix := "aaaaaaaaaaaaaaaaaaaaaaaa"
	n := 100
	data := make([]string, n)
	for i := range data {
		data[i] = prefix + randomString(rand.Intn(10)+1)
	}
	want := slices.Clone(data)
	slices.Sort(want)

	// Test via radixSortSplit using the same closure style as sortKeyString.
	key := func(s string) string { return s }
	elemCmp := func(a, b string) int {
		return strings.Compare(key(a), key(b))
	}
	abbrevs := make([]uint64, n)
	for i := range data {
		abbrevs[i] = abbreviateString(key(data[i]))
	}
	radixSortSplit(abbrevs, data, 0, elemCmp)

	if !slices.Equal(data, want) {
		for i := range data {
			if data[i] != want[i] {
				t.Errorf("radixSortSplit (key closure): first mismatch at %d: got %q want %q", i, data[i], want[i])
				break
			}
		}
	} else {
		t.Logf("radixSortSplit (key closure): OK")
	}

	// Now test via sortKeyString with same data.
	data2 := slices.Clone(want) // start from sorted, shuffle
	rand.Shuffle(len(data2), func(i, j int) { data2[i], data2[j] = data2[j], data2[i] })
	want2 := slices.Clone(data2)
	slices.Sort(want2)

	sortKeyString(data2, func(s string) string { return s }, nil)
	if !slices.Equal(data2, want2) {
		for i := range data2 {
			if data2[i] != want2[i] {
				t.Errorf("sortKeyString: first mismatch at %d: got %q want %q", i, data2[i], want2[i])
				break
			}
		}
	} else {
		t.Logf("sortKeyString: OK")
	}
}

func TestSortSharedPrefix(t *testing.T) {
	for _, n := range []int{10, 33, 50, 100, 1000, 10000, 100000} {
		prefix := "aaaaaaaaaaaaaaaaaaaaaaaa"
		data := make([]string, n)
		for i := range data {
			data[i] = prefix + randomString(rand.Intn(10)+1)
		}
		want := slices.Clone(data)
		slices.Sort(want)
		Sort(data)
		if !slices.Equal(data, want) {
			t.Errorf("shared prefix n=%d: mismatch", n)
			if n <= 20 {
				t.Logf("got:  %v", data)
				t.Logf("want: %v", want)
			} else {
				// find first mismatch
				for i := range data {
					if data[i] != want[i] {
						t.Logf("first mismatch at %d: got %q want %q", i, data[i], want[i])
						break
					}
				}
			}
		}
	}
}
