package psort

import (
	"cmp"
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// --- SortKey basic correctness ---

func TestSortKeyStrings(t *testing.T) {
	n := 100_000
	if testing.Short() {
		n /= 100
	}
	data := make([]string, n)
	for i := range data {
		data[i] = randomString(rand.Intn(50) + 1)
	}
	want := slices.Clone(data)
	slices.Sort(want)

	SortKey(data, func(s string) string { return s })
	if !slices.Equal(data, want) {
		t.Errorf("SortKey string identity: result differs from slices.Sort")
	}
}

func TestSortKeyBytes(t *testing.T) {
	n := 100_000
	if testing.Short() {
		n /= 100
	}
	data := make([][]byte, n)
	for i := range data {
		data[i] = []byte(randomString(rand.Intn(50) + 1))
	}
	want := slices.Clone(data)
	slices.SortFunc(want, func(a, b []byte) int { return strings.Compare(string(a), string(b)) })

	SortKeyBytes(data, func(b []byte) []byte { return b })
	for i := range data {
		if string(data[i]) != string(want[i]) {
			t.Fatalf("SortKey []byte: mismatch at index %d", i)
		}
	}
}

func TestSortBytes(t *testing.T) {
	n := 100_000
	if testing.Short() {
		n /= 100
	}
	data := make([][]byte, n)
	for i := range data {
		data[i] = []byte(randomString(rand.Intn(50) + 1))
	}
	want := slices.Clone(data)
	slices.SortFunc(want, func(a, b []byte) int { return strings.Compare(string(a), string(b)) })

	SortBytes(data)
	for i := range data {
		if string(data[i]) != string(want[i]) {
			t.Fatalf("SortBytes: mismatch at index %d", i)
		}
	}
}

func TestSortKeyInts(t *testing.T) {
	n := 100_000
	if testing.Short() {
		n /= 100
	}
	data := make([]int, n)
	for i := range data {
		data[i] = rand.Intn(n)
	}
	want := slices.Clone(data)
	slices.Sort(want)

	SortKey(data, func(x int) int { return x })
	if !slices.Equal(data, want) {
		t.Errorf("SortKey int identity: result differs from slices.Sort")
	}
}

// --- SortKey with struct + key extraction ---

type person struct {
	name string
	age  int
}

func TestSortKeyStructByString(t *testing.T) {
	n := 50_000
	if testing.Short() {
		n /= 100
	}
	data := make([]person, n)
	for i := range data {
		data[i] = person{name: randomString(rand.Intn(20) + 1), age: rand.Intn(100)}
	}

	SortKey(data, func(p person) string { return p.name })

	if !slices.IsSortedFunc(data, func(a, b person) int {
		return strings.Compare(a.name, b.name)
	}) {
		t.Errorf("not sorted by name")
	}
}

func TestSortKeyStructByInt(t *testing.T) {
	n := 50_000
	if testing.Short() {
		n /= 100
	}
	data := make([]person, n)
	for i := range data {
		data[i] = person{name: randomString(10), age: rand.Intn(100)}
	}

	SortKey(data, func(p person) int { return p.age })
	if !slices.IsSortedFunc(data, func(a, b person) int {
		return cmp.Compare(a.age, b.age)
	}) {
		t.Errorf("not sorted by age")
	}
}

// --- Tiebreaker ---

func TestSortKeyThen(t *testing.T) {
	n := 50_000
	if testing.Short() {
		n /= 100
	}
	data := make([]person, n)
	for i := range data {
		data[i] = person{
			name: randomString(rand.Intn(5) + 1), // few unique names to force ties
			age:  rand.Intn(100),
		}
	}

	cmpFn := func(a, b person) int {
		if d := strings.Compare(a.name, b.name); d != 0 {
			return d
		}
		return cmp.Compare(a.age, b.age)
	}
	want := slices.Clone(data)
	slices.SortFunc(want, cmpFn)

	sortKeyThen(data, func(p person) string { return p.name },
		func(a, b person) int { return cmp.Compare(a.age, b.age) })

	if !slices.EqualFunc(data, want, func(a, b person) bool {
		return a.name == b.name && a.age == b.age
	}) {
		t.Errorf("tiebreaker: result differs from expected")
	}
}

func TestSortKeyBytesThen(t *testing.T) {
	type rec struct {
		key   []byte
		value int
	}
	n := 50_000
	if testing.Short() {
		n /= 100
	}
	data := make([]rec, n)
	for i := range data {
		data[i] = rec{
			key:   []byte(randomString(rand.Intn(5) + 1)),
			value: rand.Intn(100),
		}
	}

	cmpFn := func(a, b rec) int {
		if d := strings.Compare(string(a.key), string(b.key)); d != 0 {
			return d
		}
		return cmp.Compare(a.value, b.value)
	}
	want := slices.Clone(data)
	slices.SortFunc(want, cmpFn)

	sortKeyBytesThen(data, func(r rec) []byte { return r.key },
		func(a, b rec) int { return cmp.Compare(a.value, b.value) })

	if !slices.EqualFunc(data, want, func(a, b rec) bool {
		return string(a.key) == string(b.key) && a.value == b.value
	}) {
		t.Errorf("tiebreaker bytes: result differs from expected")
	}
}

// --- Edge cases ---

func TestSortKeyEmpty(t *testing.T) {
	SortKey([]string(nil), func(s string) string { return s })
	SortKey([]string{}, func(s string) string { return s })
}

func TestSortKeySmall(t *testing.T) {
	// Below minParallel — exercises the serial fallback.
	for _, n := range []int{1, 2, 10, 50, minParallel - 1} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			data := make([]string, n)
			for i := range data {
				data[i] = randomString(rand.Intn(20) + 1)
			}
			want := slices.Clone(data)
			slices.Sort(want)
			SortKey(data, func(s string) string { return s })
			if !slices.Equal(data, want) {
				t.Errorf("small sort mismatch at n=%d", n)
			}
		})
	}
}

func TestSortKeyShortStrings(t *testing.T) {
	// Strings shorter than 8 bytes — abbreviated key contains the full content.
	n := 50_000
	if testing.Short() {
		n /= 100
	}
	data := make([]string, n)
	for i := range data {
		data[i] = randomString(rand.Intn(7) + 1) // 1–7 bytes
	}
	want := slices.Clone(data)
	slices.Sort(want)

	SortKey(data, func(s string) string { return s })
	if !slices.Equal(data, want) {
		t.Errorf("short strings: mismatch")
	}
}

func TestSortKeySharedPrefix(t *testing.T) {
	// Strings that share a long prefix — abbreviated keys will tie,
	// forcing fallback to full string comparison.
	n := 50_000
	if testing.Short() {
		n /= 100
	}
	prefix := "aaaaaaaaaaaaaaaaaaaaaaaa" // 24 chars, well past 8-byte abbrev
	data := make([]string, n)
	for i := range data {
		data[i] = prefix + randomString(rand.Intn(10)+1)
	}
	want := slices.Clone(data)
	slices.Sort(want)

	SortKey(data, func(s string) string { return s })
	if !slices.Equal(data, want) {
		t.Errorf("shared prefix: mismatch")
	}
}

// --- GOMAXPROCS variation ---

func TestSortKeyGOMAXPROCS(t *testing.T) {
	n := 100_000
	for _, nproc := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("nproc=%d", nproc), func(t *testing.T) {
			data := make([]string, n)
			for i := range data {
				data[i] = randomString(rand.Intn(30) + 1)
			}
			want := slices.Clone(data)
			slices.Sort(want)

			old := runtime.GOMAXPROCS(nproc)
			SortKey(data, func(s string) string { return s })
			runtime.GOMAXPROCS(old)

			if !slices.Equal(data, want) {
				t.Errorf("GOMAXPROCS=%d: mismatch", nproc)
			}
		})
	}
}

// --- Benchmarks ---

func BenchmarkSortKeyStrings(b *testing.B) {
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
				SortKey(data, func(s string) string { return s })
			}
		})
	}
}

func BenchmarkSortKeyStructByName(b *testing.B) {
	restoreMinParallel(b)
	for _, size := range []int{1 << 10, 1 << 16, 1 << 20} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			unsorted := make([]person, size)
			for i := range unsorted {
				unsorted[i] = person{name: randomString(20), age: rand.Intn(100)}
			}
			data := make([]person, size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(data, unsorted)
				SortKey(data, func(p person) string { return p.name })
			}
		})
	}
}
