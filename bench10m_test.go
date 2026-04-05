package psort

import (
	"math/rand"
	"slices"
	"strings"
	"testing"
)

func BenchmarkInts10M(b *testing.B) {
	restoreMinParallel(b)
	const size = 10_000_000
	unsorted := make([]int, size)
	for i := range unsorted {
		unsorted[i] = rand.Intn(size)
	}
	data := make([]int, size)

	b.Run("Sort", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copy(data, unsorted)
			Sort(data)
		}
	})

	b.Run("StdlibSort", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copy(data, unsorted)
			slices.Sort(data)
		}
	})
}

func BenchmarkStrings10M(b *testing.B) {
	restoreMinParallel(b)
	const size = 10_000_000
	unsorted := make([]string, size)
	for i := range unsorted {
		buf := make([]byte, 20)
		for j := range buf {
			buf[j] = byte('a' + rand.Intn(26))
		}
		unsorted[i] = string(buf)
	}
	data := make([]string, size)

	b.Run("Sort", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copy(data, unsorted)
			Sort(data)
		}
	})

	b.Run("SortInPlace", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copy(data, unsorted)
			SortInPlace(data)
		}
	})

	b.Run("SortFunc", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copy(data, unsorted)
			SortFunc(data, strings.Compare)
		}
	})

	b.Run("SortKey", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copy(data, unsorted)
			SortKey(data, func(s string) string { return s })
		}
	})

	b.Run("StdlibSort", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copy(data, unsorted)
			slices.Sort(data)
		}
	})
}
