package psort

import (
	"math"
	"math/rand"
	"slices"
	"testing"
)

// TestSortFloat64WithNaN verifies correctness when data contains NaN.
// cmp.Less treats NaN as less than all non-NaN values, so the partition
// must still produce a valid ordering under that total order.
func TestSortFloat64WithNaN(t *testing.T) {
	for _, n := range []int{minParallel, 10000, 100000} {
		t.Run("Sort", func(t *testing.T) {
			data := make([]float64, n)
			for i := range data {
				switch rand.Intn(10) {
				case 0:
					data[i] = math.NaN()
				case 1:
					data[i] = math.Inf(1)
				case 2:
					data[i] = math.Inf(-1)
				case 3:
					data[i] = -0.0
				default:
					data[i] = rand.Float64()*200 - 100
				}
			}
			Sort(data)
			if !slices.IsSorted(data) {
				t.Errorf("float64 with NaN not sorted (n=%d)", n)
			}
		})
	}
}
