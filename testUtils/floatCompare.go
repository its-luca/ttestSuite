package testUtils

import (
	"math"
	"math/big"
)

//FloatEqUpTo returns true if abs(a-b)<=maxDiff
func FloatEqUpTo(a, b, maxDiff float64) bool {
	return math.Abs(a-b) <= maxDiff
}

//FloatSliceEqUpTo returns true if FloatEqUpTo(a[i],b[i],maxDiff) holds for all elements
func FloatSliceEqUpTo(a, b []float64, maxDiff float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !FloatEqUpTo(a[i], b[i], maxDiff) {
			return false
		}
	}
	return true
}

//BigFloatEqUpTo returns true if abs(a-b)<=maxDiff
func BigFloatEqUpTo(a, b, maxDiff *big.Float) bool {
	c := big.NewFloat(0)
	//compute abs difference
	if a.Cmp(b) == 1 {
		c.Sub(a, b)
	} else {
		c.Sub(b, a)
	}
	//true if c <= maxDiff
	return 0 >= c.Cmp(maxDiff)
}
