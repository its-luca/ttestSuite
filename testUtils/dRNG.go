package testUtils

import "math/rand"

//DRNGFloat64Slice wraps DRNGFloat64SliceCustomScale with scaleFactor set to 1000
func DRNGFloat64Slice(length int, seed int64) []float64 {
	return DRNGFloat64SliceCustomScale(length, seed, 1000)
}

//DRNGFloat64SliceCustomScale returns a slice of length entries with pseudo random values from -scaleFactor to scaleFactor
//Calling with the same seed will yield the same sequence. Intended to generate large test data sets
func DRNGFloat64SliceCustomScale(length int, seed int64, scaleFactor float64) []float64 {
	dRNGSource := rand.NewSource(seed)
	dRNG := rand.New(dRNGSource)
	buf := make([]float64, length)
	for i := 0; i < length; i++ {
		sign := dRNG.Float32()
		buf[i] = dRNG.Float64() * scaleFactor
		if sign <= 0.5 {
			buf[i] *= -1
		}
	}
	return buf
}
