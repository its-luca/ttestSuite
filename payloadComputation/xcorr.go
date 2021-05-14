package payloadComputation

import (
	"fmt"
	"math"
	"math/big"
)

//dotProduct computes the dot product of a and b. If they have different lengths an error is returned
func dotProduct(a, b []float64) (*big.Float, error) {
	if len(a) != len(b) {
		return nil, fmt.Errorf("slices have different lenghts")
	}
	prod := big.NewFloat(0)
	tmp := big.NewFloat(0)
	for i := range a {
		tmp.SetFloat64(a[i] * b[i])
		prod.Add(prod, tmp)
	}
	return prod, nil
}

func dotProductFloat64(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("slices have different lenghts")
	}
	if len(a) == 0 {
		return 0, nil
	}
	sum := a[0] * b[0]
	oldSum := sum
	for i := range a {
		sum += a[i] * b[i]
		if sum < oldSum {
			return 0, fmt.Errorf("overfow")
		} else {
			oldSum = sum
		}
	}

	return sum, nil
}

func NormalizedCrossCorrelateFloat64AgainstTotal(a []float64, b []float64) (float64, error) {
	Rab, err := dotProductFloat64(a, b)
	if err != nil {
		return 0, err
	}
	Raa, err := dotProductFloat64(a, a)
	if err != nil {
		return 0, err
	}
	Rbb, err := dotProductFloat64(b, b)
	if err != nil {
		return 0, err
	}
	return Rab / (math.Sqrt(Raa * Rbb)), nil
}

//NormalizedCrossCorrelateAgainstTotal implements matlab's xcorr(a,b,0,'normalized')
func NormalizedCrossCorrelateAgainstTotal(a []float64, b []float64) (*big.Float, error) {
	Rab, err := dotProduct(a, b)
	if err != nil {
		return nil, err
	}
	Raa, err := dotProduct(a, a)
	if err != nil {
		return nil, err
	}
	Rbb, err := dotProduct(b, b)
	if err != nil {
		return nil, err
	}
	return Rab.Quo(Rab, Raa.Sqrt(Raa.Mul(Raa, Rbb))), nil
}
