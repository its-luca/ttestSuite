package payloadComputation

import (
	"fmt"
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
