package payloadComputation

import (
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"math"
	"math/big"
)

type dotProductBigFunc = func(a, b []float64) (*big.Float, error)

//configures which dot product implementation should be used by default in dependent functions
var defaultDotProductBig = dotProductOptBig
var defaultDotProductFloat64 = dotProductFloat64

//dotProductAllBig computes the dot product of a and b. If they have different lengths, or length 0, an error is returned
//Internally all computations are done as big.Float
func dotProductAllBig(a, b []float64) (*big.Float, error) {
	if len(a) != len(b) {
		return nil, fmt.Errorf("slices have different lenghts")
	}
	if len(a) == 0 {
		return nil, fmt.Errorf("one of the inputs is empty")
	}
	prod := big.NewFloat(0)
	tmp := big.NewFloat(0)
	for i := range a {
		tmp.SetFloat64(a[i] * b[i])
		prod.Add(prod, tmp)
	}
	return prod, nil
}

//dotProductOptBig computes the dot product of a and b. If they have different lengths, or length 0, an error is returned
//Internally, summation is done with float64 untill close to maxs/min value. Then result is saved in big.Float.
//Benchmarking shows this is faster than doing everything as big.Float
func dotProductOptBig(a, b []float64) (*big.Float, error) {
	if len(a) != len(b) {
		return nil, fmt.Errorf("slices have different lenghts")
	}
	if len(a) == 0 {
		return nil, fmt.Errorf("one of the inputs is empty")
	}

	//find biggest square
	maxVal := a[0] * b[0]
	minVal := a[0] * b[0]
	var p float64
	for i := range a {
		p = a[i] * b[i]
		if p > maxVal {
			maxVal = p
		}
		if p < minVal {
			minVal = p
		}
	}
	absOfLargestNegAddend := math.Abs(minVal)
	sum := big.NewFloat(0)
	partialSum := float64(0)
	var prod float64
	upperBound := math.MaxFloat64 - maxVal
	lowerBound := -math.MaxFloat64 - absOfLargestNegAddend
	for i := range a {
		prod = a[i] * b[i]
		//check if adding prod could lead to out of bounds, if so, add
		//to float and reset to current value
		if (partialSum < upperBound) && (partialSum > lowerBound) {
			partialSum += prod
		} else {
			sum.Add(sum, big.NewFloat(partialSum))
			partialSum = prod
		}
	}
	sum.Add(sum, big.NewFloat(partialSum))
	return sum, nil
}

//dotProductFloat64, computes the dot product of a and b. If they have different lengths, or length 0, an error is returned.
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
			return 0, fmt.Errorf("overflow")
		} else {
			oldSum = sum
		}
	}

	return sum, nil
}

//NormalizedCrossCorrelateFloat64 implements matlab's xcorr(a,b,0,'normalized')
func NormalizedCrossCorrelateFloat64(a []float64, b []float64) (float64, error) {
	Rab, err := defaultDotProductFloat64(a, b)
	if err != nil {
		return 0, err
	}
	Raa, err := defaultDotProductFloat64(a, a)
	if err != nil {
		return 0, err
	}
	Rbb, err := defaultDotProductFloat64(b, b)
	if err != nil {
		return 0, err
	}
	return Rab / (math.Sqrt(Raa * Rbb)), nil
}

//NormalizedCrossCorrelationBig implements matlab's xcorr(a,b,0,'normalized'). Before normalization
//all computations are done with big.Float value range
func NormalizedCrossCorrelationBig(a []float64, b []float64) (*big.Float, error) {
	Rab, err := defaultDotProductBig(a, b)
	if err != nil {
		return nil, err
	}
	Raa, err := defaultDotProductBig(a, a)
	if err != nil {
		return nil, err
	}
	Rbb, err := defaultDotProductBig(b, b)
	if err != nil {
		return nil, err
	}
	return Rab.Quo(Rab, Raa.Sqrt(Raa.Mul(Raa, Rbb))), nil
}

type xCorrJob struct {
	referenceTrace []float64
	trace          []float64
	resPTR         *float64
}

//ComputeCorrelation computes correlation between refereceTrace and all entries of traces using NormalizedCrossCorrelationBig in
//a parallelized manner
func ComputeCorrelation(ctx context.Context, workerCount int, refereceTrace []float64, traces [][]float64) ([]float64, error) {

	if len(traces) == 0 {
		return nil, fmt.Errorf("traces is empty")
	}
	//float64 ok as the normalized value is small; DO NOT CHANGE SIZE as we use pointers to
	//individual elements to store results in the  worker function
	normalizedCorrelationValues := make([]float64, len(traces))

	workers, ctx := errgroup.WithContext(ctx)

	jobs := make(chan xCorrJob)

	//spawn workers
	for i := 0; i < workerCount; i++ {
		workers.Go(func() error {
			for j := range jobs {
				corr, err := NormalizedCrossCorrelationBig(j.referenceTrace, j.trace)
				if err != nil {
					return fmt.Errorf("failed to calc correlation : %v", err)
				}
				//write result into assigned mem addr
				floatCorr, _ := corr.Float64()
				*(j.resPTR) = floatCorr
			}
			return nil
		})
	}

	//feed jobs
	traceIDX := 0
	for traceIDX < len(traces) {
		select {
		case <-ctx.Done():
			break
		default:
			jobs <- xCorrJob{
				referenceTrace: refereceTrace,
				trace:          traces[traceIDX],
				resPTR:         &(normalizedCorrelationValues[traceIDX]),
			}
			traceIDX++
		}
	}
	close(jobs)

	if err := workers.Wait(); err != nil {
		return nil, fmt.Errorf("error in worker or abort signal from os: %v", err)
	}
	if traceIDX != len(traces) {
		return nil, fmt.Errorf("aborted due to cancelled context")
	}
	return normalizedCorrelationValues, nil
}
