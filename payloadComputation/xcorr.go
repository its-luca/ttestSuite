package payloadComputation

import (
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
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
			return 0, fmt.Errorf("overflow")
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

type xCorrJob struct {
	referenceTrace []float64
	trace          []float64
	resPTR         *float64
}

//computeCorrelation, computes correlation between s.pwMeanFixed and all the fixed case traces returned by traceReader
func computeCorrelation(ctx context.Context, workerCount int, refereceTrace []float64, traces [][]float64) ([]float64, error) {

	if len(traces) == 0 {
		return nil, fmt.Errorf("traces is empty")
	}
	//float64 ok as the normalized value is small; DO NOT CHANGE SIZE as we use pointers to
	//individual elements to store results in the  worker function
	normalizedCorrelationValues := make([]float64, len(traces))

	workers, ctx := errgroup.WithContext(ctx)

	//fed by decoder, processed by workers
	jobs := make(chan xCorrJob)

	//spawn workers
	for i := 0; i < workerCount; i++ {
		workers.Go(func() error {
			for j := range jobs {
				corr, err := NormalizedCrossCorrelateAgainstTotal(j.referenceTrace, j.trace)
				if err != nil {
					return fmt.Errorf("failed to calc correlation : %v", err)
				}
				//write back result
				floatCorr, _ := corr.Float64()
				*(j.resPTR) = floatCorr

				/*
					corr, err := NormalizedCrossCorrelateFloat64AgainstTotal(j.referenceTrace,j.trace)
					if err != nil {
						return fmt.Errorf("failed to calc correlation : %v", err)
					}
					*(j.resPTR) = corr
				*/
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
