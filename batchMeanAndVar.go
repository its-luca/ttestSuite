package main

//Mean and Variances computation for Fixed vs Random T-test

import (
	"fmt"
	"math"
	"sync"
)

type BatchMeanAndVar struct {
	lenFixed             float64
	lenRandom            float64
	pwSumFixed           []float64
	pwSumRandom          []float64
	pwSumOfSquaresFixed  []float64
	pwSumOfSquaresRandom []float64
	datapointsPerTrace   int
}

func minInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}

//updates the point ise sums in sum with the datapoints in traces
func addPwSumFloat64(sum []float64, traces [][]float64) []float64 {
	if len(traces) > 1 {
		if len(traces[0]) != len(sum) {
			panic(fmt.Sprintf("sum has len %v but trace has length %v", len(sum), len(traces[0])))
		}
	}
	for traceIDX := range traces {
		for pointIDX := range traces[traceIDX] {
			sum[pointIDX] += traces[traceIDX][pointIDX]
		}
	}
	return sum
}

//updates the point ise sums in sum with the squares of the datapoints in traces
func addPwSumOfSquaresFloat64(sum []float64, traces [][]float64) []float64 {
	if len(traces) > 1 {
		if len(traces[0]) != len(sum) {
			panic(fmt.Sprintf("sum has len %v but trace has length %v", len(sum), len(traces[0])))
		}
	}
	for traceIDX := range traces {
		for pointIDX := range traces[traceIDX] {
			sum[pointIDX] += math.Pow(traces[traceIDX][pointIDX], 2)
		}
	}
	return sum
}

func NewBatchMeanAndVar(datapointsPerTrace int) *BatchMeanAndVar {

	bmv := &BatchMeanAndVar{
		lenFixed:             float64(0),
		lenRandom:            float64(0),
		pwSumFixed:           make([]float64, datapointsPerTrace),
		pwSumRandom:          make([]float64, datapointsPerTrace),
		pwSumOfSquaresFixed:  make([]float64, datapointsPerTrace),
		pwSumOfSquaresRandom: make([]float64, datapointsPerTrace),
		datapointsPerTrace:   datapointsPerTrace,
	}

	return bmv
}

func (bmv *BatchMeanAndVar) Update(fixed, random [][]float64) {
	bmv.lenFixed += float64(len(fixed))
	bmv.lenRandom += float64(len(random))

	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		bmv.pwSumFixed = addPwSumFloat64(bmv.pwSumFixed, fixed)
	}()
	go func() {
		defer wg.Done()
		bmv.pwSumRandom = addPwSumFloat64(bmv.pwSumRandom, random)
	}()
	go func() {
		defer wg.Done()
		bmv.pwSumOfSquaresFixed = addPwSumOfSquaresFloat64(bmv.pwSumOfSquaresFixed, fixed)
	}()
	go func() {
		defer wg.Done()
		bmv.pwSumOfSquaresRandom = addPwSumOfSquaresFloat64(bmv.pwSumOfSquaresRandom, random)
	}()

	wg.Wait()
}

//calculate t test value for each point
func (bmv *BatchMeanAndVar) ComputeLQ() []float64 {
	//calc pw means; we assured that batch array are of same length

	pwMeanFixed := make([]float64, bmv.datapointsPerTrace)
	pwMeanRandom := make([]float64, bmv.datapointsPerTrace)
	pwMeanSquaredFixed := make([]float64, bmv.datapointsPerTrace)
	pwMeanSquaredRandom := make([]float64, bmv.datapointsPerTrace)
	for i := 0; i < bmv.datapointsPerTrace; i++ {
		pwMeanFixed[i] = bmv.pwSumFixed[i] / bmv.lenFixed
		pwMeanRandom[i] = bmv.pwSumRandom[i] / bmv.lenRandom

		pwMeanSquaredFixed[i] = bmv.pwSumOfSquaresFixed[i] / bmv.lenFixed
		pwMeanSquaredRandom[i] = bmv.pwSumOfSquaresRandom[i] / bmv.lenRandom
	}

	//calc pw var
	pwVarFixed := make([]float64, bmv.datapointsPerTrace)
	pwVarRandom := make([]float64, bmv.datapointsPerTrace)
	for i := 0; i < bmv.datapointsPerTrace; i++ {
		pwVarFixed[i] = pwMeanSquaredFixed[i] - math.Pow(pwMeanFixed[i], 2)
		pwVarRandom[i] = pwMeanSquaredRandom[i] - math.Pow(pwMeanRandom[i], 2)
	}

	//??
	denom := make([]float64, bmv.datapointsPerTrace)
	for i := 0; i < bmv.datapointsPerTrace; i++ {
		tmpFixed := pwVarFixed[i] / bmv.lenFixed
		tmpRandom := pwVarRandom[i] / bmv.lenRandom
		denom[i] = math.Sqrt(tmpFixed + tmpRandom)
	}

	tValues := make([]float64, bmv.datapointsPerTrace)
	for i := 0; i < bmv.datapointsPerTrace; i++ {
		tValues[i] = (pwMeanFixed[i] - pwMeanRandom[i]) / denom[i]
	}

	return tValues

}

//merges other into bmv
func (bmv *BatchMeanAndVar) MergeBatchMeanAndVar(other *BatchMeanAndVar) {
	bmv.lenFixed += other.lenFixed
	bmv.lenRandom += other.lenRandom

	for i := 0; i < bmv.datapointsPerTrace; i++ {
		bmv.pwSumOfSquaresRandom[i] += other.pwSumOfSquaresRandom[i]
		bmv.pwSumOfSquaresFixed[i] += other.pwSumOfSquaresFixed[i]

		bmv.pwSumRandom[i] += other.pwSumRandom[i]
		bmv.pwSumFixed[i] += other.pwSumFixed[i]
	}

}
