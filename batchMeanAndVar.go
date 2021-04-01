package main

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
}

//returns the sum of the entries
func sumFloat64(s []float64) float64 {
	sum := float64(0)
	for i, _ := range s {
		sum += s[i]
	}
	return sum
}

//updates the point ise sums in sum with the datapoints in traces
func addPwSumFloat64(sum []float64, traces [][]float64) []float64 {
	if len(traces) > 1 {
		if len(traces[0]) != len(sum) {
			panic(fmt.Sprintf("sum has len %v but trace has length %v", len(sum), len(traces[0])))
		}
	}

	for traceIDX, _ := range traces {

		for pointIDX, _ := range traces[traceIDX] {
			sum[pointIDX] += traces[traceIDX][pointIDX]
		}

	}
	return sum
}

//returns the sum of the squares of each entry
func sumOfSquaresFloat64(s []float64) float64 {
	sum := float64(0)
	for i, _ := range s {
		sum += math.Pow(s[i], 2)
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
	for traceIDX, _ := range traces {
		for pointIDX, _ := range traces[traceIDX] {
			sum[pointIDX] += math.Pow(traces[traceIDX][pointIDX], 2)
		}
	}
	return sum
}

func NewBatchMeanAndVar(fixed, random [][]float64) (*BatchMeanAndVar, error) {
	if len(fixed) == 0 || len(random) == 0 {
		return nil, fmt.Errorf("fixed or random set is empty")
	}
	if len(fixed[0]) != len(random[1]) {
		return nil, fmt.Errorf("number of datpoints per trace in fixed is %v but in random it's %v\n",
			len(fixed[0]), len(random[0]))
	}

	pwSumFixed := make([]float64, len(fixed[0]))
	pwSumSquaredFixed := make([]float64, len(fixed[0]))
	pwSumRandom := make([]float64, len(random[0]))
	pwSumSquaredRandom := make([]float64, len(random[0]))

	bmv := &BatchMeanAndVar{
		lenFixed:             float64(len(fixed)),
		lenRandom:            float64(len(random)),
		pwSumFixed:           addPwSumFloat64(pwSumFixed, fixed),
		pwSumRandom:          addPwSumFloat64(pwSumRandom, random),
		pwSumOfSquaresFixed:  addPwSumOfSquaresFloat64(pwSumSquaredFixed, fixed),
		pwSumOfSquaresRandom: addPwSumOfSquaresFloat64(pwSumSquaredRandom, random),
	}
	return bmv, nil
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
	traceLen := len(bmv.pwSumRandom)
	pwMeanFixed := make([]float64, traceLen)
	pwMeanRandom := make([]float64, traceLen)
	pwMeanSquaredFixed := make([]float64, traceLen)
	pwMeanSquaredRandom := make([]float64, traceLen)
	for i := 0; i < traceLen; i++ {
		pwMeanFixed[i] = bmv.pwSumFixed[i] / bmv.lenFixed
		pwMeanRandom[i] = bmv.pwSumRandom[i] / bmv.lenRandom

		pwMeanSquaredFixed[i] = bmv.pwSumOfSquaresFixed[i] / bmv.lenFixed
		pwMeanSquaredRandom[i] = bmv.pwSumOfSquaresRandom[i] / bmv.lenRandom
	}

	//calc pw var
	pwVarFixed := make([]float64, traceLen)
	pwVarRandom := make([]float64, traceLen)
	for i := 0; i < traceLen; i++ {
		pwVarFixed[i] = pwMeanSquaredFixed[i] - math.Pow(pwMeanFixed[i], 2)
		pwVarRandom[i] = pwMeanSquaredRandom[i] - math.Pow(pwMeanRandom[i], 2)
	}

	//??
	denom := make([]float64, traceLen)
	for i := 0; i < traceLen; i++ {
		tmpFixed := pwVarFixed[i] / bmv.lenFixed
		tmpRandom := pwVarRandom[i] / bmv.lenRandom
		denom[i] = math.Sqrt(tmpFixed + tmpRandom)
	}

	tValues := make([]float64, traceLen)
	for i := 0; i < traceLen; i++ {
		tValues[i] = (pwMeanFixed[i] - pwMeanRandom[i]) / denom[i]
	}

	return tValues

}
