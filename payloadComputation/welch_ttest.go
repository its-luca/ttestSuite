package payloadComputation

//Implementation of Welch's TTest as a WorkerPayload

import (
	"errors"
	"fmt"
	"math"
	"sync"
)

var ErrOneSetEmpty = errors.New("cannot compute, at least one of the sets is empty")

type WelchTTtest struct {
	lenFixed             float64
	lenRandom            float64
	pwSumFixed           []float64
	pwSumRandom          []float64
	pwSumOfSquaresFixed  []float64
	pwSumOfSquaresRandom []float64
	datapointsPerTrace   int
}

//addPwSumFloat64 point wise adds all traces to sum
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

//addPwSumOfSquaresFloat64 point wise adds the square of all trace datapoints to sum
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

//NewBatchMeanAndVar creates a new WelchTTtest instance.
//All calls to Update must contains exactly datapointsPerTrace entries per trace otherwise we panic
func NewBatchMeanAndVar(datapointsPerTrace int) WorkerPayload {
	bmv := &WelchTTtest{
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

func (bmv *WelchTTtest) MaxSubroutines() int {
	return 4
}

func (bmv *WelchTTtest) Name() string {
	return "Welch's T-Test"
}

func (bmv *WelchTTtest) Update(fixed, random [][]float64) {
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

func (bmv *WelchTTtest) Finalize() ([]float64, error) {
	if bmv.lenRandom == 0 || bmv.lenFixed == 0 {
		return nil, ErrOneSetEmpty
	}
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
	denominator := make([]float64, bmv.datapointsPerTrace)
	for i := 0; i < bmv.datapointsPerTrace; i++ {
		tmpFixed := pwVarFixed[i] / bmv.lenFixed
		tmpRandom := pwVarRandom[i] / bmv.lenRandom
		denominator[i] = math.Sqrt(tmpFixed + tmpRandom)
	}

	tValues := make([]float64, bmv.datapointsPerTrace)
	for i := 0; i < bmv.datapointsPerTrace; i++ {
		tValues[i] = (pwMeanFixed[i] - pwMeanRandom[i]) / denominator[i]
	}

	return tValues, nil

}

func (bmv *WelchTTtest) Merge(other WorkerPayload) error {
	otherAsBMV, ok := other.(*WelchTTtest)
	if !ok {
		return fmt.Errorf("cannot merge %s with %s", bmv.Name(), other.Name())
	}
	bmv.lenFixed += otherAsBMV.lenFixed
	bmv.lenRandom += otherAsBMV.lenRandom

	for i := 0; i < bmv.datapointsPerTrace; i++ {
		bmv.pwSumOfSquaresRandom[i] += otherAsBMV.pwSumOfSquaresRandom[i]
		bmv.pwSumOfSquaresFixed[i] += otherAsBMV.pwSumOfSquaresFixed[i]

		bmv.pwSumRandom[i] += otherAsBMV.pwSumRandom[i]
		bmv.pwSumFixed[i] += otherAsBMV.pwSumFixed[i]
	}
	return nil
}

func (bmv *WelchTTtest) DeepCopy() WorkerPayload {
	res := &WelchTTtest{
		lenFixed:             bmv.lenFixed,
		lenRandom:            bmv.lenRandom,
		pwSumFixed:           make([]float64, bmv.datapointsPerTrace),
		pwSumRandom:          make([]float64, bmv.datapointsPerTrace),
		pwSumOfSquaresFixed:  make([]float64, bmv.datapointsPerTrace),
		pwSumOfSquaresRandom: make([]float64, bmv.datapointsPerTrace),
		datapointsPerTrace:   bmv.datapointsPerTrace,
	}
	copy(res.pwSumFixed, bmv.pwSumFixed)
	copy(res.pwSumRandom, bmv.pwSumRandom)
	copy(res.pwSumOfSquaresFixed, bmv.pwSumOfSquaresFixed)
	copy(res.pwSumOfSquaresRandom, bmv.pwSumOfSquaresRandom)
	return res
}
