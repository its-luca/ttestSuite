package payloadComputation

//Implementation of Welch's TTest as a WorkerPayload

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"ttestSuite/tPlot"
)

var ErrOneSetEmpty = errors.New("cannot compute, at least one of the sets is empty")

type WelchTTest struct {
	lenFixed             float64
	lenRandom            float64
	pwSumFixed           []float64
	pwSumRandom          []float64
	pwSumOfSquaresFixed  []float64
	pwSumOfSquaresRandom []float64
	datapointsPerTrace   int
	fieldsToSave         []interface{}
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

//NewWelchTTest creates a new WelchTTest instance.
//All calls to Update must contains exactly datapointsPerTrace entries per trace otherwise we panic
func NewWelchTTest(datapointsPerTrace int) WorkerPayload {
	bmv := &WelchTTest{
		lenFixed:             float64(0),
		lenRandom:            float64(0),
		pwSumFixed:           make([]float64, datapointsPerTrace),
		pwSumRandom:          make([]float64, datapointsPerTrace),
		pwSumOfSquaresFixed:  make([]float64, datapointsPerTrace),
		pwSumOfSquaresRandom: make([]float64, datapointsPerTrace),
		datapointsPerTrace:   datapointsPerTrace,
	}
	bmv.fieldsToSave = []interface{}{
		&bmv.lenFixed,
		&bmv.lenRandom,
		&bmv.pwSumFixed,
		&bmv.pwSumRandom,
		&bmv.pwSumOfSquaresFixed,
		&bmv.pwSumOfSquaresRandom,
		&bmv.datapointsPerTrace,
	}

	return bmv
}

func (bmv *WelchTTest) MaxSubroutines() int {
	return 4
}

func (bmv *WelchTTest) Name() string {
	return "Welch's T-Test"
}

func (bmv *WelchTTest) Update(fixed, random [][]float64) {
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

func (bmv *WelchTTest) Finalize() ([]float64, error) {
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

func (bmv *WelchTTest) Merge(other WorkerPayload) error {
	otherAsBMV, ok := other.(*WelchTTest)
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

func (bmv *WelchTTest) DeepCopy() WorkerPayload {
	res := &WelchTTest{
		lenFixed:             bmv.lenFixed,
		lenRandom:            bmv.lenRandom,
		pwSumFixed:           make([]float64, bmv.datapointsPerTrace),
		pwSumRandom:          make([]float64, bmv.datapointsPerTrace),
		pwSumOfSquaresFixed:  make([]float64, bmv.datapointsPerTrace),
		pwSumOfSquaresRandom: make([]float64, bmv.datapointsPerTrace),
		datapointsPerTrace:   bmv.datapointsPerTrace,
		fieldsToSave:         bmv.fieldsToSave,
	}
	copy(res.pwSumFixed, bmv.pwSumFixed)
	copy(res.pwSumRandom, bmv.pwSumRandom)
	copy(res.pwSumOfSquaresFixed, bmv.pwSumOfSquaresFixed)
	copy(res.pwSumOfSquaresRandom, bmv.pwSumOfSquaresRandom)
	return res
}

//Encode applies gob to each field of bmv
func (bmv *WelchTTest) Encode(w io.Writer) error {
	encoder := gob.NewEncoder(w)

	for _, v := range bmv.fieldsToSave {
		if err := encoder.Encode(v); err != nil {
			return err
		}
	}
	return nil
}

//Decode decodes a WelchTTest that hase been encoded with Encode
func (bmv *WelchTTest) Decode(r io.Reader) error {
	decoder := gob.NewDecoder(r)
	for _, v := range bmv.fieldsToSave {
		if err := decoder.Decode(v); err != nil {
			return err
		}
	}
	return nil
}

//Plot creates a line plot for values with a trace length adaptive threshold line
//and stores the result in writer. The threshold values are from https://eprint.iacr.org/2017/287.pdf
func (bmv *WelchTTest) Plot(values []float64, writer io.Writer) error {
	//trace length dependent threshold values from https://eprint.iacr.org/2017/287.pdf
	//index 0 trace length 10^2, index 1 trace length 10^3 and so on
	thresholds := []float64{4.417, 4.892, 5.327, 5.731, 6.110, 6.467, 6.806}
	if l := float64(bmv.datapointsPerTrace); l == math.Inf(1) || l == 0 || l == math.NaN() || l < 0 {
		return fmt.Errorf("trace length may not be <0 or +Inf or NaN")
	}
	order := math.Log10(float64(bmv.datapointsPerTrace))
	var threshold float64
	if order < 2 {
		threshold = thresholds[0]
	} else if order > 8 {
		threshold = thresholds[len(thresholds)-1]
	} else {
		threshold = thresholds[int(math.Floor(order-2))]
	}

	return tPlot.PlotAndStore(values, threshold, writer)
}
