package payloadComputation

//Implementation of Welch's TTest as a WorkerPayload

import (
	"encoding/csv"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"reflect"
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
}

func (bmv *WelchTTest) genFieldToSaveSlice() []interface{} {
	return []interface{}{
		&bmv.lenFixed,
		&bmv.lenRandom,
		&bmv.pwSumFixed,
		&bmv.pwSumRandom,
		&bmv.pwSumOfSquaresFixed,
		&bmv.pwSumOfSquaresRandom,
		&bmv.datapointsPerTrace,
	}
}

func (bmv *WelchTTest) Reset() {
	bmv.lenFixed = 0
	bmv.lenRandom = 0
	for i := range bmv.pwSumFixed {
		bmv.pwSumFixed[i] = 0
	}
	for i := range bmv.pwSumRandom {
		bmv.pwSumRandom[i] = 0
	}
	for i := range bmv.pwSumOfSquaresFixed {
		bmv.pwSumOfSquaresFixed[i] = 0
	}
	for i := range bmv.pwSumOfSquaresRandom {
		bmv.pwSumOfSquaresRandom[i] = 0
	}
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

func addPwSumAndSumSQFloat64Mul(sum, sumSQ []float64, traces [][]float64) ([]float64, []float64) {
	if len(traces) > 1 {
		if len(traces[0]) != len(sum) {
			panic(fmt.Sprintf("sum has len %v but trace has length %v", len(sum), len(traces[0])))
		}
	}
	for traceIDX := range traces {
		for pointIDX := range traces[traceIDX] {
			sum[pointIDX] += traces[traceIDX][pointIDX]
			sumSQ[pointIDX] += traces[traceIDX][pointIDX] * traces[traceIDX][pointIDX]
		}
	}
	return sum, sumSQ
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
	wg.Add(2)
	go func() {
		defer wg.Done()
		bmv.pwSumFixed, bmv.pwSumOfSquaresFixed = addPwSumAndSumSQFloat64Mul(bmv.pwSumFixed, bmv.pwSumOfSquaresFixed, fixed)
	}()
	go func() {
		defer wg.Done()
		bmv.pwSumRandom, bmv.pwSumOfSquaresRandom = addPwSumAndSumSQFloat64Mul(bmv.pwSumRandom, bmv.pwSumOfSquaresRandom, random)
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

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < bmv.datapointsPerTrace; i++ {
			bmv.pwSumOfSquaresRandom[i] += otherAsBMV.pwSumOfSquaresRandom[i]
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < bmv.datapointsPerTrace; i++ {
			bmv.pwSumOfSquaresFixed[i] += otherAsBMV.pwSumOfSquaresFixed[i]
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < bmv.datapointsPerTrace; i++ {
			bmv.pwSumRandom[i] += otherAsBMV.pwSumRandom[i]
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < bmv.datapointsPerTrace; i++ {

			bmv.pwSumFixed[i] += otherAsBMV.pwSumFixed[i]
		}
	}()

	wg.Wait()
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
	}
	copy(res.pwSumFixed, bmv.pwSumFixed)
	copy(res.pwSumRandom, bmv.pwSumRandom)
	copy(res.pwSumOfSquaresFixed, bmv.pwSumOfSquaresFixed)
	copy(res.pwSumOfSquaresRandom, bmv.pwSumOfSquaresRandom)
	return res
}

//just temporary until snapshot and resume branch is done
func storeAsCSV(v []float64, w io.Writer) error {
	//convert slice to strings
	vAsString := make([]string, len(v))
	for i := range v {
		vAsString[i] = fmt.Sprintf("%f", v[i])
	}

	csvWriter := csv.NewWriter(w)
	if err := csvWriter.Write(vAsString); err != nil {
		return fmt.Errorf("csv write failed: %v", err)
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return fmt.Errorf("csv write failed: %v", err)
	}
	return nil
}

func (bmv *WelchTTest) WriteToCSV(w io.Writer) error {
	for _, v := range bmv.genFieldToSaveSlice() {
		if floatV, ok := v.(*float64); ok {
			if err := storeAsCSV([]float64{*floatV}, w); err != nil {
				return err
			}
		} else if sliceV, ok := v.(*[]float64); ok {
			if err := storeAsCSV(*sliceV, w); err != nil {
				return err
			}
		} else if intV, ok := v.(*int); ok {
			if err := storeAsCSV([]float64{float64(*intV)}, w); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("encountered field unexpected type %v", reflect.TypeOf(v))
		}
	}
	log.Printf("len fixed : %v", bmv.lenFixed)
	log.Printf("First pwSumFixed entries are: %v", bmv.pwSumFixed[:10])
	return nil
}

//Encode applies gob to each field of bmv
func (bmv *WelchTTest) Encode(w io.Writer) error {
	encoder := gob.NewEncoder(w)

	for _, v := range bmv.genFieldToSaveSlice() {
		if err := encoder.Encode(v); err != nil {
			return err
		}
	}
	return nil
}

//Decode decodes a WelchTTest that hase been encoded with Encode
func (bmv *WelchTTest) Decode(r io.Reader) error {
	decoder := gob.NewDecoder(r)
	for _, v := range bmv.genFieldToSaveSlice() {
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
