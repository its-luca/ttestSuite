package payloadComputation

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
)

func Test_EncodeDecode(t *testing.T) {

	//fill object with some state
	datapointsPerTrace := 100
	bmv := NewWelchTTest(datapointsPerTrace)
	dataFixed := make([][]float64, 0)
	dataRandom := make([][]float64, 0)
	for i := 0; i < 10; i++ {
		dataFixed = append(dataFixed, make([]float64, 0))
		dataRandom = append(dataRandom, make([]float64, 0))
		for j := 0; j < datapointsPerTrace; j++ {
			dataFixed[i] = append(dataFixed[i], rand.Float64())
			dataRandom[i] = append(dataRandom[i], rand.Float64())
		}
	}
	bmv.Update(dataFixed, dataRandom)
	want, err := bmv.Finalize()
	if err != nil {
		t.Fatalf("Unexpected error creating want :%v", err)
	}

	//encode into buf and decode into new struct
	encodeBuffer := &bytes.Buffer{}
	if err := bmv.Encode(encodeBuffer); err != nil {
		t.Fatalf("Unexpected encode error : %v", err)
	}

	restoredBMV := NewWelchTTest(datapointsPerTrace)
	if err := restoredBMV.Decode(encodeBuffer); err != nil {
		t.Fatalf("Unexpected decode error : %v", err)
	}

	//check state of decoded struct

	got, err := restoredBMV.Finalize()
	if err != nil {
		t.Fatalf("Unexpected error finalizing decoded value :%v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("wanted %v got %v", want, got)
	}

}
