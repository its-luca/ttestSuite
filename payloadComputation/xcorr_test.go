package payloadComputation

import (
	"context"
	"fmt"
	"github.com/its-luca/ttestSuite/testUtils"
	"math/big"
	"math/rand"
	"testing"
)

func setupBench() ([]float64, []float64) {
	const size = 9 * 10000000
	a := make([]float64, size)
	b := make([]float64, size)
	for i := 0; i < size; i++ {
		a[i] = rand.Float64() * 10.0
		b[i] = rand.Float64() * 10.0
	}
	return a, b
}

func TestDotProductBigImplementations(t *testing.T) {
	tests := []struct {
		name    string
		a       []float64
		b       []float64
		want    *big.Float
		wantErr bool
	}{
		{
			name: "Simple",
			a:    []float64{1, 2, 2},
			b:    []float64{-1, 2, -2},
			want: big.NewFloat(-1),
		},
		{
			name:    "Empty",
			a:       []float64{},
			b:       []float64{},
			wantErr: true,
		},
		{
			name: "Long against matlab result",
			a:    testUtils.DRNGFloat64Slice(100000, 45),
			b:    testUtils.DRNGFloat64Slice(100000, 46),
			want: big.NewFloat(119293541.9327),
		},
	}

	implementattions := []struct {
		name           string
		dotProductImpl dotProductBigFunc
	}{
		{
			name:           "dotProductAllBig",
			dotProductImpl: dotProductAllBig,
		},
		{
			name:           "dotProductOptBig",
			dotProductImpl: dotProductOptBig,
		},
	}

	for _, bm := range tests {
		for _, impl := range implementattions {
			t.Run(fmt.Sprintf("%v%v", bm.name, impl.name), func(t *testing.T) {
				got, err := impl.dotProductImpl(bm.a, bm.b)
				if (err != nil) != bm.wantErr {
					t.Errorf("%v() error = %v, wantErr %v", impl.name, err, bm.wantErr)
					return
				}
				if err == nil && !testUtils.BigFloatEqUpTo(got, bm.want, big.NewFloat(0.05)) {
					t.Errorf("wanted %v got %v", bm.want, got)
				}
			})
		}
	}
}

func BenchmarkDotProductBigImplementations(b *testing.B) {
	benchmarks := []struct {
		name           string
		dotProductImpl dotProductBigFunc
	}{
		{
			name:           "dotProductAllBig",
			dotProductImpl: dotProductAllBig,
		},
		{
			name:           "dotProductOptBig",
			dotProductImpl: dotProductOptBig,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			s1, s2 := setupBench()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := bm.dotProductImpl(s1, s2); err != nil {
					b.Errorf("Unexpected error : %v", err)
				}
			}
		})
	}
}

func TestNormalizedCrossCorrelationBig(t *testing.T) {
	type args struct {
		a []float64
		b []float64
	}
	tests := []struct {
		name    string
		args    args
		want    float64
		wantErr bool
	}{
		{
			name: "small difference",
			args: struct {
				a []float64
				b []float64
			}{
				a: []float64{1, 2, 3, 4},
				b: []float64{1.2, 2, 3.1, 4.2},
			},
			want:    0.9995, //result of xcorr(A,B,0,'normalized') in matlab
			wantErr: false,
		},
		{
			name: "small equal",
			args: struct {
				a []float64
				b []float64
			}{
				a: []float64{1, 2, 3, 4},
				b: []float64{1, 2, 3, 4},
			},
			want: 1, wantErr: false,
		},
		{
			name: "medium different",
			args: struct {
				a []float64
				b []float64
			}{
				a: []float64{0.489252638400019, 0.337719409821377, 0.900053846417662, 0.369246781120215, 0.111202755293787, 0.780252068321138, 0.389738836961253, 0.241691285913833, 0.403912145588115, 0.0964545251683886},
				b: []float64{0.939001561999887, 0.875942811492984, 0.550156342898422, 0.622475086001228, 0.587044704531417, 0.207742292733028, 0.301246330279491, 0.470923348517591, 0.230488160211559, 0.844308792695389},
			},
			want: 0.7138, wantErr: false,
		},
		{
			name: "large different",
			args: struct {
				a []float64
				b []float64
			}{
				a: []float64{0.489252638400019, 0.337719409821377, 0.900053846417662, 0.369246781120215, 0.111202755293787, 0.780252068321138, 0.389738836961253, 0.241691285913833, 0.403912145588115, 0.0964545251683886, 0.131973292606335, 0.942050590775485, 0.956134540229802, 0.575208595078466, 0.0597795429471558, 0.234779913372406, 0.353158571222071, 0.821194040197959, 0.0154034376515551, 0.0430238016578078, 0.168990029462704, 0.649115474956452, 0.731722385658670, 0.647745963136307, 0.450923706430945, 0.547008892286345, 0.296320805607773, 0.744692807074156, 0.188955015032545, 0.686775433365315, 0.183511155737270, 0.368484596490337, 0.625618560729690, 0.780227435151377, 0.0811257688657853, 0.929385970968730, 0.775712678608402, 0.486791632403172, 0.435858588580919, 0.446783749429806, 0.306349472016557, 0.508508655381127, 0.510771564172110, 0.817627708322262, 0.794831416883453, 0.644318130193692, 0.378609382660268, 0.811580458282477, 0.532825588799455, 0.350727103576883},
				b: []float64{0.939001561999887, 0.875942811492984, 0.550156342898422, 0.622475086001228, 0.587044704531417, 0.207742292733028, 0.301246330279491, 0.470923348517591, 0.230488160211559, 0.844308792695389, 0.194764289567049, 0.225921780972399, 0.170708047147859, 0.227664297816554, 0.435698684103899, 0.311102286650413, 0.923379642103244, 0.430207391329584, 0.184816320124136, 0.904880968679893, 0.979748378356085, 0.438869973126103, 0.111119223440599, 0.258064695912067, 0.408719846112552, 0.594896074008614, 0.262211747780845, 0.602843089382083, 0.711215780433683, 0.221746734017240, 0.117417650855806, 0.296675873218327, 0.318778301925882, 0.424166759713807, 0.507858284661118, 0.0855157970900440, 0.262482234698333, 0.801014622769739, 0.0292202775621463, 0.928854139478045, 0.730330862855453, 0.488608973803579, 0.578525061023439, 0.237283579771521, 0.458848828179931, 0.963088539286913, 0.546805718738968, 0.521135830804002, 0.231594386708524, 0.488897743920167},
			},
			want: 0.6904, wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizedCrossCorrelationBig(tt.args.a, tt.args.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("NormalizedCrossCorrelationBig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			gotAsFloat, _ := got.Float64()
			if !testUtils.FloatEqUpTo(gotAsFloat, tt.want, 0.001) {
				t.Errorf("NormalizedCrossCorrelationBig() got = %v, want %v", gotAsFloat, tt.want)
			}
		})
	}
}

func TestComputeCorrelation(t *testing.T) {

	//generated with matlab
	traceA := []float64{0.489252638400019, 0.337719409821377, 0.900053846417662, 0.369246781120215, 0.111202755293787, 0.780252068321138, 0.389738836961253, 0.241691285913833, 0.403912145588115, 0.0964545251683886, 0.131973292606335, 0.942050590775485, 0.956134540229802, 0.575208595078466, 0.0597795429471558, 0.234779913372406, 0.353158571222071, 0.821194040197959, 0.0154034376515551, 0.0430238016578078, 0.168990029462704, 0.649115474956452, 0.731722385658670, 0.647745963136307, 0.450923706430945, 0.547008892286345, 0.296320805607773, 0.744692807074156, 0.188955015032545, 0.686775433365315, 0.183511155737270, 0.368484596490337, 0.625618560729690, 0.780227435151377, 0.0811257688657853, 0.929385970968730, 0.775712678608402, 0.486791632403172, 0.435858588580919, 0.446783749429806, 0.306349472016557, 0.508508655381127, 0.510771564172110, 0.817627708322262, 0.794831416883453, 0.644318130193692, 0.378609382660268, 0.811580458282477, 0.532825588799455, 0.350727103576883}
	traceB := []float64{0.939001561999887, 0.875942811492984, 0.550156342898422, 0.622475086001228, 0.587044704531417, 0.207742292733028, 0.301246330279491, 0.470923348517591, 0.230488160211559, 0.844308792695389, 0.194764289567049, 0.225921780972399, 0.170708047147859, 0.227664297816554, 0.435698684103899, 0.311102286650413, 0.923379642103244, 0.430207391329584, 0.184816320124136, 0.904880968679893, 0.979748378356085, 0.438869973126103, 0.111119223440599, 0.258064695912067, 0.408719846112552, 0.594896074008614, 0.262211747780845, 0.602843089382083, 0.711215780433683, 0.221746734017240, 0.117417650855806, 0.296675873218327, 0.318778301925882, 0.424166759713807, 0.507858284661118, 0.0855157970900440, 0.262482234698333, 0.801014622769739, 0.0292202775621463, 0.928854139478045, 0.730330862855453, 0.488608973803579, 0.578525061023439, 0.237283579771521, 0.458848828179931, 0.963088539286913, 0.546805718738968, 0.521135830804002, 0.231594386708524, 0.488897743920167}
	xCorrAA := 1.0
	xCorrAB := 0.6904

	type args struct {
		ctx           context.Context
		workerCount   int
		refereceTrace []float64
		traces        [][]float64
	}
	tests := []struct {
		name    string
		args    args
		want    []float64
		wantErr bool
	}{
		{
			name: "Ok",
			args: args{
				ctx:           context.Background(),
				workerCount:   4,
				refereceTrace: traceA,
				traces:        [][]float64{traceA, traceB, traceA, traceB, traceA, traceB},
			},
			want: []float64{xCorrAA, xCorrAB, xCorrAA, xCorrAB, xCorrAA, xCorrAB},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ComputeCorrelation(tt.args.ctx, tt.args.workerCount, tt.args.refereceTrace, tt.args.traces)
			if (err != nil) != tt.wantErr {
				t.Errorf("ComputeCorrelation() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && !testUtils.FloatSliceEqUpTo(got, tt.want, 0.001) {
				t.Errorf("ComputeCorrelation() got = %v, want %v", got, tt.want)
			}
		})
	}
}
