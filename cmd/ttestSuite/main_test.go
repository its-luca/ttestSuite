package main

import (
	"context"
	"errors"
	"github.com/its-luca/ttestSuite/mocks"
	"github.com/its-luca/ttestSuite/payloadComputation"
	"github.com/its-luca/ttestSuite/testUtils"
	"io"
	"io/ioutil"
	"log"
	"math"
	"path/filepath"
	"testing"
)

//Test_IntegrationTestTTest calls Run with WelchTTest payload using a real wfm file parser and compares against
//known results from the old matlab implementation
func Test_IntegrationTestTTest(t *testing.T) {
	//load correct results from csv file. We generated them fromt he matlab implementation
	csvData, err := ioutil.ReadFile("../../testData/Test_IntegrationTestTTest-want-t-values.csv")
	if err != nil {
		t.Fatalf("Failed to load test data file : %v\n", err)
	}
	tmp, err := mocks.ParseCSVToFloat64(csvData)
	if err != nil {
		t.Fatalf("Failed to parse correct t test values from file %v\n", err)
	}
	wanTValues := tmp[0]

	//regenerate trace data via determenistic rng
	dRNGValues := testUtils.DRNGFloat64Slice(50000, 486)
	fakeFileTraces := [][]float64{
		dRNGValues[:10000],
		dRNGValues[10000:20000],
		dRNGValues[20000:30000],
		dRNGValues[30000:40000],
		dRNGValues[40000:50000],
	}
	fakeFiles := [][][]float64{fakeFileTraces, fakeFileTraces}
	fakeFilesCaseData := [][]int{
		{0, 1, 1, 0, 0},
		{1, 0, 1, 1, 0},
	}
	fakeSource, fakeParser, err := mocks.CreateFloatSourceParserPair(fakeFiles, fakeFilesCaseData, -1)
	if err != nil {
		t.Fatalf("Failed to setup trace source : %v", err)
	}

	//setup ttest execution

	creator, err := payloadComputation.GetWorkerPayloadCreator("ttest")
	if err != nil {
		t.Fatalf("failed to setup worker payload creator for test : %v\n", err)
	}
	config, err := payloadComputation.NewComputationRuntime(3, 1, 1, creator,
		func(_ []float64, _ payloadComputation.WorkerPayload, _ int) error {
			return nil
		},
		log.New(io.Discard, "", 0),
		log.New(io.Discard, "", 0),
		log.New(io.Discard, "", 0),
	)
	if err != nil {
		t.Fatalf("Failed to setup computation runtime for test : %v", err)
	}

	//run test

	payload, err := config.Run(context.Background(), fakeSource, fakeParser)
	if err != nil {
		t.Fatalf("Ttest failed : %v\n", err)
	}

	gotTValues, err := payload.Finalize()
	if err != nil {
		t.Fatalf("unexpected error finalizing payload computation : %v\n", err)
	}

	//check results

	if gotLen, wantLen := len(gotTValues), len(wanTValues); gotLen != wantLen {
		t.Errorf("wanted %v values got %v valuesan", wantLen, gotLen)
	}

	minLen := int(math.Min(float64(len(gotTValues)), float64(len(wanTValues))))

	errCnt := 0
	errThresh := 5
	for i := 0; i < minLen; i++ {
		if got, want := gotTValues[i], wanTValues[i]; math.Abs(got-want) > 0.001 {
			t.Errorf("t value %v wanted %v got %v\n", i, want, got)
			errCnt++
			if errCnt > errThresh {
				t.Fatalf("To many errrors, abort\n")
			}
		}
	}
}

func Test_createCollisionFreeName(t *testing.T) {
	type args struct {
		outPath       string
		doesFileExist func(path string) bool
	}
	tests := []struct {
		name        string
		args        args
		want        string
		wantErr     bool
		specificErr error
	}{
		{
			name: "No collision",
			args: args{
				outPath: "./cwd/freeName",
				doesFileExist: func(path string) bool {
					return path != "./cwd/freeName"
				},
			},
			want:    "./cwd/freeName",
			wantErr: false,
		},
		{
			name: "Fixable collision",
			args: args{
				outPath: "./cwd/usedName",
				doesFileExist: func(path string) bool {
					used := map[string]bool{filepath.Clean("./cwd/usedName"): true, filepath.Clean("./cwd/usedName-1"): true, filepath.Clean("./cwd/usedName-2"): true}
					return used[filepath.Clean(path)]
				},
			},
			want:    "./cwd/usedName-3",
			wantErr: false,
		},
		{
			name: "Unfixable collision",
			args: args{
				outPath: "./cwd/unfixable",
				doesFileExist: func(path string) bool {
					return true
				},
			},
			wantErr:     true,
			specificErr: errCollisionAvoidanceFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createCollisionFreeName(tt.args.outPath, tt.args.doesFileExist)
			if (err != nil) != tt.wantErr {
				t.Errorf("createCollisionFreeName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.specificErr != nil && !errors.Is(err, tt.specificErr) {
				t.Errorf("createCollisionFreeName() wanted error %v got %v\n", tt.specificErr, err)
				return
			}
			if !tt.wantErr && filepath.Clean(got) != filepath.Clean(tt.want) {
				t.Errorf("createCollisionFreeName() got = %v, want %v", got, tt.want)
			}
		})
	}
}
