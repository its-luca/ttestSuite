package main

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"math"
	"path/filepath"
	"testing"
	"ttestSuite/mocks"
	"ttestSuite/payloadComputation"
	"ttestSuite/traceSource"
	"ttestSuite/wfm"
)

type mockManyFileSimTraceFileReader struct {
	totalFileCount int
	file           []byte
	caseLog        []int
}

func (m *mockManyFileSimTraceFileReader) TotalBlockCount() int {
	return m.totalFileCount
}

func (m *mockManyFileSimTraceFileReader) GetBlock(_ int) ([]byte, []int, error) {
	return m.file, m.caseLog, nil
}

//Test_IntegrationTestTTest calls Run with WelchTTest payload using a real wfm file parser and compares against
//known results from the old matlab implementation
func Test_IntegrationTestTTest(t *testing.T) {
	//load correct results from csv file
	csvData, err := ioutil.ReadFile("../../testData/correct-t-values.csv")
	if err != nil {
		t.Fatalf("Failed to load test data file : %v\n", err)
	}

	tmp, err := mocks.ParseCSVToFloat64(csvData)
	if err != nil {
		t.Fatalf("Failed to parse correct t test values from file %v\n", err)
	}
	wanTValues := tmp[0]
	rawCaseFile, err := ioutil.ReadFile("../../testData/sample-case-log.txt")
	if err != nil {
		t.Fatalf("Failed to read case file : %v\n", err)
	}
	caseLog, err := traceSource.ParseCaseLog(rawCaseFile)
	if err != nil {
		t.Fatalf("Failed to parse case log file")
	}

	rawTraceFile, err := ioutil.ReadFile("../../testData/trace (1).wfm")
	if err != nil {
		t.Fatalf("Failed to read trace file : %v\n", err)
	}

	simManyFilesReader := &mockManyFileSimTraceFileReader{
		totalFileCount: 5,
		file:           rawTraceFile,
		caseLog:        caseLog,
	}

	//prepare caseLog for repeated reading
	tmpCaseLog := make([]int, 0, simManyFilesReader.totalFileCount*len(caseLog))
	for i := 0; i < simManyFilesReader.totalFileCount; i++ {
		tmpCaseLog = append(tmpCaseLog, caseLog...)
	}
	caseLog = tmpCaseLog
	simManyFilesReader.caseLog = caseLog
	//hacky file reader setup done
	creator, err := payloadComputation.GetWorkerPayloadCreator("ttest")
	if err != nil {
		t.Fatalf("failed to setup worker payload creator for test : %v\n", err)
	}
	config := payloadComputation.ComputationRuntime{
		ComputeWorkers:       3,
		BufferSizeInGB:       1,
		SnapshotInterval:     1,
		WorkerPayloadCreator: creator,
		SnapshotSaver: func(_ []float64, _ payloadComputation.WorkerPayload, _ int) error {
			return nil
		},
		DebugLog: log.New(io.Discard, "", 0),
		InfoLog:  log.New(io.Discard, "", 0),
		ErrLog:   log.New(io.Discard, "", 0),
	}

	payload, err := config.Run(context.Background(), simManyFilesReader, wfm.Parser{})
	if err != nil {
		t.Fatalf("Ttest failed : %v\n", err)
	}

	gotTValues, err := payload.Finalize()
	if err != nil {
		t.Fatalf("unexpected error finalizing payload computation : %v\n", err)
	}

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
