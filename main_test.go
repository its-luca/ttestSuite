package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/pbnjay/memory"
	"io/ioutil"
	"math"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"ttestSuite/traceSource"
	"ttestSuite/wfm"
)

func parseCSVToFloat64(csvData []byte) ([][]float64, error) {
	//parse csv to array
	scanner := bufio.NewScanner(bytes.NewReader(csvData))
	buf := make([]byte, 0, len(csvData))
	scanner.Buffer(buf, len(buf))
	scanner.Split(bufio.ScanLines)

	parsedData := make([][]float64, 0)
	for scanner.Scan() {
		line := scanner.Text()
		tokens := strings.Split(line, ",")
		row := make([]float64, 0, len(tokens))
		for _, token := range tokens {
			f, err := strconv.ParseFloat(strings.TrimSpace(token), 64)
			if err != nil {
				return nil, fmt.Errorf("Failed to parse test input %v to float64 : %v\n", token, err)
			}
			row = append(row, f)
		}
		parsedData = append(parsedData, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("Failed to parse test data faile : %v\n", err)
	}

	return parsedData, nil
}

func Test_wfmToTraces(t *testing.T) {
	//load correct results from csv file
	csvData, err := ioutil.ReadFile("./testData/correct-first-3-rows.csv")
	if err != nil {
		t.Fatalf("Failed to load test data file : %v\n", err)
	}

	correctTraces, err := parseCSVToFloat64(csvData)
	if err != nil {
		t.Fatal(err)
	}

	//load test input
	rawWFM, err := ioutil.ReadFile("./testData/trace (1).wfm")
	if err != nil {
		t.Fatalf("Failed to read test input :%v\n", err)
	}

	//call function that we want to test
	frames, err := wfm.ParseTraces(rawWFM, nil)
	if err != nil {
		t.Fatalf("unexpected Error %v\n", err)
	}

	//we only check the first few traces
	if len(frames) < len(correctTraces) {
		t.Fatalf("ouput needs at least %v traces for the test but we got %v", len(correctTraces), len(frames))
	}

	errCounter := 0
	errThresh := 5
	//compare output
	for traceIDX := range correctTraces {
		if gotLen, wantLen := len(frames[traceIDX]), len(correctTraces[traceIDX]); gotLen != wantLen {
			t.Errorf("trace %v has length %v but we want %v\n", traceIDX, gotLen, wantLen)
		}
		for i := range frames {
			if got, want := frames[traceIDX][i], correctTraces[traceIDX][i]; math.Abs(got-want) > 0.001 {
				t.Errorf("trace %v entry %v got %v want %v\n", traceIDX, i, got, want)
				errCounter++
				if errCounter > errThresh {
					t.Errorf("Aborting due to too many errors\n")
					t.FailNow()
				}

			}
		}
	}

}

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

func Test_parseAndTTest(t *testing.T) {
	//load correct results from csv file
	csvData, err := ioutil.ReadFile("./testData/correct-t-values.csv")
	if err != nil {
		t.Fatalf("Failed to load test data file : %v\n", err)
	}

	tmp, err := parseCSVToFloat64(csvData)
	if err != nil {
		t.Fatalf("Failed to parse correct t test values from file %v\n", err)
	}
	wanTValues := tmp[0]
	rawCaseFile, err := ioutil.ReadFile("./testData/sample-case-log.txt")
	if err != nil {
		t.Fatalf("Failed to read case file : %v\n", err)
	}
	caseLog, err := traceSource.ParseCaseLog(rawCaseFile)
	if err != nil {
		t.Fatalf("Failed to parse case log file")
	}

	rawTraceFile, err := ioutil.ReadFile("./testData/trace (1).wfm")
	if err != nil {
		t.Fatalf("Failed to read trace file : %v\n", err)
	}

	simManyFilesReader := &mockManyFileSimTraceFileReader{
		totalFileCount: 32,
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

	config := Config{
		ComputeWorkers: runtime.NumCPU() - 1,
		FeederWorkers:  1,
		BufferSizeInGB: maxInt(1, int(memory.TotalMemory()/Giga)-10),
	}

	batchMeanAndVar, err := TTest(simManyFilesReader, config)
	if err != nil {
		t.Fatalf("Ttest failed : %v\n", err)
	}

	gotTValues := batchMeanAndVar.ComputeLQ()

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
