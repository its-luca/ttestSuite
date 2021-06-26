package wfm

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"strconv"
	"strings"
	"testing"
)

//ParseCSVToFloat64 takes a raw csv file and parses each line as into a float64 slice
func ParseCSVToFloat64(csvData []byte) ([][]float64, error) {
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

//TestParser_ParseTracesParseTraces calls the wfm parser on a real trace file and compares the result
//against known values from the old matlab implementation
func TestParser_ParseTracesParseTraces(t *testing.T) {
	//load correct results from csv file
	const wantFrames = 10
	const wantPointsPerTrace = 1250
	csvData, err := ioutil.ReadFile("../testData/trace-10-frames-a-1250-points-want-flat.csv")
	if err != nil {
		t.Fatalf("Failed to load test data file : %v\n", err)
	}

	wantTraceDataFlat, err := ParseCSVToFloat64(csvData)
	if err != nil {
		t.Fatal(err)
	}

	//load test input
	rawWFM, err := ioutil.ReadFile("../testData/trace-10-frames-a-1250-points.wfm")
	if err != nil {
		t.Fatalf("Failed to read test input :%v\n", err)
	}

	//call function that we want to test
	frames, err := ParseTraces(rawWFM, nil)
	if err != nil {
		t.Fatalf("unexpected Error %v\n", err)
	}

	//we only check the first few traces
	if len(frames) != wantFrames {
		t.Fatalf("expected  %v frames got %v", frames, len(frames))
	}

	errCounter := 0
	errThresh := 5
	//compare output
	for traceIDX := range wantTraceDataFlat {
		if gotLen := len(frames[traceIDX]); gotLen != wantPointsPerTrace {
			t.Errorf("trace %v has length %v but we want %v\n", traceIDX, gotLen, wantPointsPerTrace)
		}
		for i := range frames {
			if got, want := frames[traceIDX][i], wantTraceDataFlat[0][traceIDX*wantPointsPerTrace+i]; math.Abs(got-want) > 0.001 {
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
