package main

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"math"
	"strconv"
	"strings"
	"testing"
)

func Test_wfmToTraces(t *testing.T) {
	//load correct results from csv file
	csvData, err := ioutil.ReadFile("./correct-first-3-rows.csv")
	if err != nil {
		t.Fatalf("Failed to load test data file : %v\n", err)
		t.FailNow()
	}

	//parse csv to array
	scanner := bufio.NewScanner((bytes.NewReader(csvData)))
	buf := make([]byte, 0, len(csvData))
	scanner.Buffer(buf, len(buf))
	scanner.Split(bufio.ScanLines)

	correctTraces := make([][]float64, 0)
	for scanner.Scan() {
		line := scanner.Text()
		tokens := strings.Split(line, ",")
		trace := make([]float64, 0, len(tokens))
		for _, token := range tokens {
			f, err := strconv.ParseFloat(strings.TrimSpace(token), 64)
			if err != nil {
				t.Fatalf("Failed to parse test input %v to float64 : %v\n", token, err)
				t.FailNow()
			}
			trace = append(trace, f)
		}
		correctTraces = append(correctTraces, trace)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Failed to parse test data faile : %v\n", err)
		t.FailNow()
	}

	//load test input
	rawWFM, err := ioutil.ReadFile("./trace-1.wfm")
	if err != nil {
		t.Fatalf("Failed to read test input :%v\n", err)
		t.Fail()
	}

	//call function that we want to test
	frames, err := wfmToTraces(rawWFM, nil)
	if err != nil {
		t.Fatalf("unexpected Error %v\n", err)
		t.FailNow()
	}

	//we only check the first few traces
	if len(frames) < len(correctTraces) {
		t.Fatalf("ouput needs at least %v traces for the test but we got %v", len(correctTraces), len(frames))
		t.FailNow()
	}

	errCounter := 0
	errThresh := 5
	//compare output
	for traceIDX, _ := range correctTraces {
		if gotLen, wantLen := len(frames[traceIDX]), len(correctTraces[traceIDX]); gotLen != wantLen {
			t.Errorf("trace %v has length %v but we want %v\n", traceIDX, gotLen, wantLen)
		}
		for i, _ := range frames {
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
