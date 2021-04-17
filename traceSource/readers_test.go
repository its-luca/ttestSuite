package traceSource

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"
)

func TestStreamingTraceFileReader_GetBlock(t *testing.T) {

	//setup
	const wantTotalFileCount = 5
	const folderPath = "../testData"
	const traceFileName = "trace (1).wfm"
	traceFilePath := filepath.Join(folderPath, traceFileName)
	const caseFileName = "tmp-case-file.txt"
	newFiles := make(chan string)

	wantTraceFile, err := ioutil.ReadFile(traceFilePath)
	if err != nil {
		t.Fatalf("Failed to read test file %v\n", err)
	}
	rawCaseFile, err := ioutil.ReadFile(filepath.Join(folderPath, "sample-case-log.txt"))
	if err != nil {
		t.Fatalf("Failed to read raw case file %v\n", err)
	}
	wantCaseData, err := ParseCaseLog(rawCaseFile)
	if err != nil {
		t.Fatalf("Failed to parse test case data : %v\n", err)
	}

	testCaseFile, err := os.Create(filepath.Join(folderPath, "tmp-case-file.txt"))
	if err != nil {
		t.Fatalf("Failed to create test case file: %v\n", err)
	}
	defer func() {
		if err := os.Remove(path.Join(folderPath, "tmp-case-file.txt")); err != nil {
			t.Errorf("Failed to clean up test case file :%v\n", err)
		}
	}()

	//simulate receiving files
	go func() {
		time.Sleep(500 * time.Millisecond)
		for i := 0; i < wantTotalFileCount; i++ {
			//append new case data
			if _, err := io.Copy(testCaseFile, bytes.NewReader(rawCaseFile)); err != nil {
				t.Errorf("Failed to append new test case file content : %v\n", err)
			}
			if err := testCaseFile.Sync(); err != nil {
				t.Errorf("Failed to flush testCaseFile content :%v\n", err)
			}
			//send new file name
			newFiles <- traceFileName
		}
	}()

	//# test behaviour
	r := NewStreamingTraceFileReader(wantTotalFileCount, folderPath, caseFileName, newFiles)

	if gotTotalBlockCount := r.TotalBlockCount(); gotTotalBlockCount != wantTotalFileCount {
		t.Errorf("want totalBlockCount %v, got %v", wantTotalFileCount, gotTotalBlockCount)
	}

	checkFile := func(nr int) {
		gotTraceFile, gotCaseData, err := r.GetBlock(nr)
		if err != nil {
			t.Fatalf("unexpected error reading file number %v : %v\n", nr, err)
		}
		if wantLen, gotLen := len(wantTraceFile), len(gotTraceFile); wantLen != gotLen {
			t.Errorf("trace file %v want %v bytes, got %v bytes\n", nr, wantLen, gotLen)
		}
		for i := range wantTraceFile {
			if wantTraceFile[i] != gotTraceFile[i] {
				t.Errorf("trace file %v, expected byte %v to be %v but got %v\n", nr, i, wantTraceFile[i], gotTraceFile[i])
			}
		}
		if wantLen, gotLen := len(wantCaseData), len(gotCaseData); wantLen != gotLen {
			t.Errorf("trace file %v want %v case entries , got %v case entries\n", nr, wantLen, gotLen)
		}
		for i := range wantCaseData {
			if wantCaseData[i] != gotCaseData[i] {
				t.Errorf("case file %v, expected entry %v to be %v but got %v\n", nr, i, wantCaseData[i], gotCaseData[i])
			}
		}

	}
	for i := 0; i < wantTotalFileCount; i++ {
		checkFile(i)
	}

	//reading already received files should be fine
	checkFile(0)

	//reading out of bounds should give error
	_, _, err = r.GetBlock(wantTotalFileCount)
	//WANT ERROR
	if err == nil {
		t.Errorf("reading out of bounds, expected err got none")
	}

}
