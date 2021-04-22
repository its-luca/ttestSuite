package traceSource

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"ttestSuite/wfm"
)

//TraceFileReader reads trace files folderPath using the passed idToFileName function to generate on order on the files.
//All files must be available at instantiation time
type TraceFileReader struct {
	totalFileCount int
	folderPath     string
	tracesPerFile  int
	caseLog        []int
	//map id (linear order used in program logic) to file name
	idToFileName func(id int) string
}

//NewDefaultTraceFileReader assumes "trace (x).wfm" with x in [1,totalFileCount] as the naming scheme
func NewDefaultTraceFileReader(fileCount int, folderPath, caseFileName string) (*TraceFileReader, error) {
	tfr := &TraceFileReader{
		totalFileCount: fileCount,
		folderPath:     folderPath,
		idToFileName: func(id int) string {
			return fmt.Sprintf("trace (%v).wfm", id+1)
		},
	}
	rawFile, err := ioutil.ReadFile(filepath.Join(tfr.folderPath, tfr.idToFileName(0)))
	if err != nil {
		return nil, fmt.Errorf("failed to get traces per file : %v", err)
	}
	rawCaseLog, err := ioutil.ReadFile(filepath.Join(tfr.folderPath, caseFileName))
	if err != nil {
		return nil, fmt.Errorf("failed to read case file : %v", err)
	}
	caseLog, err := ParseCaseLog(rawCaseLog)
	if err != nil {
		return nil, fmt.Errorf("failed to parse case log file : %v", err)
	}
	tfr.tracesPerFile, err = wfm.GetNumberOfTraces(rawFile)
	if err != nil {
		return nil, fmt.Errorf("failed to parse case log file : %v", err)
	}
	tfr.caseLog = caseLog
	return tfr, nil
}

func (recv *TraceFileReader) TotalBlockCount() int {
	return recv.totalFileCount
}

func (recv *TraceFileReader) GetBlock(nr int) ([]byte, []int, error) {
	fileContent, err := ioutil.ReadFile(filepath.Join(recv.folderPath, recv.idToFileName(nr)))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read wfm file : %v", err)
	}
	startIDX := nr * recv.tracesPerFile
	stopIDX := (nr + 1) * recv.tracesPerFile

	return fileContent, recv.caseLog[startIDX:stopIDX], nil
}
