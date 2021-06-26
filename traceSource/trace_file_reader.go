package traceSource

import (
	"fmt"
	"github.com/its-luca/ttestSuite/wfm"
	"io/ioutil"
	"path/filepath"
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
	reverseOrder bool
}

/*
//NewReversedTraceFileReader assumes "trace (x).wfm" with x in [1,totalFileCount] as the naming scheme
//but accesses the files in reverse order, e.g. GetBlock(0) will return the content of "trace (totalFileCount).wfm"
func NewReversedTraceFileReader(fileCount int, folderPath, caseFileName string) (*TraceFileReader, error) {
	reader, err := NewDefaultTraceFileReader(fileCount, folderPath, caseFileName)
	if err != nil {
		return nil, err
	}
	reader.idToFileName = func(id int) string {
		//note that the numbers in the file names are in [1,totalFilesCount], so NO -1
		s:= fmt.Sprintf("trace (%v).wfm", reader.totalFileCount-id)
		log.Printf("reverse reader, accessing file: %v",s)
		return s
	}
	return reader, nil
}*/

//NewDefaultTraceFileReader assumes "trace (x).wfm" with x in [1,totalFileCount] as the naming scheme. If reverseOrder is set
// GetBlock(nr) will return the trace (and case data) for "trace (totalFileCount-nr).wfm" instead of "trace (nr+1).wfm"
func NewDefaultTraceFileReader(fileCount int, folderPath, caseFileName string, reverseOrder bool) (*TraceFileReader, error) {
	tfr := &TraceFileReader{
		totalFileCount: fileCount,
		folderPath:     folderPath,
		reverseOrder:   reverseOrder,
	}
	if reverseOrder {
		tfr.idToFileName = func(id int) string {
			//note that the numbers in the file names are in [1,totalFilesCount], so NO -1
			return fmt.Sprintf("trace (%v).wfm", tfr.totalFileCount-id)
		}
	} else {
		tfr.idToFileName = func(id int) string {
			return fmt.Sprintf("trace (%v).wfm", id+1)
		}
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

	var startIDX, stopIDX int
	if recv.reverseOrder {
		stopIDX = (recv.totalFileCount * recv.tracesPerFile) - (nr * recv.tracesPerFile)
		startIDX = stopIDX - recv.tracesPerFile
	} else {
		startIDX = nr * recv.tracesPerFile
		stopIDX = (nr + 1) * recv.tracesPerFile
	}

	return fileContent, recv.caseLog[startIDX:stopIDX], nil
}
