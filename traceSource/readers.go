package traceSource

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"wfmParser/wfm"
)

//array with num traces entries. 1 stands for random case, 0 for fixed case
func ParseCaseLog(rawLog []byte) ([]int, error) {
	scanner := bufio.NewScanner(bytes.NewReader(rawLog))
	scanner.Split(bufio.ScanLines)

	caseLog := make([]int, 0)
	for scanner.Scan() {
		line := scanner.Text()

		switch line {
		case "1":
			caseLog = append(caseLog, 1)
			break
		case "0":
			caseLog = append(caseLog, 0)
			break
		default:
			return nil, fmt.Errorf("unexpected entry %v in case log", line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning for lines : %v\n", err)
	}
	return caseLog, nil
}

//Interface for trace sources
type TraceBlockReader interface {
	//Total number of trace blocks
	TotalBlockCount() int
	//Reads the block identified by nr and returns the traces along with the case markers
	GetBlock(nr int) ([]byte, []int, error)
}

//Reads trace data from files with regular names
type TraceFileReader struct {
	totalFileCount int
	folderPath     string
	tracesPerFile  int
	caseLog        []int
	//map id to file name
	idToFileName func(id int) string
}

//NewDefaultTraceFileReader, assumes "trace (x).wfm" with x in [1,totalFileCount] as the naming scheme
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
	tfr.tracesPerFile = wfm.GetNumberOfTraces(rawFile)
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

//Does not expect all files to be available from the start but allows to receive updates
//about available files count. Idea is to process files as they come in from the oscilloscope
type StreamingTraceFileReader struct {
	//amount of files we are expecting
	totalFileCount int
	//filenames of already available files, stored in order of received
	availableFiles []string
	folderPath     string
	tracesPerFile  int
	caseLog        []int
	caseFileName   string
	fileNameChan   <-chan string
}

func NewStreamingTraceFileReader(fileCount int, folderPath, caseFileName string, nextFile <-chan string) *StreamingTraceFileReader {
	r := &StreamingTraceFileReader{
		totalFileCount: fileCount,
		availableFiles: make([]string, 0, fileCount),
		folderPath:     folderPath,
		tracesPerFile:  0,
		caseFileName:   caseFileName,
		caseLog:        nil,
		fileNameChan:   nextFile,
	}
	return r
}

func (recv *StreamingTraceFileReader) TotalBlockCount() int {
	return recv.totalFileCount
}

func (recv *StreamingTraceFileReader) GetBlock(nr int) ([]byte, []int, error) {

	if nr >= recv.totalFileCount {
		return nil, nil, fmt.Errorf("nr is out of bounds (got %v, need <= %v)", nr, recv.totalFileCount)
	}

	if nr >= len(recv.availableFiles) {
		//wait until file is available
		for nr >= len(recv.availableFiles) {
			newFileName := <-recv.fileNameChan
			//check if channel was closed
			if newFileName == "" {
				return nil, nil, fmt.Errorf("streaming source closed before file number %v was available", nr)
			}
			recv.availableFiles = append(recv.availableFiles, newFileName)
		}
		//update case file
		rawCaseLog, err := ioutil.ReadFile(filepath.Join(recv.folderPath, recv.caseFileName))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read case file : %v", err)
		}
		recv.caseLog, err = ParseCaseLog(rawCaseLog)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse case log file : %v", err)
		}
	}

	//file already available, just read and return
	fileContent, err := ioutil.ReadFile(filepath.Join(recv.folderPath, recv.availableFiles[nr]))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read wfm file : %v", err)
	}
	if recv.tracesPerFile == 0 {
		recv.tracesPerFile = wfm.GetNumberOfTraces(fileContent)
	}
	startIDX := nr * recv.tracesPerFile
	stopIDX := (nr + 1) * recv.tracesPerFile
	if startIDX >= len(recv.caseLog) || stopIDX > len(recv.caseLog) {
		return nil, nil, fmt.Errorf("file nr %v, caes indices are start=%v end=%v but len is only %v",
			nr, startIDX, stopIDX, len(recv.caseLog))
	}
	return fileContent, recv.caseLog[startIDX:stopIDX], nil

}
