package traceSource

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/its-luca/ttestSuite/wfm"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
)

//StreamingTraceFileReader does not expect all files to be available from the start but allows to receive updates
//about file availability count. Idea is to process files as they come in from the oscilloscope. Number of files
//must be known in advance for now
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

//GetBlock may block if file denoted by nr is not yet available
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
		recv.tracesPerFile, err = wfm.GetNumberOfTraces(fileContent)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse file : %v", err)
		}
	}
	startIDX := nr * recv.tracesPerFile
	stopIDX := (nr + 1) * recv.tracesPerFile
	if startIDX >= len(recv.caseLog) || stopIDX > len(recv.caseLog) {
		return nil, nil, fmt.Errorf("file nr %v, caes indices are start=%v end=%v but len is only %v",
			nr, startIDX, stopIDX, len(recv.caseLog))
	}
	return fileContent, recv.caseLog[startIDX:stopIDX], nil

}

type receiver struct {
	newFileNames            chan string
	totalNumberOfTraceFiles int
	receivedTraceFiles      int
	//signal that receivedTraceFiles>=totalNumberOfTraceFiles
	doneReceiving        chan interface{}
	receivedMeasureStart chan bool
}

type measureStartMsg struct {
	TotalNumberOfTraceFiles int
}
type measureUpdateMsg struct {
	FileName string
}

//waits for a measureUpdateMsg and performs state switch to running measurement
func (rec *receiver) handleMeasureStart(w http.ResponseWriter, r *http.Request) {
	log.Printf("handleMeasureStart handler called")

	if rec.totalNumberOfTraceFiles != 0 {
		http.Error(w, fmt.Sprintf("Already received measure start message for %v trace files",
			rec.totalNumberOfTraceFiles), http.StatusBadRequest)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed ready body", http.StatusInternalServerError)
		log.Printf("Failed to read body : %v\n", err)
		return
	}
	defer func() {
		if err := r.Body.Close(); err != nil {
			log.Printf("failed to close request body")
		}
	}()

	msg := measureStartMsg{}
	if err := json.Unmarshal(body, &msg); err != nil {
		http.Error(w, "failed to parse body", http.StatusBadRequest)
		log.Printf("Failed to parse body : %v\n", err)
		return
	}
	if msg.TotalNumberOfTraceFiles <= 0 {
		http.Error(w, "Number of trace files must be > 0", http.StatusBadRequest)
		return
	}
	rec.totalNumberOfTraceFiles = msg.TotalNumberOfTraceFiles
	rec.receivedMeasureStart <- true

}

//writes names of new measurement files to recv.newFileNames
func (rec *receiver) handleMeasureUpdate(w http.ResponseWriter, r *http.Request) {
	log.Printf("handleMeasureUpdate handler called")

	if rec.totalNumberOfTraceFiles == 0 {
		http.Error(w, "Send a measure start message first", http.StatusBadRequest)
		log.Printf("Discared early measure update message\n")
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed ready body", http.StatusInternalServerError)
		log.Printf("Failed to read body : %v\n", err)
		return
	}
	defer func() {
		if err := r.Body.Close(); err != nil {
			log.Printf("failed to close request body")
		}
	}()

	msg := measureUpdateMsg{}
	if err := json.Unmarshal(body, &msg); err != nil {
		http.Error(w, "failed to parse body", http.StatusBadRequest)
		log.Printf("Failed to parse body : %v\n", err)
		return
	}

	//extract only the file name
	msg.FileName = filepath.Base(msg.FileName)
	if msg.FileName == "" || msg.FileName == "." || msg.FileName == "/" {
		http.Error(w, fmt.Sprintf("%v is and invalid file name", msg.FileName), http.StatusBadRequest)
		log.Printf("invalid file name %v", msg.FileName)
		return
	}
	rec.newFileNames <- msg.FileName
	rec.receivedTraceFiles++
	if rec.receivedTraceFiles >= rec.totalNumberOfTraceFiles {
		rec.doneReceiving <- nil
	}
}

func NewReceiver() *receiver {
	tmp := &receiver{
		newFileNames:         make(chan string, 50),
		doneReceiving:        make(chan interface{}),
		receivedMeasureStart: make(chan bool),
	}
	return tmp
}

func (rec *receiver) Routes() http.Handler {
	router := http.NewServeMux()
	router.Handle("/start", http.HandlerFunc(rec.handleMeasureStart))
	router.Handle("/newFile", http.HandlerFunc(rec.handleMeasureUpdate))
	return router
}

func (rec *receiver) WaitForMeasureStart(ctx context.Context) (int, <-chan string) {
	go func() {
		select {
		case <-ctx.Done():
		case <-rec.doneReceiving:
		}
		close(rec.newFileNames)
		close(rec.receivedMeasureStart)
		return
	}()
	log.Printf("waiting for start message...\n")
	if <-rec.receivedMeasureStart {
		log.Printf("received, expecting %v trace files\n", rec.totalNumberOfTraceFiles)
	} else {
		log.Printf("was aborted before reciving measure start message")
	}

	return rec.totalNumberOfTraceFiles, rec.newFileNames
}
