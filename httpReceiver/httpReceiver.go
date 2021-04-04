package httpReceiver

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
)

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
	defer r.Body.Close()

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
	defer r.Body.Close()

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
