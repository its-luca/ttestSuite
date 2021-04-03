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
func (recv *receiver) handleMeasureStart(w http.ResponseWriter, r *http.Request) {
	log.Printf("handleMeasureStart handler called")

	if recv.totalNumberOfTraceFiles != 0 {
		http.Error(w, fmt.Sprintf("Already received measure start message for %v trace files",
			recv.totalNumberOfTraceFiles), http.StatusBadRequest)
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
	recv.totalNumberOfTraceFiles = msg.TotalNumberOfTraceFiles
	recv.receivedMeasureStart <- true

}

//writes names of new measurement files to recv.newFileNames
func (recv *receiver) handleMeasureUpdate(w http.ResponseWriter, r *http.Request) {
	log.Printf("handleMeasureUpdate handler called")

	if recv.totalNumberOfTraceFiles == 0 {
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
	recv.newFileNames <- msg.FileName
	recv.receivedTraceFiles++
	if recv.receivedTraceFiles >= recv.totalNumberOfTraceFiles {
		recv.doneReceiving <- nil
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

/*
//Takes control over srv and adds measure start and measure update messages handlers.
//Terminates the server once done
//Returns total number of expected measurement files and a channel that is update with the incoming file names
func newReceiverFromWebserver(ctx context.Context,srv Server) (int,<- chan string,error){
	rec := receiver{
		newFileNames:            make(chan string, 50),
		doneReceiving:           make(chan interface{}),
		startWg:                 sync.WaitGroup{},
	}
	//start webserver

	srv.Handler = routes(rec)
	rec.startWg.Add(1)
	go func() {
		//when the webserver finishes there will be no more new files
		defer close(rec.newFileNames)
		//only used by handleMeasureUpdate which cannot be called once webserver is closed
		defer close(rec.doneReceiving)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err,http.ErrServerClosed) {
			log.Printf("webserver crashed : %v\n",err)
		}
	}()
	go func() {
		//wait until context is cancelled or we received all files
		select {
		case  <- ctx.Done():
		case <-rec.doneReceiving:
		}
		shutdownCtx, cancel:= context.WithDeadline(ctx, time.Now().Add(15*time.Second))
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("Gracefull shutdown failed : %\n", err)
		}
	}()
	//wait for start message
	rec.startWg.Wait()


	return rec.totalNumberOfTraceFiles,rec.newFileNames,nil
}
/*
//Opens an http server on addr that handles measure start and measure update messages.
//Returns total number of expected measurement files and a channel that is update with the incoming file names
func NewReceiver(ctx context.Context,addr string) (int,<- chan string,error) {

	srv := &http.Server{
		Addr:    addr,
	}
	return newReceiverFromWebserver(ctx,srv)
}*/
