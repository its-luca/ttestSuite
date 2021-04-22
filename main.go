package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"github.com/pbnjay/memory"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"ttestSuite/httpReceiver"
	"ttestSuite/tPlot"
	"ttestSuite/traceSource"
	"ttestSuite/wfm"
)

//Giga SI unit prefix
const Giga = 1024 * 1024 * 1024

//Mega SI unit prefix
const Mega = 1024 * 1024

//application bundles the command line configuration options
type application struct {
	pathTraceFolder      string
	traceFileCount       int
	pathCaseLogFile      string
	numWorkers           int
	fileBufferInGB       int
	streamFromAddr       string
	out                  string
	tTestThreshold       float64
	snapshotInterval     int
	workerPayloadCreator WorkerPayloadCreator
}

//ParseAndValidateFlags parses flags provided in os.Args, and returns the parsed values if all logic checks pass.
//Otherwise a multiline error is returns that also contains on overview over all flags
func ParseAndValidateFlags() (*application, error) {

	usageBuf := &bytes.Buffer{}
	cmdFlags := flag.NewFlagSet("default", flag.ContinueOnError)
	cmdFlags.SetOutput(usageBuf)
	//fill usageBuf with description

	pathTraceFolder := cmdFlags.String("traceFolder", "", "Path to folder containing wfm trace files (expected naming scheme is trace (v).wfm where v is an incrementing number (starting at 1) in sync with the case log file")
	traceFileCount := cmdFlags.Int("traceFileCount", 0, "Number of trace files. Ignored in streaming setup")
	pathCaseLogFile := cmdFlags.String("caseFile", "", "Path to the trace file (either 0 or 1, one entry per line)")
	numWorkers := cmdFlags.Int("numWorkers", runtime.NumCPU()-1, "Number of threads to do the t-test computation (also note numFeeders). Influences CPU usage")
	fileBufferInGB := cmdFlags.Int("fileBufferInGB", 2, "Memory allowed for buffering input files in GB")
	streamFromAddr := cmdFlags.String("streamFromAddr", "", "If set, we will listen on the provided addr to receive updates about file availability. Files are still read from disk!")
	out := cmdFlags.String("out", "./t-values.csv", "Path for t-test result file")
	tTestThreshold := cmdFlags.Float64("tTestThresh", 6, "Threshold value for t-test plot")
	snapshotInterval := cmdFlags.Int("snapshotInterval", 0, "Save intermediate result every x trace files")
	payloadComputation := cmdFlags.String("payloadComputation", "ttest", fmt.Sprintf("Choose which of the following computation should be performed on the data: %s", GetAvailablePayloads()))
	var workerPayloadCreator WorkerPayloadCreator
	cmdFlags.PrintDefaults()

	if err := cmdFlags.Parse(os.Args[1:]); err != nil {
		return nil, fmt.Errorf("%v\n%s", err, usageBuf.String())
	}

	err := func() (descriptiveError error) {
		//append usage string if we return an error
		defer func() {
			if descriptiveError != nil {
				descriptiveError = fmt.Errorf("%v\nUsage:\n%s", descriptiveError.Error(), usageBuf.String())
			}
		}()

		if *pathTraceFolder == "" {
			descriptiveError = fmt.Errorf("please set path to trace folder")
			return
		}

		if *traceFileCount <= 0 && *streamFromAddr == "" {
			descriptiveError = fmt.Errorf("please set number of trace files to a positive number")
			return
		}

		if *snapshotInterval == 0 { //means not specified by user, only do one snapshot
			*snapshotInterval = *traceFileCount
		} else if *snapshotInterval > *traceFileCount {
			descriptiveError = fmt.Errorf("snapshot interval may not be large than the number of trace files (%v)", *traceFileCount)
			return
		}

		if *pathCaseLogFile == "" {
			descriptiveError = fmt.Errorf("please set path to case log file")
			return

		}

		if *numWorkers < 0 {
			descriptiveError = fmt.Errorf("please set numWorkers to a number in [1,%v[", runtime.NumCPU()-1)
			return

		}

		if *fileBufferInGB < 1 {
			descriptiveError = fmt.Errorf("file buffer neeeds to be at least one GB")
			return

		}
		if *fileBufferInGB > int(memory.TotalMemory()/Giga) {
			descriptiveError = fmt.Errorf("your file buffer is larger than the available memory")
			return
		}

		var err error
		workerPayloadCreator, err = GetWorkerPayloadCreator(*payloadComputation)
		if err != nil {
			descriptiveError = fmt.Errorf("failed to instantiate payload compuation creator \"%v\": %v", *payloadComputation, err)
			return
		}

		return descriptiveError
	}()

	if err != nil {
		return nil, err
	}

	return &application{
		pathTraceFolder:      *pathTraceFolder,
		traceFileCount:       *traceFileCount,
		pathCaseLogFile:      *pathCaseLogFile,
		numWorkers:           *numWorkers,
		fileBufferInGB:       *fileBufferInGB,
		streamFromAddr:       *streamFromAddr,
		out:                  *out,
		tTestThreshold:       *tTestThreshold,
		snapshotInterval:     *snapshotInterval,
		workerPayloadCreator: workerPayloadCreator,
	}, nil

}

func main() {

	//Handle command line options
	app, err := ParseAndValidateFlags()
	if err != nil {
		fmt.Printf("Error parsing config : %v\n", err)
		return
	}

	config := ComputationConfig{
		ComputeWorkers:   app.numWorkers,
		BufferSizeInGB:   app.fileBufferInGB,
		SnapshotInterval: app.snapshotInterval,
	}

	//Startup TTtest
	var traceReader traceSource.TraceBlockReader
	if app.streamFromAddr == "" {
		var err error
		traceReader, err = traceSource.NewDefaultTraceFileReader(app.traceFileCount, app.pathTraceFolder, filepath.Base(app.pathCaseLogFile))
		if err != nil {
			log.Fatalf("Failed to create trace file reader : %v", err)
		}
	} else {
		var filenameUpdates <-chan string

		receiver := httpReceiver.NewReceiver()
		srv := &http.Server{
			Addr:    app.streamFromAddr,
			Handler: receiver.Routes(),
		}
		go func() {
			log.Printf("Listening on %v", srv.Addr)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("webserver crashed : %v\n", err)
			}
		}()
		//note that the webserver is started concurrently, else we could not receive the start message
		receiverCtx, receiverCancel := context.WithCancel(context.Background())
		shutdownRequest := make(chan os.Signal, 1)
		signal.Notify(shutdownRequest, os.Interrupt)
		go func() {
			<-shutdownRequest
			log.Printf("initiating shutdown...")
			receiverCancel()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				log.Printf("gracefull server shutdown failed : %v\n", err)
			}
		}()

		app.traceFileCount, filenameUpdates = receiver.WaitForMeasureStart(receiverCtx)

		traceReader = traceSource.NewStreamingTraceFileReader(app.traceFileCount, app.pathTraceFolder, filepath.Base(app.pathCaseLogFile), filenameUpdates)
	}

	//todo: propagate cancellation
	//todo: add useful callback
	workerPayload, err := TTest(context.TODO(), traceReader, wfm.Parser{}, app.workerPayloadCreator, func(payload WorkerPayload) error { return nil }, config)
	if err != nil {
		log.Fatalf("Ttest failed : %v\n", err)
	}

	if workerPayload == nil {
		log.Fatal("You did not provide input files\n")
	}
	resultValues, err := workerPayload.Finalize()
	if err != nil {
		log.Fatalf("failed to compute t values : %v\n", err)
	}

	fmt.Printf("First t values are %v\n", resultValues[:10])

	//TTest done, write/plot results

	doesFileExists := func(path string) bool {
		_, err := os.Stat(path)
		return !os.IsNotExist(err)
	}
	fileExtension := ""
	outPath := filepath.Dir(app.out)
	tokens := strings.Split(filepath.Base(app.out), ".")
	if len(tokens) > 1 {
		fileExtension = tokens[1]
	}
	nameCandidate := filepath.Base(app.out)
	suffix := 1
	fileNameCollision := doesFileExists(app.out)
	for fileNameCollision && suffix < 100 {
		//file with *out as name already exists
		nameCandidate = fmt.Sprintf("%v-%v.%v", strings.Split(path.Base(app.out), ".")[0], suffix, fileExtension)
		fileNameCollision = doesFileExists(filepath.Join(outPath, nameCandidate))
		if fileNameCollision {
			suffix++
		}

	}
	if fileNameCollision {
		log.Printf("Filename collision avoidance failed, overwriting\n")
	} else if suffix > 1 {
		fmt.Printf("Detected name colision, renamed %v to %v\n", path.Base(app.out), nameCandidate)
	}

	app.out = filepath.Join(outPath, nameCandidate)
	outFile, err := os.Create(filepath.Join(outPath, nameCandidate))
	if err != nil {
		log.Printf("%v\n", err)
		fmt.Printf("Failed to write to %v, dumping data to console instead\nlength=%v\n%v\n", app.out, len(resultValues), resultValues)
		return
	}
	defer func() {
		if err := outFile.Close(); err != nil {
			log.Printf("Failed to close %v : %v", outFile.Name(), err)
		}
	}()

	tValuesAsStrings := make([]string, len(resultValues))
	for i := range resultValues {
		tValuesAsStrings[i] = fmt.Sprintf("%f", resultValues[i])
	}
	csvWriter := csv.NewWriter(outFile)
	if err := csvWriter.Write(tValuesAsStrings); err != nil {
		fmt.Printf("Failed to write to outputfile  %v : %v\n.Dumping data to console instead\nlength=%v\n%v\n", app.out, err, len(resultValues), resultValues)
		return
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		fmt.Printf("Failed to flush to outputfile %v : %v\n.Dumping data to console instead\nlength=%v\n%v\n", app.out, err, len(resultValues), resultValues)
		return
	}

	//create plot
	plotPath := filepath.Join(outPath, strings.Split(nameCandidate, ".")[0]+"-plot.png")
	fmt.Printf("Storing plot in %v\n", plotPath)
	plotFile, err := os.Create(plotPath)
	if err != nil {
		fmt.Printf("Failed to create plot file : %v\n", err)
		return
	}
	defer func() {
		if err := plotFile.Close(); err != nil {
			log.Printf("Failed to close %v : %v", plotFile.Name(), err)
		}
	}()

	if err := tPlot.PlotAndStore(resultValues, app.tTestThreshold, plotFile); err != nil {
		fmt.Printf("Failed to save plot :%v", err)
		return
	}

}
