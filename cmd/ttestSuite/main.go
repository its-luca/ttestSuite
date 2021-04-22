//Package main provides an cli interface for ttestSuite
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"github.com/pbnjay/memory"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
	"ttestSuite/payloadComputation"
	"ttestSuite/traceSource"
	"ttestSuite/wfm"
)

//application bundles the command line configuration options
type application struct {
	pathTraceFolder      string
	traceFileCount       int
	pathCaseLogFile      string
	numWorkers           int
	fileBufferInGB       int
	streamFromAddr       string
	outFolderPath        string
	tTestThreshold       float64
	snapshotInterval     int
	workerPayloadCreator payloadComputation.WorkerPayloadCreator
}

//closeWithErrLog is a helper that calls Close on c and prints a log message if an error occurs
func closeWithErrLog(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		log.Printf("failed to close %v : %v", name, err)
	}
}

var errCollisionAvoidanceFailed = errors.New("unable to avoid file/folder name collision, using returned name may overwrite data ")

//defaultCreateCollisionFreeName is a convenience wrapper for createCollisionFreeName checking for
//collision using os.Stat
func defaultCreateCollisionFreeName(outPath string) (string, error) {
	return createCollisionFreeName(outPath, func(path string) bool {
		_, err := os.Stat(path)
		return !os.IsNotExist(err)
	})
}

//createCollisionFreeName checks if outPath already exists and tries to add numbers from 1 to 100 as suffix
//to find a unused name. If all are token errCollisionAvoidanceFailed is returned
func createCollisionFreeName(outPath string, doesFileExist func(path string) bool) (string, error) {
	outPathDir := filepath.Dir(outPath)

	//split filename by "." to separate name and extensions (if it exists, else set it to "")
	fileNameTokens := strings.Split(filepath.Base(outPath), ".")
	fileExtension := ""
	if len(fileNameTokens) > 1 {
		fileExtension = fileNameTokens[1]
	}

	nameCandidate := filepath.Base(outPath)
	suffix := 1
	fileNameCollision := doesFileExist(outPath)
	for fileNameCollision && suffix < 100 {
		//file/folder with *outFolderPath as name already exists
		if fileExtension != "" {
			nameCandidate = fmt.Sprintf("%v-%v.%v", strings.Split(path.Base(outPath), ".")[0], suffix, fileExtension)
		} else {
			nameCandidate = fmt.Sprintf("%v-%v", strings.Split(path.Base(outPath), ".")[0], suffix)
		}
		fileNameCollision = doesFileExist(filepath.Join(outPathDir, nameCandidate))
		if fileNameCollision {
			suffix++
		}

	}
	result := filepath.Join(outPathDir, nameCandidate)
	if fileNameCollision {
		return result, errCollisionAvoidanceFailed
	}

	return result, nil
}

func StorePlot(values []float64, plotable payloadComputation.Plotable, folderPath, nameSuffix string) error {
	//create plot and store as png
	plotPath := filepath.Join(folderPath, fmt.Sprintf("plot-%s.png", nameSuffix))
	fmt.Printf("Storing plot in %v\n", plotPath)
	plotFile, err := os.Create(plotPath)
	if err != nil {
		return fmt.Errorf("failed to create plot file : %v\n", err)

	}
	defer closeWithErrLog(plotFile.Name(), plotFile)

	if err := plotable.Plot(values, plotFile); err != nil {
		return err
	}
	return plotFile.Sync()
}

func StoreRaw(rawPayloadState payloadComputation.WorkerPayload, folderPath, nameSuffix string) error {
	//save rawPayloadState as binary encoding of the struct
	gobFile, err := os.Create(filepath.Join(folderPath, fmt.Sprintf("rawState-%s.bin", nameSuffix)))
	if err != nil {
		return fmt.Errorf("faled to create gob file : %v\n", err)
	}
	defer closeWithErrLog(gobFile.Name(), gobFile)

	if err := rawPayloadState.Encode(gobFile); err != nil {
		return fmt.Errorf("failed to store binary encoding of state : %v", err)
	}
	return gobFile.Sync()
}

func StoreAsCSV(result []float64, folderPath, nameSuffix string) error {
	//store result as csv
	tValuesAsStrings := make([]string, len(result))
	for i := range result {
		tValuesAsStrings[i] = fmt.Sprintf("%f", result[i])
	}
	resultFile, err := os.Create(filepath.Join(folderPath, fmt.Sprintf("values-%s.csv", nameSuffix)))
	if err != nil {
		return fmt.Errorf("Failed to create output file : %v\n.Dumping data to console instead\nlength=%v\n%v\n", err, len(result), result)

	}
	defer closeWithErrLog(resultFile.Name(), resultFile)

	csvWriter := csv.NewWriter(resultFile)
	if err := csvWriter.Write(tValuesAsStrings); err != nil {
		return fmt.Errorf("Failed to write to outputfile  %v : %v\n.Dumping data to console instead\nlength=%v\n%v\n", resultFile.Name(), err, len(result), result)
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return fmt.Errorf("Failed to flush to outputfile %v : %v\n.Dumping data to console instead\nlength=%v\n%v\n", resultFile.Name(), err, len(result), result)

	}
	return resultFile.Sync()
}

func Store(result []float64, rawSnapshot payloadComputation.WorkerPayload, folderPath, suffix string, storeRaw bool) error {
	errList := make([]error, 0)

	if err := StoreAsCSV(result, folderPath, suffix); err != nil {
		errList = append(errList, fmt.Errorf("failed to sae as csv file : %v", err))
	}
	if storeRaw {
		if err := StoreRaw(rawSnapshot, folderPath, suffix); err != nil {
			errList = append(errList, fmt.Errorf("failed to raw data :%v", err))
		}
	}

	if plotable, ok := rawSnapshot.(payloadComputation.Plotable); ok {
		if err := StorePlot(result, plotable, folderPath, suffix); err != nil {
			errList = append(errList, fmt.Errorf("failed to plot: %v", err))
		}
	}

	if len(errList) == 0 {
		return nil
	}

	mergedErrStr := "failed to (fully)save snapshot("
	for _, v := range errList {
		mergedErrStr += v.Error() + ","
	}
	mergedErrStr += ")"
	return errors.New(mergedErrStr)
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
	outFolderPath := cmdFlags.String("outFolderPath", "", "Directory path for saving results. Defaults creating a folder name after pathTraceFolder in the current directory")
	tTestThreshold := cmdFlags.Float64("tTestThresh", 6, "Threshold value for t-test plot")
	snapshotInterval := cmdFlags.Int("snapshotInterval", 0, "Save intermediate result every x trace files")
	payloadName := cmdFlags.String("payloadComputation", "ttest", fmt.Sprintf("Choose which of the following computation should be performed on the data: %s", payloadComputation.GetAvailablePayloads()))
	var workerPayloadCreator payloadComputation.WorkerPayloadCreator
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
		if *fileBufferInGB > int(memory.TotalMemory()/payloadComputation.Giga) {
			descriptiveError = fmt.Errorf("your file buffer is larger than the available memory")
			return
		}

		//set default value, may be changed by file name collision avoidance system
		if *outFolderPath == "" {
			*outFolderPath = fmt.Sprintf("%s-results", filepath.Base(*pathTraceFolder))
		}

		var err error
		workerPayloadCreator, err = payloadComputation.GetWorkerPayloadCreator(*payloadName)
		if err != nil {
			descriptiveError = fmt.Errorf("failed to instantiate payload compuation creator \"%v\": %v", *payloadName, err)
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
		outFolderPath:        *outFolderPath,
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

	//Prepare traceReader
	var traceReader traceSource.TraceBlockReader
	if app.streamFromAddr == "" {
		var err error
		traceReader, err = traceSource.NewDefaultTraceFileReader(app.traceFileCount, app.pathTraceFolder, filepath.Base(app.pathCaseLogFile))
		if err != nil {
			log.Fatalf("Failed to create trace file reader : %v", err)
		}
	} else { //setup streaming mode
		//publish new files here
		var filenameUpdates <-chan string

		//start webserver
		receiver := traceSource.NewReceiver()
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

		//setup graceful shutdown, note that the webserver is started concurrently, else we could not receive the start message
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

		//wait for the measure script to send the start command
		app.traceFileCount, filenameUpdates = receiver.WaitForMeasureStart(receiverCtx)

		traceReader = traceSource.NewStreamingTraceFileReader(app.traceFileCount, app.pathTraceFolder, filepath.Base(app.pathCaseLogFile), filenameUpdates)
	}

	//Prepare output directory for saving files. We do this here so we can already use it in snapshot saver callback
	app.outFolderPath, err = defaultCreateCollisionFreeName(app.outFolderPath)
	if err != nil {
		if errors.Is(err, errCollisionAvoidanceFailed) {
			//deliberate decision to not delete files/folders as the latter might also deleted unexpected files
			//instead we just overwrite
			log.Printf("failed to avoid file name collision, overwriting %v", app.outFolderPath)
		} else {
			app.outFolderPath = filepath.Join(os.TempDir(), strconv.FormatInt(rand.Int63(), 10))
			log.Printf("Failed to generate outputfile name, resorting to %v\n", app.outFolderPath)
		}
	}
	err = os.Mkdir(app.outFolderPath, os.ModePerm)
	if err != nil {
		log.Fatalf("Failed to create output directory %v\n", app.outFolderPath)
		return
	}

	//Call Run function

	config := payloadComputation.ComputationConfig{
		ComputeWorkers:       app.numWorkers,
		BufferSizeInGB:       app.fileBufferInGB,
		SnapshotInterval:     app.snapshotInterval,
		WorkerPayloadCreator: app.workerPayloadCreator,
		SnapshotSaver: func(result []float64, rawSnapshot payloadComputation.WorkerPayload, snapshotIDX int) error {
			return Store(result, rawSnapshot, app.outFolderPath, strconv.FormatInt(int64(snapshotIDX), 10), false)
		},
	}

	//todo: propagate cancellation
	workerPayload, err := payloadComputation.Run(context.TODO(), traceReader, wfm.Parser{}, config)
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

	//Run done, write/plot results
	if err := Store(resultValues, workerPayload, app.outFolderPath, "final", true); err != nil {
		log.Fatalf("Failed to store final resutls : %v", err)
	}

}
