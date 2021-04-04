package main

import (
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
)

//Si unit prefix
const Giga = 1024 * 1024 * 1024
const Mega = 1024 * 1024

//Defines CPU and memory usage
type Config struct {
	//number of compute workers to spawn; increase if not cpu gated
	ComputeWorkers int
	//number of feeder workers to spawn; increase if not I/O gated
	FeederWorkers int
	//controls buffer (unit trace files) available to FeederWorkers; increase to fill RAM for max performance
	BufferSizeInGB int
}

func main() {
	pathTraceFolder := flag.String("traceFolder", "", "Path to folder containing traces files (names trace (v).wfm where v is an incrementing number (starting at 1) in sync with the case log file")
	traceFileCount := flag.Int("traceFileCount", 0, "Number of trace files. Ignored in streaming setup")
	pathCaseLogFile := flag.String("caseFile", "", "Path to the trace file (either 0 or 1, one entry per line")
	numWorkers := flag.Int("numWorkers", runtime.NumCPU()-1, "Number of threads to t-test computation (also note numFeeders). Influences CPU usage")
	numFeeders := flag.Int("numFeeders", 1, "Number of threads for reading input files (in our lab reading a single file does not max out network connectivity). Influences I/O usage")
	fileBufferInGB := flag.Int("fileBufferInGB", maxInt(1, int(memory.TotalMemory()/Giga)-10), "Memory allowed for buffering input files in GB")
	streamFromAddr := flag.String("streamFromAddr", "", "If set, we will listen on the provided addr to receive updates about file availability")
	out := flag.String("out", "./t-values.csv", "Path t-test result file")
	tTestThreshold := flag.Float64("tTestThresh", 6, "Threshold value for t test plot")

	flag.Parse()

	if *pathTraceFolder == "" {
		fmt.Printf("Please set path to trace folder\n")
		flag.PrintDefaults()
		return
	}
	if *traceFileCount <= 0 && *streamFromAddr == "" {
		fmt.Printf("Please set number of trace files to a positive number\n")
		flag.PrintDefaults()
		return
	}

	if *pathCaseLogFile == "" {
		fmt.Printf("Please set path to case log file\n")
		flag.PrintDefaults()
		return
	}

	if *numWorkers < 0 {
		fmt.Printf("Please set numWorkers to a numer in [1,%v[\n", runtime.NumCPU()-1)
		flag.PrintDefaults()
		return
	}
	if *numWorkers > runtime.NumCPU()-1 {
		fmt.Printf("numWorkers is set to %v but (considering the feeder thread) you only have %v vcores left. This will add threading overhead\n", *numWorkers, runtime.NumCPU()-1)
	}

	if *numFeeders < 1 {
		fmt.Printf("You neeed at least one feeder!")
		flag.PrintDefaults()
		return
	}

	if *fileBufferInGB < 1 {
		fmt.Printf("Your file buffer is too small")
		flag.PrintDefaults()
		return
	}
	if *fileBufferInGB > int(memory.TotalMemory()/Giga) {
		fmt.Printf("Your file buffer is large than the available memory!")
		flag.PrintDefaults()
		return
	}

	config := Config{
		ComputeWorkers: *numWorkers,
		FeederWorkers:  *numFeeders,
		BufferSizeInGB: *fileBufferInGB,
	}

	var traceReader traceSource.TraceBlockReader
	if *streamFromAddr == "" {
		var err error
		traceReader, err = traceSource.NewDefaultTraceFileReader(*traceFileCount, *pathTraceFolder, filepath.Base(*pathCaseLogFile))
		if err != nil {
			log.Fatalf("failed to create trace file reader : %v", err)
		}
	} else {
		var filenameUpdates <-chan string

		receiver := httpReceiver.NewReceiver()
		srv := &http.Server{
			Addr:    *streamFromAddr,
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
			log.Printf("initiating shutdown")
			receiverCancel()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				log.Printf("gracefull server shutdown failed : %v\n", err)
			}
		}()

		*traceFileCount, filenameUpdates = receiver.WaitForMeasureStart(receiverCtx)

		traceReader = traceSource.NewStreamingTraceFileReader(*traceFileCount, *pathTraceFolder, filepath.Base(*pathCaseLogFile), filenameUpdates)
	}

	batchMeanAndVar, err := TTest(traceReader, config)
	if err != nil {
		log.Fatalf("Ttest failed : %v\n", err)
	}

	if batchMeanAndVar == nil {
		log.Fatal("You did not provide input files\n")
	}
	//Calc t test values
	tValues := batchMeanAndVar.ComputeLQ()

	fmt.Printf("First t values are %v\n", tValues[:10])

	doesFileExists := func(path string) bool {
		_, err := os.Stat(path)
		return !os.IsNotExist(err)
	}
	fileExtension := ""
	outPath := filepath.Dir(*out)
	tokens := strings.Split(filepath.Base(*out), ".")
	if len(tokens) > 1 {
		fileExtension = tokens[1]
	}
	nameCandidate := filepath.Base(*out)
	suffix := 1
	fileNameCollision := doesFileExists(*out)
	for fileNameCollision && suffix < 100 {
		//file with *out as name already exists
		nameCandidate = fmt.Sprintf("%v-%v.%v", strings.Split(path.Base(*out), ".")[0], suffix, fileExtension)
		fileNameCollision = doesFileExists(filepath.Join(outPath, nameCandidate))
		if fileNameCollision {
			suffix++
		}

	}
	if fileNameCollision {
		log.Printf("Filename collision avoidance failed, overwriting\n")
	} else if suffix > 1 {
		fmt.Printf("Detected name colision, renamed %v to %v\n", path.Base(*out), nameCandidate)
	}

	*out = filepath.Join(outPath, nameCandidate)
	outFile, err := os.Create(filepath.Join(outPath, nameCandidate))
	if err != nil {
		log.Printf("%v\n", err)
		fmt.Printf("Failed to write to %v, dumping data to console instead\nlength=%v\n%v\n", *out, len(tValues), tValues)
		return
	}
	defer func() {
		if err := outFile.Close(); err != nil {
			log.Printf("Failed to close %v : %v", outFile.Name(), err)
		}
	}()

	tValuesAsStrings := make([]string, len(tValues))
	for i := range tValues {
		tValuesAsStrings[i] = fmt.Sprintf("%f", tValues[i])
	}
	csvWriter := csv.NewWriter(outFile)
	if err := csvWriter.Write(tValuesAsStrings); err != nil {
		fmt.Printf("Failed to write to outputfile  %v : %v\n.Dumping data to console instead\nlength=%v\n%v\n", *out, err, len(tValues), tValues)
		return
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		fmt.Printf("Failed to flush to outputfile %v : %v\n.Dumping data to console instead\nlength=%v\n%v\n", *out, err, len(tValues), tValues)
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

	if err := tPlot.PlotAndStore(tValues, *tTestThreshold, plotFile); err != nil {
		fmt.Printf("Failed to save plot :%v", err)
		return
	}

}
