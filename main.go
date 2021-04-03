package main

import (
	"flag"
	"fmt"
	"github.com/pbnjay/memory"
	"io/ioutil"
	"log"
	"runtime"
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
	traceFileCount := flag.Int("traceFileCount", 0, "Number of trace files")
	pathCaseLogFile := flag.String("caseFile", "", "Path to the trace file (either 0 or 1, one entry per line")
	numWorkers := flag.Int("numWorkers", runtime.NumCPU()-1, "Number of threads to t-test computation (also note numFeeders). Influences CPU usage")
	numFeeders := flag.Int("numFeeders", 1, "Number of threads for reading input files (in our lab reading a single file does not max out network connectivity). Influences I/O usage")
	fileBufferInGB := flag.Int("fileBufferInGB", maxInt(1, int(memory.TotalMemory()/Giga)-10), "Memory allowed for buffering input files in GB")

	flag.Parse()

	if *pathTraceFolder == "" {
		fmt.Printf("Please set path to trace folder\n")
		flag.PrintDefaults()
		return
	}
	if *traceFileCount == 0 {
		fmt.Printf("Please set number of trace files\n")
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

	rawCaseFile, err := ioutil.ReadFile(*pathCaseLogFile)
	if err != nil {
		log.Fatalf("Failed to read case file : %v\n", err)
	}
	caseLog, err := parseCaseLog(rawCaseFile)
	if err != nil {
		log.Fatalf("Failed to parse case file : %v\n", err)
	}

	traceFileReader := NewDefaultTraceFileReader(*traceFileCount, *pathTraceFolder)

	batchMeanAndVar, err := TTest(caseLog, traceFileReader, config)
	if err != nil {
		log.Fatalf("Ttest failed : %v\n", err)
	}

	if batchMeanAndVar == nil {
		log.Fatal("You did not provide input files\n")
	}
	//Calc t test values
	tValues := batchMeanAndVar.ComputeLQ()

	fmt.Printf("First t values are %v\n", tValues[:10])

}
