package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"ttestSuite/payloadComputation"
	"ttestSuite/traceSource"
	"ttestSuite/wfm"
)

type partialTTestState struct {
	pwMeanFixed  []float64
	pwMeanRandom []float64
	countFixed   float64
	countRandom  float64
}

//loadMeansFromTTestStateCSV parses csv an returns partialTTestState
func loadMeansFromTTestStateCSV(r io.Reader) (*partialTTestState, error) {
	csvReader := csv.NewReader(r)

	//disable "all fields must have same entry count" check
	csvReader.FieldsPerRecord = -1
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}
	if cnt := len(records); cnt != 7 {
		return nil, fmt.Errorf("csv only has %v records, expected 7", cnt)
	}
	lenFixed, err := strconv.ParseFloat(records[0][0], 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse lenFixed : %v", err)
	}
	lenRandom, err := strconv.ParseFloat(records[1][0], 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse lenRandom : %v", err)
	}
	pwSumFixedAsStr := records[2]
	pwSumRandomAsStr := records[3]
	if len(pwSumFixedAsStr) != len(pwSumRandomAsStr) {
		return nil, fmt.Errorf("pwSum* entries have different lenghts")
	}

	pwMeanFixed := make([]float64, len(pwSumFixedAsStr))
	for i, v := range pwSumFixedAsStr {
		pwMeanFixed[i], err = strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert %v to float64", v)
		}
		pwMeanFixed[i] /= lenFixed
	}
	pwMeanRandom := make([]float64, len(pwSumRandomAsStr))
	for i, v := range pwSumRandomAsStr {
		pwMeanRandom[i], err = strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert %v to float64", v)
		}
		pwMeanRandom[i] /= lenRandom
	}

	s := &partialTTestState{
		pwMeanFixed:  pwMeanFixed,
		pwMeanRandom: pwMeanRandom,
		countFixed:   lenFixed,
		countRandom:  lenRandom,
	}
	return s, nil

}

//StoreAsCSV writes v to w in csv format
func StoreAsCSV(v []float64, w io.Writer) error {
	//convert slice to strings
	vAsString := make([]string, len(v))
	for i := range v {
		vAsString[i] = fmt.Sprintf("%f", v[i])
	}

	csvWriter := csv.NewWriter(w)
	if err := csvWriter.Write(vAsString); err != nil {
		return fmt.Errorf("csv write failed: %v", err)
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return fmt.Errorf("csv write failed: %v", err)
	}
	return nil
}

type job struct {
	pwMeanFixed []float64
	trace       []float64
	resPTR      *float64
}

type blockBundle struct {
	caseMarkers []int
	rawTraces   []byte
}

//computeCorrelation, computes correlation between s.pwMeanFixed and all the fixed case traces returned by traceReader
func computeCorrelation(traceReader traceSource.TraceBlockReader, parser wfm.TraceParser, traceFileCount int, s *partialTTestState) ([]float64, error) {
	//float64 ok as the normalized value is small; DO NOT CHANGE SIZE as we use pointers to
	//individual elements to store results in the  worker function
	normalizedCorrelationFixed := make([]float64, int(s.countFixed))

	//create context that closes done when OS signal arrives or main is done
	mainCtx, mainCtxCancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer mainCtxCancel()

	workers, mainCtx := errgroup.WithContext(mainCtx)

	//fed by decoder, processed by workers
	jobs := make(chan job, 2*runtime.NumCPU())
	//fed by feeder, processed by decoder
	input := make(chan blockBundle, 10)

	//spawn workers
	workerCount := runtime.NumCPU() - 2
	if workerCount <= 0 {
		workerCount = 1
	}
	for i := 0; i < workerCount; i++ {
		workers.Go(func() error {
			for j := range jobs {
				corr, err := payloadComputation.NormalizedCrossCorrelationBig(j.pwMeanFixed, j.trace)
				if err != nil {
					log.Printf("wokrer failed to calc corr: %v", err)
					return fmt.Errorf("failed to calc correlation : %v", err)
				}
				//write back result
				floatCorr, _ := corr.Float64()
				*(j.resPTR) = floatCorr
			}
			log.Printf("worker detected close job channel")
			return nil
		})
	}

	//spawn feeder
	workers.Go(func() error {
		defer close(input)
		for fileIDX := 0; fileIDX < traceFileCount; fileIDX++ {
			select {
			case <-mainCtx.Done():
				return fmt.Errorf("exiting due to OS signal")
			default:
				log.Printf("Processing fileIDX %v of %v\n", fileIDX, traceFileCount-1)
				rawTraces, cases, err := traceReader.GetBlock(fileIDX)
				if err != nil {
					return fmt.Errorf("failed to read block %v: %v", fileIDX, err)
				}
				input <- blockBundle{
					caseMarkers: cases,
					rawTraces:   rawTraces,
				}
			}
		}
		return nil
	})

	//spawn decoder
	workers.Go(func() error {
		var frameBuf [][]float64
		indexNextFixedResult := 0
		defer func() {
			log.Printf("decoder calls close on jobs")
			close(jobs)
		}()
		for {
			select {
			case <-mainCtx.Done():
				return fmt.Errorf("exiting due to OS signal")
			case in, ok := <-input:
				if !ok {
					return nil
				}

				parsedTraces, err := parser.ParseTraces(in.rawTraces, frameBuf)
				if err != nil {
					return fmt.Errorf("failed to parse block : %v", err)
				}
				for i, caseMarker := range in.caseMarkers {
					if caseMarker != 0 {
						continue
					}
					select { //without select, if all workers have an error jobs could run full and we would be stuck
					//forever without checking the context
					case <-mainCtx.Done():
						return fmt.Errorf("exiting due to OS signal")
					default:
						jobs <- job{
							pwMeanFixed: s.pwMeanFixed,
							trace:       parsedTraces[i],
							//important that normalizedCorrelationFixed is pre-allocated and does not change size while
							//workers run
							resPTR: &normalizedCorrelationFixed[indexNextFixedResult],
						}
						indexNextFixedResult++
					}

				}
			}
		}
	})

	if err := workers.Wait(); err != nil {
		mainCtxCancel()
		return nil, fmt.Errorf("error in worker or abort signal from os: %v", err)
	}
	return normalizedCorrelationFixed, nil
}

func main() {

	//declare and check cli options

	pathTraceFolder := flag.String("traceFolder", "", "Path to folder containing wfm trace files (expected naming scheme is trace (v).wfm where v is an incrementing number (starting at 1) in sync with the case log file")
	traceFileCount := flag.Int("traceFileCount", 0, "Number of trace files. Ignored in streaming setup")
	nameCaseLogFile := flag.String("caseFile", "", "Name of  the trace file (either 0 or 1, one entry per line)")
	out := flag.String("out", "", "path to where results are stored as csv")
	pathCSVStateFile := flag.String("csvStateFilePath", "", "Path to csv file with ttest partialTTestState")

	flag.Parse()

	if *pathTraceFolder == "" {
		fmt.Printf("Specify pathTraceFolder")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *traceFileCount <= 0 {
		fmt.Printf("traceFileCount must be > 0")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *pathCSVStateFile == "" {
		fmt.Printf("Specify  athCSVStateFile")
		flag.PrintDefaults()
		os.Exit(1)
	}
	if *out == "" {
		fmt.Printf("Specify out ")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *nameCaseLogFile == "" {
		fmt.Printf("Specify nameCaseLogFile ")
		flag.PrintDefaults()
		os.Exit(1)
	}

	//prepare arguments for computeCorrelation

	//do this now so we don't have to react to error after result is calculated
	outFile, err := os.Create(*out)
	if err != nil {
		log.Fatalf("Failed to open output file : %v", err)
	}
	defer func() {
		if err := outFile.Close(); err != nil {
			log.Printf("Failed to close outFile : %v", err)
		}
	}()

	csvStateFile, err := os.Open(*pathCSVStateFile)
	if err != nil {
		log.Fatalf("Failed to open csv partialTTestState file : %v", err)
	}
	defer func() {
		if err := csvStateFile.Close(); err != nil {
			log.Printf("Failed to close csvStateFile : %v", err)
		}
	}()

	state, err := loadMeansFromTTestStateCSV(csvStateFile)
	if err != nil {
		log.Fatalf("Failed to parse partialTTestState file : %v", err)
	}
	log.Printf("first  entries are %v\n", state.pwMeanFixed[:10])

	traceReader, err := traceSource.NewDefaultTraceFileReader(*traceFileCount, *pathTraceFolder, filepath.Base(*nameCaseLogFile), false)
	if err != nil {
		log.Fatalf("Failed to create trace file reader : %v", err)
	}

	normalizedCorrelationFixed, err := computeCorrelation(traceReader, wfm.Parser{}, *traceFileCount, state)
	if err != nil {
		log.Fatalf("Failed to compute correlation: %v", err)
	}

	if err := StoreAsCSV(normalizedCorrelationFixed, outFile); err != nil {
		log.Printf("failed to store result as csv, printing to cmdline")
		fmt.Printf("%v\n", normalizedCorrelationFixed)
		os.Exit(1)
	}

}
