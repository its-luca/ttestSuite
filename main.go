package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/pbnjay/memory"
	"io/ioutil"
	"log"
	"math"
	"path"
	"runtime"
	"sync"
	"time"
)

//Si unit prefix
const Giga = 1024 * 1024 * 1024

const (
	offsetOfOffsetCurveBuffer4BInt = 0x010
	offsetOfNumberOfFF4BUint       = 0x048
	offsetOfFormat4BInt            = 0x0f0
	//relative to start of curve buffer
	//offsetOfPrechargeStart4BUint = 0x332
	//relative to start of curve buffer
	offsetOfOffsetDataStart4BUint = 0x336
	//voltage = (datapoint*scale)+offset
	offsetOfDimScale8BDouble = 0x0a8
	//voltage = (datapoint*scale)+offset
	offsetOfDimOffset8Double      = 0x0b0
	offsetHorizontalDimSize4BUint = 0x1f8
	//value for NaN
	//offsetOfNValue4B              = 0x0f8
	offsetOfOffsetPostchargeStart = 0x33a
	offsetOfOffsetPostchargeStop  = 0x33e
)

type Config struct {
	//number of compute workers to spawn; increase if not cpu gated
	ComputeWorkers int
	//number of feeder workers to spawn; increase if not I/O gated
	FeederWorkers int
	//controls buffer (unit trace files) available to FeederWorkers; increase to fill RAM for max performance
	BufferSizeInGB int
}

func maxInt(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

//based on tectronix wfm spec, has basically zero error checks right now but seems to work
func wfmToTraces(rawWFM []byte, frames [][]float64) ([][]float64, error) {

	numberOfFF := binary.LittleEndian.Uint32(rawWFM[offsetOfNumberOfFF4BUint : offsetOfNumberOfFF4BUint+4])
	//log.Printf("Number of fast frames is %v\n",numberOfFF)

	datapointsPerFF := int(binary.LittleEndian.Uint32(rawWFM[offsetHorizontalDimSize4BUint : offsetHorizontalDimSize4BUint+4]))
	//log.Printf("Datapoints per fast frame is  %v\n",datapointsPerFF)

	formatIdentifier := int(binary.LittleEndian.Uint32(rawWFM[offsetOfFormat4BInt : offsetOfFormat4BInt+4]))
	if formatIdentifier != 0 {
		return nil, fmt.Errorf("format identifier has unexpected value. this will be resolved in future version")
	}
	//log.Printf("Format identifier is   %v\n", formatIdentifier)

	//naNValue := uint16(binary.LittleEndian.Uint32(rawWFM[offsetOfNValue4B:offsetOfNValue4B+4]))
	//log.Printf("naNValue  is  %v\n",naNValue)

	yScale := math.Float64frombits(binary.LittleEndian.Uint64(rawWFM[offsetOfDimScale8BDouble : offsetOfDimScale8BDouble+8]))
	yOffset := math.Float64frombits(binary.LittleEndian.Uint64(rawWFM[offsetOfDimOffset8Double : offsetOfDimOffset8Double+8]))

	offsetCurveBuffer := int(binary.LittleEndian.Uint32(rawWFM[offsetOfOffsetCurveBuffer4BInt : offsetOfOffsetCurveBuffer4BInt+4]))
	offsetDataStart := int(binary.LittleEndian.Uint32(rawWFM[offsetOfOffsetDataStart4BUint : offsetOfOffsetDataStart4BUint+4]))
	offsetPostStartOffset := int(binary.LittleEndian.Uint32(rawWFM[offsetOfOffsetPostchargeStart : offsetOfOffsetPostchargeStart+4]))
	offsetPostStopOffset := int(binary.LittleEndian.Uint32(rawWFM[offsetOfOffsetPostchargeStop : offsetOfOffsetPostchargeStop+4]))

	postChargeBytes := offsetPostStopOffset - offsetPostStartOffset
	preChargeBytes := offsetDataStart
	//log.Printf("postcharge length is %v\n",postChargeBytes)
	//log.Printf("precharge length is %v\n",preChargeBytes)
	//log.Printf("data start = %x post start = %x\n => expecting %vdatapoints",offsetDataStart,offsetPostStartOffset,(offsetPostStartOffset-offsetDataStart)/2)

	if frames == nil {
		frames = make([][]float64, numberOfFF)
	}

	for frameIDX := range frames {
		start := offsetCurveBuffer + (frameIDX+1)*preChargeBytes + (frameIDX * datapointsPerFF * 2) + frameIDX*postChargeBytes
		end := start + (datapointsPerFF * 2)
		rawFrame := rawWFM[start:end]
		if frames[frameIDX] == nil {
			frames[frameIDX] = make([]float64, datapointsPerFF)
		}
		for i := range frames[frameIDX] {
			rawValue := int16(binary.LittleEndian.Uint16(rawFrame[2*i : 2*i+2]))
			frames[frameIDX][i] = (float64(rawValue) * yScale) + yOffset
		}
	}
	return frames, nil
}

//array with num traces entries. 1 stands for random case, 0 for fixed case
func parseCaseLog(rawLog []byte) ([]int, error) {
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

type workerContext struct {
	//feeders write, compute workers read; close once both of them are done
	jobs chan *FileLogBundle
	//writeable by feeders and compute workers; indicate error has happened and others should shut down
	shouldQuit chan interface{}
	//send error information; does not lead to shutdown unless you write to shouldQuit
	errorChan chan error
	//count of active workers
	wg *sync.WaitGroup
}

//reads trace files [start,end[ and puts them to job chan
func feederWorker(feederID, start, end, tracesPerFile int, traceSource TraceBlockReader, caseLog []int, wCtx workerContext) {
	defer func() {
		log.Printf("Feeder %v done\n", feederID)
		wCtx.wg.Done()
	}()
	log.Printf("Feeder %v started on range [%v,%v[\n", feederID, start, end)
	traceBlockIDX := start
	for {
		select {
		case <-wCtx.shouldQuit:
			log.Printf("Feeder quits because there was an error")
			return
		default:
			{
				if traceBlockIDX >= end {
					//don't close jobs channel as there might me other feeder workers left
					return
				}
				// tlog.Printf("Feeder is processing traceBlockIDX %v\n",traceBlockIDX)
				startTime := time.Now()
				//read file
				rawWFM, err := traceSource.GetBlock(traceBlockIDX)
				if err != nil {
					wCtx.errorChan <- fmt.Errorf("failed to read wfm file : %v", err)
					wCtx.shouldQuit <- nil
					return
				}
				//get case data for file
				startIDX := traceBlockIDX * tracesPerFile
				stopIDX := (traceBlockIDX + 1) * tracesPerFile
				//log.Printf("traceBlockIDX %v startIDX %v stopIDX %v total len %v\n",traceBlockIDX,startIDX,stopIDX,len(caseLog))
				flb := &FileLogBundle{
					fileIDX:    traceBlockIDX,
					file:       rawWFM,
					subCaseLog: caseLog[startIDX:stopIDX],
				}
				log.Printf("feeder prepared job  %v in %v\n", traceBlockIDX, time.Since(startTime))
				wCtx.jobs <- flb
				log.Printf("feeder prepared+enqured job  %v in %v\n", traceBlockIDX, time.Since(startTime))
				traceBlockIDX++
			}
		}
	}
}

//consumes wCtx.jobs and writes result to resultChan once in the end
func computeWorker(workerID, tracesPerFile int, resultChan chan<- *BatchMeanAndVar, wCtx workerContext) {
	defer func() {
		log.Printf("Worker %v done\n", workerID)
		wCtx.wg.Done()
	}()
	log.Printf("Worker %v started\n", workerID)
	var frames [][]float64
	var batchMeAndAndVar *BatchMeanAndVar
	fixedTraces := make([][]float64, 0, tracesPerFile/2)
	randomTraces := make([][]float64, 0, tracesPerFile/2)
	var err error
	for {
		select {
		case <-wCtx.shouldQuit:
			log.Printf("Worker %v received error message and is shuttding down\n", workerID)
			return
		case workPackage := <-wCtx.jobs:
			{
				//channel has been closed, no more jobs are coming; send results and finish
				if workPackage == nil {
					log.Printf("compute worker %v waiting to send result", workerID)
					resultChan <- batchMeAndAndVar
					return
				}
				start := time.Now()
				//log.Printf("Worker %v received job for fileIDX %v\n",workerID,workPackage.fileIDX)
				//load data; fileIDX+1 to stick to previous naming convention
				frames, err = wfmToTraces(workPackage.file, frames)
				if err != nil {
					wCtx.errorChan <- fmt.Errorf("worker %v : failed to parse wfm file : %v", workerID, err)
					wCtx.shouldQuit <- nil
					return
				}

				//reset slices (but keep backing memory) and classify traces
				randomTraces = randomTraces[:0]
				fixedTraces = fixedTraces[:0]
				for i := range frames {
					if workPackage.subCaseLog[i] == 1 {
						randomTraces = append(randomTraces, frames[i])
					} else {
						fixedTraces = append(fixedTraces, frames[i])
					}
				}

				//update mean and var values
				if batchMeAndAndVar == nil {
					batchMeAndAndVar, err = NewBatchMeanAndVar(fixedTraces, randomTraces)
					if err != nil {
						wCtx.errorChan <- fmt.Errorf("worker %v : failed to init batch mean and var : %v", workerID, err)
						wCtx.shouldQuit <- nil
						return
					}
				} else {
					batchMeAndAndVar.Update(fixedTraces, randomTraces)
				}
				log.Printf("worker %v processed job %v in %v\n", workerID, workPackage.fileIDX, time.Since(start))
			}
		}
	}
}

//raw trace file together with it's case indicator (fixed or random)
type FileLogBundle struct {
	fileIDX    int
	file       []byte
	subCaseLog []int
}

type TraceBlockReader interface {
	TotalBlockCount() int
	GetBlock(nr int) ([]byte, error)
}

type TraceFileReader struct {
	totalFileCount int
	folderPath     string
	idToFileName   func(id int) string
}

func NewDefaultTraceFileReader(fileCount int, folderPath string) *TraceFileReader {
	return &TraceFileReader{
		totalFileCount: fileCount,
		folderPath:     folderPath,
		idToFileName: func(id int) string {
			return fmt.Sprintf("trace (%v).wfm", id+1)
		},
	}
}

func (recv *TraceFileReader) TotalBlockCount() int {
	return recv.totalFileCount
}

func (recv *TraceFileReader) GetBlock(nr int) ([]byte, error) {
	//one test parse to get traces per file
	fileContent, err := ioutil.ReadFile(path.Join(recv.folderPath, recv.idToFileName(nr)))
	if err != nil {
		return nil, fmt.Errorf("failed to read wfm file : %v", err)
	}
	return fileContent, nil
}

func TTest(caseLog []int, traceSource TraceBlockReader, config Config) (*BatchMeanAndVar, error) {
	numTraceFiles := traceSource.TotalBlockCount()

	testFile, err := traceSource.GetBlock(0)
	if err != nil {
		return nil, fmt.Errorf("failed to read test file : %v", err)
	}
	testFrames, err := wfmToTraces(testFile, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to parse wfm file")
	}

	tracesPerFile := len(testFrames)
	fmt.Printf("Traces per file: %v\n", tracesPerFile)

	const byteToGB = 1024 * 1024 * 1024

	jobs := make(chan *FileLogBundle, maxInt(1, config.BufferSizeInGB/(len(testFile)*byteToGB)))
	errorChan := make(chan error)
	shouldQuit := make(chan interface{})
	resultChan := make(chan *BatchMeanAndVar, config.ComputeWorkers)
	var computeWorkerWg, feederWorkerWg sync.WaitGroup

	computeWorkerCtx := workerContext{
		jobs:       jobs,
		shouldQuit: shouldQuit,
		errorChan:  errorChan,
		wg:         &computeWorkerWg,
	}

	feederWorkerCtx := workerContext{
		jobs:       jobs,
		shouldQuit: shouldQuit,
		errorChan:  errorChan,
		wg:         &feederWorkerWg,
	}

	//create ComputeWorkers
	for id := 0; id < config.ComputeWorkers; id++ {
		computeWorkerCtx.wg.Add(1)
		go computeWorker(id, tracesPerFile, resultChan, computeWorkerCtx)
	}

	//create feeders
	blockSize := numTraceFiles / config.FeederWorkers
	for id := 0; id < config.FeederWorkers; id++ {
		feederWorkerCtx.wg.Add(1)
		startIDX := id * blockSize
		endIDX := minInt(startIDX+blockSize, numTraceFiles)
		go feederWorker(id, startIDX, endIDX, tracesPerFile, traceSource, caseLog, feederWorkerCtx)
	}

	go func() {
		for recError := range errorChan {
			log.Printf("Received error : %v", recError)
		}
	}()

	feederWorkerCtx.wg.Wait()
	log.Printf("wait for feeder workers done\n")
	//all feeders must be done before we can close the jobs chan
	close(jobs)
	log.Printf("closed jobs channel, waiting wor compute workers to finish\n")
	//wait for compute workers to finish outstanding jobs
	computeWorkerCtx.wg.Wait()
	log.Printf("compute workers finished\n")
	//all workers done, no one can send errors anymore
	close(errorChan)

	log.Printf("Merging results\n")
	var combined *BatchMeanAndVar
	//Relies on resultChan having at least a buffer of ComputeWorkers
	for i := 0; i < config.ComputeWorkers; i++ {
		//nil happens when there are less files than ComputeWorkers
		if combined == nil {
			combined = <-resultChan
		} else {
			combined = MergeBatchMeanAndVar(combined, <-resultChan)
		}
	}
	close(resultChan)

	if combined == nil {
		return nil, fmt.Errorf("no results from workers")
	}

	return combined, nil
}

func main() {
	pathTraceFolder := flag.String("traceFolder", "", "Path to folder containing traces files (names trace (v).wfm where v is an incrementing number (starting at 1) in sync with the case log file")
	numTraces := flag.Int("traceFileCount", 0, "Number of trace files")
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
	if *numTraces == 0 {
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

	traceFileReader := NewDefaultTraceFileReader(*numTraces, *pathTraceFolder)

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
