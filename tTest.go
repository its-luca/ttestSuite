package main

//Parallelized Computation of Welch's T-test

import (
	"fmt"
	"log"
	"sync"
	"time"
	"wfmParser/traceSource"
	"wfmParser/wfm"
)

//channels/wait groups for communication
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
func feederWorker(feederID, start, end int, traceSource traceSource.TraceBlockReader, wCtx workerContext) {
	defer func() {
		log.Printf("Feeder %v done\n", feederID)
		wCtx.wg.Done()
	}()
	log.Printf("Feeder %v started on range [%v,%v[\n", feederID, start, end)
	traceBlockIDX := start
	metricsTicker := time.NewTicker(5 * time.Second)
	processedElements := 0
	readTimeSinceLastTick := 0 * time.Second
	enqueueWaitSinceLastTick := 0 * time.Second
	for {
		select {
		case <-wCtx.shouldQuit:
			log.Printf("Feeder quits because there was an error")
			return
		case <-metricsTicker.C:
			log.Printf("Feeder %v:\t avg read time %v\t avg enq wait %v\t progress %v/%v\n", feederID,
				readTimeSinceLastTick/time.Duration(processedElements), enqueueWaitSinceLastTick/time.Duration(processedElements),
				traceBlockIDX-start, end-start)
			processedElements = 0
			readTimeSinceLastTick = 0
			enqueueWaitSinceLastTick = 0
		default:
			{
				if traceBlockIDX >= end {
					//don't close jobs channel as there might me other feeder workers left
					return
				}
				startTime := time.Now()
				//read file
				rawWFM, caseLog, err := traceSource.GetBlock(traceBlockIDX)
				if err != nil {
					wCtx.errorChan <- fmt.Errorf("failed get trace block/file with id %v : %v", traceBlockIDX, err)
					wCtx.shouldQuit <- nil
					return
				}
				flb := &FileLogBundle{
					fileIDX:    traceBlockIDX,
					file:       rawWFM,
					subCaseLog: caseLog,
				}
				readTimeSinceLastTick += time.Since(startTime)
				startTime = time.Now()
				wCtx.jobs <- flb
				enqueueWaitSinceLastTick += time.Since(startTime)
				processedElements++
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
				//start := time.Now()
				//log.Printf("Worker %v received job for fileIDX %v\n",workerID,workPackage.fileIDX)
				//load data; fileIDX+1 to stick to previous naming convention
				frames, err = wfm.WFMToTraces(workPackage.file, frames)
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
					batchMeAndAndVar = NewBatchMeanAndVar(len(frames[0]))
				}
				batchMeAndAndVar.Update(fixedTraces, randomTraces)

				//log.Printf("worker %v processed job %v in %v\n", workerID, workPackage.fileIDX, time.Since(start))
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

//TTest, is a parallelized implementation of Welch's T-test
func TTest(traceSource traceSource.TraceBlockReader, config Config) (*BatchMeanAndVar, error) {
	numTraceFiles := traceSource.TotalBlockCount()

	testFile, _, err := traceSource.GetBlock(0)
	if err != nil {
		return nil, fmt.Errorf("failed to read test file : %v", err)
	}

	tracesPerFile := wfm.GetNumberOfTraces(testFile)
	fmt.Printf("Traces per file: %v\n", tracesPerFile)

	jobQueueLen := maxInt(1, (config.BufferSizeInGB*1024)/(len(testFile)/Mega))
	log.Printf("jobQueueLen %v (buff\n", jobQueueLen)
	jobs := make(chan *FileLogBundle, jobQueueLen)
	errorChan := make(chan error)
	shouldQuit := make(chan interface{})
	resultChan := make(chan *BatchMeanAndVar, config.ComputeWorkers)
	var computeWorkerWg, feederWorkerWg sync.WaitGroup

	//stats ticker
	statsTicker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-shouldQuit:
				return
			case <-statsTicker.C:
				{
					log.Printf("Buffer Usage %v out of %v\n", len(jobs), cap(jobs))
				}
			}
		}
	}()

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
		go feederWorker(id, startIDX, endIDX, traceSource, feederWorkerCtx)
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
	shouldQuit <- nil
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
			combined.MergeBatchMeanAndVar(<-resultChan)
		}
	}
	close(resultChan)

	if combined == nil {
		return nil, fmt.Errorf("no results from workers")
	}

	return combined, nil
}
