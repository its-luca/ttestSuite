package main

//Parallelized Computation of Welch's T-test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"
	"ttestSuite/traceSource"
	"ttestSuite/wfm"
)

//channels/wait groups for communication
type workerContext struct {
	//feeders write, compute workers read; close once both of them are done
	jobs chan *FileLogBundle
	//closing this indicates to the workers that they should quit
	shouldQuit <-chan interface{}
	//send error information
	errorChan chan error
	//count of active workers
	wg *sync.WaitGroup
}

type snapshot struct {
	data           WorkerPayload
	snapshotIDX    int
	processedFiles int
	workerID       int
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
				//check if all blocks have been read
				if traceBlockIDX >= end {
					//don't close jobs channel as there might me other feeder workers left
					return
				}

				//read block and announce it as job
				startTime := time.Now()
				//read file
				rawWFM, caseLog, err := traceSource.GetBlock(traceBlockIDX)
				if err != nil {
					wCtx.errorChan <- fmt.Errorf("failed get trace block/file with id %v : %v", traceBlockIDX, err)
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
func computeWorker(workerID, tracesPerFile, snapshotInterval, maxSnapshotIDX int, resultChan chan<- WorkerPayload,
	traceParser wfm.TraceParser, wCtx workerContext, snapshotResults chan<- snapshot, workerPayloadCreator WorkerPayloadCreator) {
	defer func() {
		log.Printf("Worker %v done\n", workerID)
		wCtx.wg.Done()
	}()
	log.Printf("Worker %v started\n", workerID)
	var frames [][]float64
	var payload WorkerPayload
	fixedTraces := make([][]float64, 0, tracesPerFile/2)
	randomTraces := make([][]float64, 0, tracesPerFile/2)
	//set to the last snapshot threshold that we have passed
	//used to make sure we do not send data for the same threshhold twice
	curSnapshotIDX := 0
	filesSinceLastSnapshot := 0
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

					//send either as snapshot or as final result
					if curSnapshotIDX <= maxSnapshotIDX {
						var buf WorkerPayload
						if payload != nil {
							buf = payload.DeepCopy()
						}
						snapshotResults <- snapshot{
							data:           buf,
							snapshotIDX:    curSnapshotIDX,
							processedFiles: filesSinceLastSnapshot,
							workerID:       workerID,
						}
					} else {
						resultChan <- payload
					}
					return
				}

				log.Printf("worker %v: processing fileIDX %v\n", workerID, workPackage.fileIDX)
				//if true we won't get any more data contributing to this snapshot and thus can send data and move
				//on to the next interval
				if curSnapshotIDX <= maxSnapshotIDX && workPackage.fileIDX >= (curSnapshotIDX*snapshotInterval+snapshotInterval) {
					log.Printf("worker %v: sending data for snapshotIDX %v : %v files\n", workerID, curSnapshotIDX, filesSinceLastSnapshot)
					var buf WorkerPayload
					if payload != nil {
						buf = payload.DeepCopy()
					}
					snapshotResults <- snapshot{
						data:           buf,
						snapshotIDX:    curSnapshotIDX,
						processedFiles: filesSinceLastSnapshot,
						workerID:       workerID,
					}
					//skip forward to interval to which workPackage.fileIDX belongs
					for workPackage.fileIDX >= (curSnapshotIDX+1)*snapshotInterval {
						curSnapshotIDX++
					}
					log.Printf("worker %v: next snapshotIDX is %v\n", workerID, curSnapshotIDX)

					filesSinceLastSnapshot = 0
					payload = nil
				}

				frames, err = traceParser.ParseTraces(workPackage.file, frames)
				if err != nil {
					wCtx.errorChan <- fmt.Errorf("worker %v : failed to parse wfm file : %v", workerID, err)
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
				if payload == nil {
					payload = workerPayloadCreator(len(frames[0]))
				}

				payload.Update(fixedTraces, randomTraces)
				for i := range randomTraces {
					randomTraces[i] = nil
				}
				for i := range fixedTraces {
					fixedTraces[i] = nil
				}
				filesSinceLastSnapshot++
				log.Printf("worker %v: done processing %v, filesSinceLastSnapshot %v\n", workerID, workPackage.fileIDX, filesSinceLastSnapshot)

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

var ErrInvSnapInterval = errors.New("snapshot interval  large than trace file count")

//TTest is a parallelized implementation of Welch's T-test
func TTest(_ context.Context, traceSource traceSource.TraceBlockReader, traceParser wfm.TraceParser, workerPayloadCreator WorkerPayloadCreator, snapshotSaver func(payload WorkerPayload) error, config Config) (WorkerPayload, error) {
	//todo: use ctx param to propagate cancel signal
	//todo: make configurable
	numTraceFiles := traceSource.TotalBlockCount()
	if config.SnapshotInterval > numTraceFiles {
		return nil, ErrInvSnapInterval
	}

	testFile, _, err := traceSource.GetBlock(0)
	if err != nil {
		return nil, fmt.Errorf("failed to read test file : %v", err)
	}

	tracesPerFile, err := traceParser.GetNumberOfTraces(testFile)
	if err != nil {
		return nil, fmt.Errorf("failed to determine traces per file : %v", err)
	}
	fmt.Printf("Traces per file: %v\n", tracesPerFile)

	scaleDenom := len(testFile) / Mega
	jobQueueLen := 1
	//== null can happen for small test files
	if scaleDenom != 0 {
		jobQueueLen = maxInt(1, (config.BufferSizeInGB*1024)/(len(testFile)/Mega))
	}
	log.Printf("jobQueueLen %v (buff\n", jobQueueLen)

	jobs := make(chan *FileLogBundle, jobQueueLen)
	errorChan := make(chan error)
	//if closed, all listening routines should terminate.
	shouldQuit := make(chan interface{})
	resultChan := make(chan WorkerPayload, config.ComputeWorkers)
	snapshotResultChan := make(chan snapshot, config.ComputeWorkers)
	var computeWorkerWg, feederWorkerWg, snapshotWg sync.WaitGroup
	//0+snapshotInterval is first snapshot point
	fileIDXThreshForSnapshot := config.SnapshotInterval - 1
	log.Printf("initial snapshot point is %v\n", fileIDXThreshForSnapshot)
	var lastSnapshot WorkerPayload = nil

	maxSnapshotIDX := int(math.Max(0, math.Floor(float64(numTraceFiles)/float64(config.SnapshotInterval))-1))

	//orchestrate snapshots
	snapshotWg.Add(1)
	go func() {
		defer snapshotWg.Done()

		snapshotDeltaBuf := make([]WorkerPayload, maxSnapshotIDX+1)
		receivedFilesForSnapshot := make([]int, maxSnapshotIDX+1)
		uncompletedSnapshots := maxSnapshotIDX + 1
		var rollingSnapshot WorkerPayload
		recombinedUpTo := -1
		waitingForResults := make([]int, 0)
		defer func() {
			if receivedFilesForSnapshot[maxSnapshotIDX] == config.SnapshotInterval {
				lastSnapshot = rollingSnapshot
			}
		}()
		for {
			select {
			case <-shouldQuit:
				log.Printf("snapshotter: received shutdown signal\n")
				return
			case v, ok := <-snapshotResultChan:
				if !ok {
					log.Printf("snapshotter: snapshotResultChan has been closed with %v uncompleted snapshots, exiting", uncompletedSnapshots)
					return
				}

				if v.processedFiles != 0 {
					log.Printf("snapshotter: snapshot %v received %v files from %v\n", v.snapshotIDX, v.processedFiles, v.workerID)
					//add data to snapshot buffer
					if snapshotDeltaBuf[v.snapshotIDX] == nil {
						snapshotDeltaBuf[v.snapshotIDX] = v.data
					} else {
						if err := snapshotDeltaBuf[v.snapshotIDX].Merge(v.data); err != nil {
							errorChan <- fmt.Errorf("snapshotter : tried to merge worker payloads of different types")
							return
						}
					}
					receivedFilesForSnapshot[v.snapshotIDX] += v.processedFiles

					//this should never happen
					if receivedFilesForSnapshot[v.snapshotIDX] > config.SnapshotInterval {
						errorChan <- fmt.Errorf("snapshotter: received %v files for snapshotIDX %v but only expected %v\n", receivedFilesForSnapshot[v.snapshotIDX], v.snapshotIDX, config.SnapshotInterval)
					}

					//snapshot complete, compute result and store it
					if receivedFilesForSnapshot[v.snapshotIDX] == config.SnapshotInterval {
						//add to wait list
						waitingForResults = append(waitingForResults, v.snapshotIDX)
						//todo: replace with priority queue, although this array will be small
						//sort in increasing order
						sort.Sort(sort.IntSlice(waitingForResults))
						log.Printf("snapshotter: queue before merge try :%v\n", waitingForResults)
						//check if new results allow us to remove entries
						i := 0 //need this after loop
						for ; i < len(waitingForResults) && recombinedUpTo == waitingForResults[i]-1; i++ {
							if waitingForResults[i] == 0 {
								rollingSnapshot = snapshotDeltaBuf[waitingForResults[i]]
							} else {
								if err := rollingSnapshot.Merge(snapshotDeltaBuf[waitingForResults[i]]); err != nil {
									errorChan <- fmt.Errorf("failed to merge snapshotIDX delta with previous results : %v", waitingForResults[i])
									return
								}
							}
							recombinedUpTo++
							_, err := rollingSnapshot.Finalize()
							if err != nil {
								if errors.Is(err, errOneSetEmpty) {
									log.Printf("cannot finalize snapshotIDX %v, as one of the sets is empty", waitingForResults[i])
									//don't need it anymore, drop reference to allow garbage collection
									snapshotDeltaBuf[waitingForResults[i]] = nil
								} else {
									errorChan <- fmt.Errorf("snapshotter: failed to compute result for snapshotIDX %v : %v", waitingForResults[i], err)
								}
							}
							log.Printf("snapshotter: snapshotIDX %v done\n", waitingForResults[i])
							if err := snapshotSaver(rollingSnapshot.DeepCopy()); err != nil {
								log.Printf("Failed to save snapshotIDX %v : %v\n", waitingForResults[i], err)
							}
							//don't need it anymore, drop reference to allow garbage collection
							snapshotDeltaBuf[waitingForResults[i]] = nil

							uncompletedSnapshots--
							if uncompletedSnapshots == 0 {
								log.Printf("snapshotter is done\n")
								return
							}
						}
						//remove processed elements
						waitingForResults = waitingForResults[i:]
						log.Printf("snapshotter: queue after merge try :%v\n", waitingForResults)
					}
				}
			}
		}
	}()

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
		go computeWorker(id, tracesPerFile, config.SnapshotInterval, maxSnapshotIDX, resultChan, traceParser, computeWorkerCtx, snapshotResultChan, workerPayloadCreator)
	}

	//start feeder, we only support one
	feederWorkerCtx.wg.Add(1)
	go feederWorker(0, 0, numTraceFiles, traceSource, feederWorkerCtx)

	//stores the first error send over errorChan
	var backgroundError error
	go func() {
		signaledShutdown := false
		for recError := range errorChan {
			log.Printf("Received error : %v", recError)
			backgroundError = recError
			if !signaledShutdown {
				close(shouldQuit)
				signaledShutdown = true
			}
		}
		if !signaledShutdown {
			close(shouldQuit)
		}
	}()

	feederWorkerCtx.wg.Wait()
	log.Printf("feeder finished\n")
	//all feeders must be done before we can close the jobs chan
	close(jobs)
	log.Printf("closed jobs channel, waiting wor compute workers to finish\n")
	//wait for compute workers to finish remaining jobs
	computeWorkerCtx.wg.Wait()
	log.Printf("compute workers finished\n")
	//all workers done, no one can send snapshot results anymore
	close(snapshotResultChan)
	//wait for snapshoter to drain  snapshotResultChan
	snapshotWg.Wait()
	//all workers done, no one can send errors anymore
	close(errorChan)
	//all workers are done, no one can send results anymore
	close(resultChan)

	if backgroundError != nil {
		return nil, backgroundError
	}

	log.Printf("Merging results\n")
	var combined WorkerPayload
	for partialResult := range resultChan {
		if combined == nil {
			combined = partialResult
		} else {
			if err := combined.Merge(partialResult); err != nil {
				return nil, fmt.Errorf("tried to merge worker payload of different types")
			}
		}
	}

	if lastSnapshot == nil && combined == nil {
		return nil, fmt.Errorf("failed to retrive resutls")

	}
	//this happens if snapshot interval is large than the amount of input blocks
	if lastSnapshot == nil {
		return combined, nil
	}
	//this happens if the latest snapshot is equal with the final result
	if combined == nil {
		return lastSnapshot, nil
	}

	if err := combined.Merge(lastSnapshot); err != nil {
		return nil, fmt.Errorf("failed to merge last snapshot with final result")
	}

	return combined, nil
}
