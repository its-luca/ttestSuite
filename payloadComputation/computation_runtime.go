package payloadComputation

//This files is centered around the Run function which provides runs multiple parallel WorkerPayloads while still
//allowing to take snapshots of the global state at configurable intervals

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

//Giga SI unit prefix
const Giga = 1024 * 1024 * 1024

//Mega SI unit prefix
const Mega = 1024 * 1024

//maxInt returns a if a>b else b
func maxInt(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

//syncVarsBundle bundles the channels/wait groups required for parallelization
type syncVarsBundle struct {
	//feeder writes news jobs, compute workers read; close once both of them are done
	jobs chan *job
	//send error information
	errorChan chan error
	//count of active workers
	wg *sync.WaitGroup
}

//snapshotDeltaShard bundles a deep copy of a workers state *since the previous snapshot point (delta)* together with metadata about the state.
//data may only contain information belonging to files with index in [snapshotIDX,snapshotIDX+snapshotInterval[.
//processedFiles indicates the amount of files bundled in data. This is used to recognize if all shards are collected
//Given all snapshotShards for a snapshotIDX we can compute the global delta of the WorkerPayload to the previous snapshot point
//Given all preceding snapshot points we can compute the global state
type snapshotDeltaShard struct {
	data WorkerPayload
	//snapshotIDX describes the file index range to which this shard belongs
	snapshotIDX int
	//processedFiles describes how many trace files are combined in data
	processedFiles int
	//workerID is the id of the worker routine that created this shard
	workerID int
}

//feederWorker, reads trace files [start,end[ and puts them to job chan
func feederWorker(ctx context.Context, feederID, start, end int, traceSource traceSource.TraceBlockReader, syncVars syncVarsBundle) {
	defer func() {
		log.Printf("Feeder %v done\n", feederID)
		syncVars.wg.Done()
	}()
	log.Printf("Feeder %v started on range [%v,%v[\n", feederID, start, end)
	traceBlockIDX := start
	metricsTicker := time.NewTicker(5 * time.Second)
	processedElements := 0
	readTimeSinceLastTick := 0 * time.Second
	enqueueWaitSinceLastTick := 0 * time.Second
	for {
		select {
		case <-ctx.Done():
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
					syncVars.errorChan <- fmt.Errorf("failed get trace block/file with id %v : %v", traceBlockIDX, err)
					return
				}
				flb := &job{
					fileIDX:    traceBlockIDX,
					file:       rawWFM,
					subCaseLog: caseLog,
				}
				readTimeSinceLastTick += time.Since(startTime)
				startTime = time.Now()
				syncVars.jobs <- flb
				enqueueWaitSinceLastTick += time.Since(startTime)
				processedElements++
				traceBlockIDX++
			}
		}
	}
}

//computeWorker, consumes wCtx.jobs and adds them the WorkerPayload created by workerPayloadCreator.
//Every snapshotInterval files a snapshotDeltaShard is published on snapshotResults.
//In the end the delta to the latest snapshot (if there is any) is published on resultChan.
func computeWorker(ctx context.Context, workerID, tracesPerFile, snapshotInterval, maxSnapshotIDX int, resultChan chan<- WorkerPayload,
	traceParser wfm.TraceParser, syncVars syncVarsBundle, snapshotResults chan<- snapshotDeltaShard, workerPayloadCreator WorkerPayloadCreator) {
	defer func() {
		log.Printf("Worker %v done\n", workerID)
		syncVars.wg.Done()
	}()
	log.Printf("Worker %v started\n", workerID)
	var frames [][]float64
	var payload WorkerPayload
	fixedTraces := make([][]float64, 0, tracesPerFile/2)
	randomTraces := make([][]float64, 0, tracesPerFile/2)
	//set to the last snapshot threshold that we have passed
	//used to make sure we do not send data for the same threshold twice
	curSnapshotIDX := 0
	filesSinceLastSnapshot := 0
	var err error
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %v received error message and is shuttding down\n", workerID)
			return
		case workPackage := <-syncVars.jobs:
			{
				//channel has been closed, no more jobs are coming; send results and finish
				if workPackage == nil {
					log.Printf("compute worker %v waiting to send result", workerID)

					//send either as snapshotDeltaShard or as final result
					if curSnapshotIDX <= maxSnapshotIDX {
						var buf WorkerPayload
						if payload != nil {
							buf = payload.DeepCopy()
						}
						snapshotResults <- snapshotDeltaShard{
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
				//if true we won't get any more data contributing to this snapshotDeltaShard and thus can send data and move
				//on to the next interval
				if curSnapshotIDX <= maxSnapshotIDX && workPackage.fileIDX >= (curSnapshotIDX*snapshotInterval+snapshotInterval) {
					log.Printf("worker %v: sending data for snapshotIDX %v : %v files\n", workerID, curSnapshotIDX, filesSinceLastSnapshot)
					var buf WorkerPayload
					if payload != nil {
						buf = payload.DeepCopy()
					}
					snapshotResults <- snapshotDeltaShard{
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
					syncVars.errorChan <- fmt.Errorf("worker %v : failed to parse wfm file : %v", workerID, err)
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

func snapshoter(ctx context.Context, snapshotWg *sync.WaitGroup, maxSnapshotIDX int,
	deltaShards <-chan snapshotDeltaShard, errorChan chan<- error, finalSnapshot chan<- WorkerPayload, config ComputationConfig) (lastSnapshot WorkerPayload) {
	defer snapshotWg.Done()

	snapshotDeltaBuf := make([]WorkerPayload, maxSnapshotIDX+1)
	receivedFilesForSnapshotDelta := make([]int, maxSnapshotIDX+1)
	uncompletedSnapshots := maxSnapshotIDX + 1
	//accumulates all completed snapshots, given a new delta we can add it here
	//to compute the next snapshot
	var rollingSnapshot WorkerPayload

	//due to the job buffer and the multiple workers maintaining their own state,
	//snapshot deltas may not arrive in order (although the job buffer size limits this)
	//As we need the preceding deltas to compute the snapshot we add them to this wait list
	waitingForResults := make([]int, 0)
	recombinedUpTo := -1

	//if we reach the final snapshot everything announce it on separate channel
	defer func() {
		if receivedFilesForSnapshotDelta[maxSnapshotIDX] == config.SnapshotInterval {
			finalSnapshot <- rollingSnapshot
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Printf("snapshotter: received shutdown signal\n")
			return
		case v, ok := <-deltaShards:
			if !ok {
				log.Printf("snapshotter: snapshotResultChan has been closed with %v uncompleted snapshots, exiting", uncompletedSnapshots)
				return
			}

			//workers may generate empty snapshotDeltaShards which we can ignore
			if v.processedFiles == 0 {
				continue
			}
			log.Printf("snapshotter: snapshotDeltaShard %v received %v files from %v\n", v.snapshotIDX, v.processedFiles, v.workerID)

			//add data to snapshotDeltaShard buffer
			if snapshotDeltaBuf[v.snapshotIDX] == nil {
				snapshotDeltaBuf[v.snapshotIDX] = v.data
			} else {
				if err := snapshotDeltaBuf[v.snapshotIDX].Merge(v.data); err != nil {
					errorChan <- fmt.Errorf("snapshotter : tried to merge worker payloads of different types")
					return
				}
			}
			receivedFilesForSnapshotDelta[v.snapshotIDX] += v.processedFiles

			//this should never happen
			if receivedFilesForSnapshotDelta[v.snapshotIDX] > config.SnapshotInterval {
				errorChan <- fmt.Errorf("snapshotter: received %v files for snapshotIDX %v but only expected %v\n", receivedFilesForSnapshotDelta[v.snapshotIDX], v.snapshotIDX, config.SnapshotInterval)
			}

			//if we do not have all shards for this delta we can abort here
			if receivedFilesForSnapshotDelta[v.snapshotIDX] != config.SnapshotInterval {
				continue
			}

			//if we reach this then the snapshotDeltaShard v.snapshotIDX is complete

			//add to wait list; always using wait list makes code easier than adding special case for all preceding snapshots being done
			waitingForResults = append(waitingForResults, v.snapshotIDX)
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
				snapshotResult, err := rollingSnapshot.Finalize()
				if err != nil {
					if errors.Is(err, ErrOneSetEmpty) {
						log.Printf("cannot finalize snapshotIDX %v, as one of the sets is empty", waitingForResults[i])
						//don't need it anymore, drop reference to allow garbage collection
						snapshotDeltaBuf[waitingForResults[i]] = nil
					} else {
						errorChan <- fmt.Errorf("snapshotter: failed to compute result for snapshotIDX %v : %v", waitingForResults[i], err)
					}
				}
				log.Printf("snapshotter: snapshotIDX %v done\n", waitingForResults[i])
				if err := config.SnapshotSaver(snapshotResult, rollingSnapshot.DeepCopy(), waitingForResults[i]); err != nil {
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

//job, bundles a trace file together with it's case indicator (fixed or random)
//Intended to be created by the feederWorker and consumed by the computeWorker
type job struct {
	fileIDX    int
	file       []byte
	subCaseLog []int
}

var ErrInvSnapInterval = errors.New("snapshot interval larger than trace file count")

//ComputationConfig configures resource usage and performed payload computation
type ComputationConfig struct {
	//number of compute workers to spawn; increase if not cpu gated
	ComputeWorkers int
	//controls buffer (unit trace files) available to FeederWorkers; increase to fill RAM for max performance
	BufferSizeInGB int
	//Amount of files after which a snapshotDeltaShard is created
	SnapshotInterval int
	//constructor for the WorkerPayload that should be computed
	WorkerPayloadCreator WorkerPayloadCreator
	//gets called once the next snapshot is created. Increasing order of snapshots is guaranteed.
	SnapshotSaver func(result []float64, rawSnapshot WorkerPayload, snapshotIDX int) error
}

//Run performs the parallel computation of the payload denoted by config.WorkerPayloadCreator on the data defined by traceSource and traceParser
//According to config.SnapshotInterval, config.SnapshotSaver is called with periodic snapshots/intermediate results.
func Run(ctx context.Context, traceSource traceSource.TraceBlockReader, traceParser wfm.TraceParser, config ComputationConfig) (WorkerPayload, error) {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	numTraceFiles := traceSource.TotalBlockCount()
	if config.SnapshotInterval > numTraceFiles {
		return nil, ErrInvSnapInterval
	}

	//access first block to parse number of traces per file (which is assumed to be the same for all files)
	testFile, _, err := traceSource.GetBlock(0)
	if err != nil {
		return nil, fmt.Errorf("failed to read test file : %v", err)
	}

	tracesPerFile, err := traceParser.GetNumberOfTraces(testFile)
	if err != nil {
		return nil, fmt.Errorf("failed to determine traces per file : %v", err)
	}
	fmt.Printf("Traces per file: %v\n", tracesPerFile)

	//Derive the amount of RAM to be used for the job buffer
	traceFileSizeInMB := len(testFile) / Mega
	jobQueueLen := 1
	//== null can happen for small test files
	if traceFileSizeInMB != 0 {
		jobQueueLen = maxInt(1, (config.BufferSizeInGB*1024)/traceFileSizeInMB)
	}
	log.Printf("jobQueueLen %v (buff\n", jobQueueLen)

	jobs := make(chan *job, jobQueueLen)
	errorChan := make(chan error)
	resultChan := make(chan WorkerPayload, config.ComputeWorkers)
	snapshotDeltaShardChan := make(chan snapshotDeltaShard, config.ComputeWorkers)
	var computeWorkerWg, feederWorkerWg, snapshotWg sync.WaitGroup
	//0+snapshotInterval is first snapshotDeltaShard point
	fileIDXThreshForSnapshot := config.SnapshotInterval - 1
	log.Printf("initial snapshotDeltaShard point is %v\n", fileIDXThreshForSnapshot)
	finalSnapshot := make(chan WorkerPayload, 1)
	maxSnapshotIDX := int(math.Max(0, math.Floor(float64(numTraceFiles)/float64(config.SnapshotInterval))-1))

	//orchestrate snapshots, final snapshot (it it is reached) is published on finalSnapshot channel
	snapshotWg.Add(1)
	go snapshoter(ctx, &snapshotWg, maxSnapshotIDX, snapshotDeltaShardChan, errorChan, finalSnapshot, config)

	//stats ticker
	statsTicker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTicker.C:
				{
					log.Printf("Buffer Usage %v out of %v\n", len(jobs), cap(jobs))
				}
			}
		}
	}()

	computeWorkerCtx := syncVarsBundle{
		jobs:      jobs,
		errorChan: errorChan,
		wg:        &computeWorkerWg,
	}

	feederWorkerCtx := syncVarsBundle{
		jobs:      jobs,
		errorChan: errorChan,
		wg:        &feederWorkerWg,
	}

	//create ComputeWorkers
	for id := 0; id < config.ComputeWorkers; id++ {
		computeWorkerCtx.wg.Add(1)
		go computeWorker(ctx, id, tracesPerFile, config.SnapshotInterval, maxSnapshotIDX, resultChan, traceParser, computeWorkerCtx, snapshotDeltaShardChan, config.WorkerPayloadCreator)
	}

	//start feeder, we only support one
	feederWorkerCtx.wg.Add(1)
	go feederWorker(ctx, 0, 0, numTraceFiles, traceSource, feederWorkerCtx)

	//stores the first error send over errorChan
	var backgroundError error
	go func() {
		signaledShutdown := false
		for recError := range errorChan {
			log.Printf("Received error : %v", recError)
			backgroundError = recError
			if !signaledShutdown {
				cancelCtx()
				signaledShutdown = true
			}
		}
		if !signaledShutdown {
			cancelCtx()
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
	//all workers done, no one can send snapshotDeltaShard results anymore
	close(snapshotDeltaShardChan)
	//wait for snapshoter to drain  snapshotDeltaShardChan
	snapshotWg.Wait()
	close(finalSnapshot)
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

	lastSnapshot := <-finalSnapshot
	if lastSnapshot == nil && combined == nil {
		return nil, fmt.Errorf("failed to retrive results")

	}
	//this happens if snapshotDeltaShard interval is large than the amount of input blocks
	if lastSnapshot == nil {
		return combined, nil
	}
	//this happens if the latest snapshotDeltaShard is equal with the final result
	if combined == nil {
		return lastSnapshot, nil
	}

	if err := combined.Merge(lastSnapshot); err != nil {
		return nil, fmt.Errorf("failed to merge last snapshotDeltaShard with final result")
	}

	return combined, nil
}
