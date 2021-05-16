package payloadComputation

//This files is centered around the Run function which provides runs multiple parallel WorkerPayloads while still
//allowing to take snapshots of the global state at configurable intervals

import (
	"context"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
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

func minFloat64Slice(a []float64) float64 {
	if len(a) == 1 {
		return a[0]
	}
	min := a[0]
	for i := 1; i < len(a); i++ {
		if min > a[i] {
			min = a[i]
		}
	}
	return min
}

type SnapshotSaverFunc func(result []float64, rawSnapshot WorkerPayload, snapshotIDX int) error

//ComputationRuntime configures resource usage and performed payload computation
type ComputationRuntime struct {
	//number of compute workers to spawn; increase if not cpu gated
	ComputeWorkers int
	//controls buffer (unit trace files) available to FeederWorkers; increase to fill RAM for max performance
	BufferSizeInGB int
	//Amount of files after which a snapshotDeltaShard is created
	SnapshotInterval int
	//constructor for the WorkerPayload that should be computed
	WorkerPayloadCreator WorkerPayloadCreator
	//gets called once the next snapshot is created. Increasing order of snapshots is guaranteed.
	SnapshotSaver SnapshotSaverFunc
	//For detailed status information useful for debugging but not for normal operation
	DebugLog *log.Logger
	//For status information that are useful during normal operations but could be omitted
	InfoLog *log.Logger
	//For critical warnings and errors that may not be omitted
	ErrLog            *log.Logger
	workerPayloadPool *WorkerPayloadPool
	//prometheus metrics
	MetricsRegistry                         *prometheus.Registry
	MetrMaxTestValue                        prometheus.Gauge
	prefixSizeInPercent                     float64
	MetrXCorrAgainstFixedPrefix             prometheus.Gauge
	MetrXCorrAgainstRandomPrefix            prometheus.Gauge
	MetrInputFileCount                      prometheus.Gauge
	MetrReadFilesCount                      prometheus.Counter
	MetrProcessedFilesCount                 prometheus.Counter
	MetrInputBufferFreeSlots                prometheus.Gauge
	MetrQQBufferFreeSlots                   prometheus.Gauge
	MetrSnapshotterWaitQueueSize            prometheus.Gauge
	MetrSnapshotterDeltaShardQueueFreeSlots prometheus.Gauge
}

func NewComputationRuntime(computeWorkers, bufferSizeInGB, snapshotInterval int, wpc WorkerPayloadCreator, ss SnapshotSaverFunc,
	debugLog, infoLog, errLog *log.Logger) (*ComputationRuntime, error) {
	cfg := &ComputationRuntime{
		ComputeWorkers:       computeWorkers,
		BufferSizeInGB:       bufferSizeInGB,
		SnapshotInterval:     snapshotInterval,
		WorkerPayloadCreator: wpc,
		SnapshotSaver:        ss,
		DebugLog:             debugLog,
		InfoLog:              infoLog,
		ErrLog:               errLog,
		workerPayloadPool:    nil,
		MetricsRegistry:      prometheus.NewRegistry(),
		MetrMaxTestValue: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ttestsuite_compr_max_test_value",
			Help: "Highest value of WorkerPayload result in the latest snapshot",
		}),
		prefixSizeInPercent: 5,
		MetrXCorrAgainstFixedPrefix: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ttestsuite_compr_xcorr_against_fixed_prefix",
		}),
		MetrXCorrAgainstRandomPrefix: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ttestsuite_compr_xcorr_against_random_prefix",
		}),
		MetrInputFileCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ttestsuite_compr_input_file_count",
			Help: "Amount of input files that are supposed to be processed",
		}),
		MetrReadFilesCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ttestsuite_compr_read_files_count",
			Help: "Amount of files read by the feeder",
		}),
		MetrProcessedFilesCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ttestsuite_compr_processed_files_count",
			Help: "Amount of files fully processed",
		}),
		MetrInputBufferFreeSlots: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ttestsuite_compr_input_queue_free_slots",
		}),
		MetrQQBufferFreeSlots: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ttestsuite_compr_qq_queue_free_slots",
		}),
		MetrSnapshotterWaitQueueSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ttestsuite_compr_ss_wait_queue_size",
		}),
		MetrSnapshotterDeltaShardQueueFreeSlots: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ttestsuite_compr_ss_input_queue_free_slots",
		}),
	}

	cfg.MetricsRegistry.MustRegister(cfg.MetrMaxTestValue)
	cfg.MetricsRegistry.MustRegister(cfg.MetrXCorrAgainstFixedPrefix)
	cfg.MetricsRegistry.MustRegister(cfg.MetrXCorrAgainstRandomPrefix)
	cfg.MetricsRegistry.MustRegister(cfg.MetrInputFileCount)
	cfg.MetricsRegistry.MustRegister(cfg.MetrReadFilesCount)
	cfg.MetricsRegistry.MustRegister(cfg.MetrProcessedFilesCount)
	cfg.MetricsRegistry.MustRegister(cfg.MetrInputBufferFreeSlots)
	cfg.MetricsRegistry.MustRegister(cfg.MetrQQBufferFreeSlots)
	cfg.MetricsRegistry.MustRegister(cfg.MetrSnapshotterWaitQueueSize)
	cfg.MetricsRegistry.MustRegister(cfg.MetrSnapshotterDeltaShardQueueFreeSlots)

	return cfg, nil
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
func (config *ComputationRuntime) feederWorker(ctx context.Context, start, end int, traceSource traceSource.TraceBlockReader, syncVars syncVarsBundle,
	qualityControlJobs chan<- *job) {
	defer func() {
		config.DebugLog.Printf("Feeder done\n")
		syncVars.wg.Done()
	}()
	config.DebugLog.Printf("Feeder started on range [%v,%v[\n", start, end)
	traceBlockIDX := start
	metricsTicker := time.NewTicker(5 * time.Second)
	processedElements := 0
	readTimeSinceLastTick := 0 * time.Second
	enqueueWaitSinceLastTick := 0 * time.Second
	for {
		select {
		case <-ctx.Done():
			config.ErrLog.Printf("Early feeder quit due to abort signal")
			return
		case <-metricsTicker.C:
			if processedElements != 0 {
				config.InfoLog.Printf("Feeder:\t avg read time %v\t avg enq wait %v\t progress %v/%v\n",
					readTimeSinceLastTick/time.Duration(processedElements), enqueueWaitSinceLastTick/time.Duration(processedElements),
					traceBlockIDX-start, end-start)
			}
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
				qualityControlJobs <- flb
				enqueueWaitSinceLastTick += time.Since(startTime)
				processedElements++
				config.MetrReadFilesCount.Inc()
				config.MetrInputBufferFreeSlots.Set(float64(cap(syncVars.jobs) - len(syncVars.jobs)))
				config.MetrQQBufferFreeSlots.Set(float64(cap(qualityControlJobs) - len(qualityControlJobs)))
				traceBlockIDX++
			}
		}
	}
}

func (config *ComputationRuntime) qualityControl(ctx context.Context, totalFileCount, traceLength int, parser wfm.TraceParser,
	errorChan chan error, jobs <-chan *job, wg *sync.WaitGroup) {
	defer wg.Done()

	const sampleStepSize = 10 //TODO: make configureable
	filesInPrefix := int(math.Round(float64(totalFileCount) * (config.prefixSizeInPercent / 100)))
	lenFixed := float64(0)
	lenRandom := float64(0)
	prefixSumFixed := make([]float64, traceLength)
	prefixSumRandom := make([]float64, traceLength)
	bufferFixed := make([][]float64, 0)
	bufferRandom := make([][]float64, 0)
	var prefixMeanRandom []float64
	var prefixMeanFixed []float64
	var parsedTracesBuf [][]float64
	config.MetrXCorrAgainstRandomPrefix.Set(-1)
	config.MetrXCorrAgainstFixedPrefix.Set(-1)
	for {
		select {
		case <-ctx.Done():
			config.ErrLog.Printf("Early qualityControl exit due to arbort signal")
			return
		case j, ok := <-jobs:
			if !ok { //channel closed
				return
			}
			//if we are not building the prefix anymore, check if we wan to process file
			//for small inputs filesInPrefix may be 0 in which case we will always skip
			if filesInPrefix == 0 || (j.fileIDX > filesInPrefix && (j.fileIDX%sampleStepSize != 0)) {
				continue
			}

			var err error
			parsedTracesBuf, err = parser.ParseTraces(j.file, parsedTracesBuf)
			if err != nil {
				errorChan <- fmt.Errorf("qualityControl failed to parse files %v : %v", j.fileIDX, err)
				return
			}

			//cluster to case marker
			for i := range parsedTracesBuf {
				caseMarker := j.subCaseLog[i]
				if caseMarker == 0 {
					bufferFixed = append(bufferFixed, parsedTracesBuf[i])
				} else if caseMarker == 1 {
					bufferRandom = append(bufferRandom, parsedTracesBuf[i])
				} else {
					errorChan <- fmt.Errorf("qualityControl hit unexpected case marker : %v", caseMarker)
					return
				}
			}

			if j.fileIDX < filesInPrefix {
				addPwSumFloat64(prefixSumFixed, bufferFixed)
				lenFixed += float64(len(bufferFixed))
				addPwSumFloat64(prefixSumRandom, bufferRandom)
				lenRandom += float64(len(bufferRandom))
				//if this file completes the prefix, create means
				if j.fileIDX == (filesInPrefix - 1) {
					for i := 0; i < traceLength; i++ {
						prefixSumFixed[i] /= lenFixed
						prefixSumRandom[i] /= lenRandom
					}
					prefixMeanFixed = prefixSumFixed
					prefixMeanRandom = prefixSumRandom
					config.InfoLog.Printf("Finished building prefix for xcorr tests")
				}
			} else {
				config.InfoLog.Printf("Updating xCorr values")
				minXCorrFixed := float64(1) //values is in [0,1] , so this is max
				minXCorrRandom := float64(1)
				xcorrFixed, err := ComputeCorrelation(ctx, 4, prefixMeanFixed, bufferFixed)
				if err != nil {
					config.ErrLog.Printf("qualityControl failed to calc xcorr against fixed : %v", err)
					config.MetrXCorrAgainstFixedPrefix.Set(-1)

				} else {
					minXCorrFixed = math.Min(minXCorrFixed, minFloat64Slice(xcorrFixed))
					config.MetrXCorrAgainstFixedPrefix.Set(minXCorrFixed)
				}

				xcorrRandom, err := ComputeCorrelation(ctx, 4, prefixMeanRandom, bufferRandom)
				if err != nil {
					config.ErrLog.Printf("qualityControl failed to calc xcorr against random : %v", err)
					config.MetrXCorrAgainstRandomPrefix.Set(-1)
				} else {
					minXCorrRandom = math.Min(minXCorrRandom, minFloat64Slice(xcorrRandom))
					config.MetrXCorrAgainstRandomPrefix.Set(minXCorrRandom)
				}
				config.InfoLog.Printf("Done updating xCorr values")
			}
			bufferRandom = bufferRandom[:0]
			bufferFixed = bufferFixed[:0]

		}
	}
}

//computeWorker, consumes wCtx.jobs and adds them the WorkerPayload created by workerPayloadCreator.
//Every snapshotInterval files a snapshotDeltaShard is published on snapshotResults.
//In the end the delta to the latest snapshot (if there is any) is published on resultChan.
func (config *ComputationRuntime) computeWorker(ctx context.Context, workerID, tracesPerFile, snapshotInterval, maxSnapshotIDX int, resultChan chan<- WorkerPayload,
	traceParser wfm.TraceParser, syncVars syncVarsBundle, snapshotResults chan<- snapshotDeltaShard) {
	defer func() {
		config.DebugLog.Printf("Worker %v done\n", workerID)
		syncVars.wg.Done()
	}()
	config.DebugLog.Printf("Worker %v started\n", workerID)
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
			config.ErrLog.Printf("Worker %v quits due to shutdown signal\n", workerID)
			return
		case workPackage := <-syncVars.jobs:
			{
				//channel has been closed, no more jobs are coming; send results and finish
				if workPackage == nil {
					config.DebugLog.Printf("compute worker %v waiting to send result", workerID)

					//send either as snapshotDeltaShard or as final result
					if curSnapshotIDX <= maxSnapshotIDX {
						snapshotResults <- snapshotDeltaShard{
							data:           payload,
							snapshotIDX:    curSnapshotIDX,
							processedFiles: filesSinceLastSnapshot,
							workerID:       workerID,
						}
					} else {
						resultChan <- payload
					}
					return
				}

				config.DebugLog.Printf("worker %v: processing fileIDX %v\n", workerID, workPackage.fileIDX)
				//if true we won't get any more data contributing to this snapshotDeltaShard and thus can send data and move
				//on to the next interval
				if curSnapshotIDX <= maxSnapshotIDX && workPackage.fileIDX >= (curSnapshotIDX*snapshotInterval+snapshotInterval) {
					config.DebugLog.Printf("worker %v: sending data for snapshotIDX %v : %v files\n", workerID, curSnapshotIDX, filesSinceLastSnapshot)
					snapshotResults <- snapshotDeltaShard{
						data:           payload,
						snapshotIDX:    curSnapshotIDX,
						processedFiles: filesSinceLastSnapshot,
						workerID:       workerID,
					}
					//skip forward to interval to which workPackage.fileIDX belongs
					for workPackage.fileIDX >= (curSnapshotIDX+1)*snapshotInterval {
						curSnapshotIDX++
					}
					config.DebugLog.Printf("worker %v: next snapshotIDX is %v\n", workerID, curSnapshotIDX)

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
					//payload = config.WorkerPayloadCreator(len(frames[0]))
					payload = config.workerPayloadPool.Get()
				}

				payload.Update(fixedTraces, randomTraces)
				for i := range randomTraces {
					randomTraces[i] = nil
				}
				for i := range fixedTraces {
					fixedTraces[i] = nil
				}
				filesSinceLastSnapshot++
				config.MetrProcessedFilesCount.Inc()
				config.DebugLog.Printf("worker %v: done processing %v, filesSinceLastSnapshot %v\n", workerID, workPackage.fileIDX, filesSinceLastSnapshot)

			}
		}
	}
}

func (config *ComputationRuntime) snapshoter(ctx context.Context, snapshotWg *sync.WaitGroup, maxSnapshotIDX int,
	deltaShards <-chan snapshotDeltaShard, errorChan chan<- error, finalSnapshot chan<- WorkerPayload) {
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
			config.InfoLog.Printf("snapshotter: sending final snapshot...")
			finalSnapshot <- rollingSnapshot
		}
	}()

	statsTicker := time.NewTicker(10 * time.Second)
	lastTick := time.Now()
	defer statsTicker.Stop()
	//accounting for remaining time prediction
	receivedFilesSinceLastTick := 0
	remainingFiles := (maxSnapshotIDX + 1) * config.SnapshotInterval

	for {
		select {
		case <-ctx.Done():
			config.ErrLog.Printf("snapshoter quitting due to shutdown signal\n")
			return
		case tick := <-statsTicker.C:
			elapsedSeconds := tick.Sub(lastTick).Seconds()
			if elapsedSeconds == 0 {
				continue
			}
			filesPerSec := float64(receivedFilesSinceLastTick) / elapsedSeconds
			remainingFiles -= receivedFilesSinceLastTick
			if filesPerSec > 0 {
				config.InfoLog.Printf("processed %v files since %v, projecting %v remaining\n",
					receivedFilesSinceLastTick, lastTick.Format("15:04:05"), time.Duration(float64(remainingFiles)/filesPerSec)*time.Second)
			}
			receivedFilesSinceLastTick = 0
			lastTick = tick
		case deltaShard, ok := <-deltaShards: //INVARIANT: at the end of this case deltaShard.data must alwas be put
			// back into config.workerPayloadPool. If we want a copy make an explicit one
			if !ok {
				config.ErrLog.Printf("snapshoter: snapshotResultChan has been closed with %v uncompleted snapshots, exiting", uncompletedSnapshots)
				return
			}

			//workers may generate empty snapshotDeltaShards which we can ignore
			if deltaShard.processedFiles == 0 {
				//mark workerPayload in deltaShard as free
				if deltaShard.data != nil {
					config.workerPayloadPool.Put(deltaShard.data)
				}
				continue
			}
			config.DebugLog.Printf("snapshoter: snapshotDeltaShard %v received %v files from %v\n", deltaShard.snapshotIDX, deltaShard.processedFiles, deltaShard.workerID)

			//if buffer is nil, request a new one. We don't use deltaShard.data directly to simplify the
			//logic required for putting it back into the config.workerPayloadPool
			if snapshotDeltaBuf[deltaShard.snapshotIDX] == nil {
				snapshotDeltaBuf[deltaShard.snapshotIDX] = config.workerPayloadPool.Get()
			}
			//merge deltaShard into buffer
			if err := snapshotDeltaBuf[deltaShard.snapshotIDX].Merge(deltaShard.data); err != nil {
				errorChan <- fmt.Errorf("snapshotter : tried to merge worker payloads of different types")
				return
			}
			receivedFilesForSnapshotDelta[deltaShard.snapshotIDX] += deltaShard.processedFiles
			receivedFilesSinceLastTick += deltaShard.processedFiles

			//this should never happen
			if receivedFilesForSnapshotDelta[deltaShard.snapshotIDX] > config.SnapshotInterval {
				errorChan <- fmt.Errorf("snapshotter: received %v files for snapshotIDX %v but only expected %v\n", receivedFilesForSnapshotDelta[deltaShard.snapshotIDX], deltaShard.snapshotIDX, config.SnapshotInterval)
			}

			//if we do not have all shards for this delta we can abort here
			if receivedFilesForSnapshotDelta[deltaShard.snapshotIDX] != config.SnapshotInterval {
				//mark workerPayload in deltaShard as free, note that
				config.workerPayloadPool.Put(deltaShard.data)
				continue
			}

			//if we reach this then the snapshotDeltaShard deltaShard.snapshotIDX is complete

			//add to wait list; always using wait list makes code easier than adding special case for all preceding snapshots being done
			waitingForResults = append(waitingForResults, deltaShard.snapshotIDX)
			//sort in increasing order
			sort.Sort(sort.IntSlice(waitingForResults))
			config.DebugLog.Printf("snapshoter: queue before merge try :%v\n", waitingForResults)

			//check if new results allow us to remove entries
			i := 0 //need this after loop
			for ; i < len(waitingForResults) && recombinedUpTo == waitingForResults[i]-1; i++ {
				//if this is the first snapshot we have to alloc rollingSnapshot
				if waitingForResults[i] == 0 {
					rollingSnapshot = config.workerPayloadPool.Get()
				}
				if err := rollingSnapshot.Merge(snapshotDeltaBuf[waitingForResults[i]]); err != nil {
					errorChan <- fmt.Errorf("failed to merge snapshotIDX delta with previous results : %v", waitingForResults[i])
					return
				}
				recombinedUpTo++
				snapshotResult, err := rollingSnapshot.Finalize()
				if err != nil {
					if errors.Is(err, ErrOneSetEmpty) {
						config.ErrLog.Printf("cannot produce snapshotIDX %v, as one of the sets is empty", waitingForResults[i])
						//don't need it anymore,put back into pool
						config.workerPayloadPool.Put(snapshotDeltaBuf[waitingForResults[i]])
						snapshotDeltaBuf[waitingForResults[i]] = nil //drop reference for potential gargabe collection
					} else {
						errorChan <- fmt.Errorf("snapshotter: failed to compute result for snapshotIDX %v : %v", waitingForResults[i], err)
					}
				}
				config.InfoLog.Printf("snapshoter: snapshotIDX %v done\n", waitingForResults[i])
				if err := config.SnapshotSaver(snapshotResult, rollingSnapshot.DeepCopy(), waitingForResults[i]); err != nil {
					config.ErrLog.Printf("Failed to save snapshotIDX %v : %v\n", waitingForResults[i], err)
				}

				//Start Metrics Update
				maxValue := float64(0)
				for i := range snapshotResult {
					if maxValue < snapshotResult[i] {
						maxValue = snapshotResult[i]
					}
				}
				config.MetrMaxTestValue.Set(maxValue)
				//End of Metrics Update

				//don't need it anymore, drop reference to allow garbage collection
				config.workerPayloadPool.Put(snapshotDeltaBuf[waitingForResults[i]])
				snapshotDeltaBuf[waitingForResults[i]] = nil

				uncompletedSnapshots--
				if uncompletedSnapshots == 0 {
					config.InfoLog.Printf("snapshoter is done\n")
					return
				}
			}
			//remove processed elements
			waitingForResults = waitingForResults[i:]
			config.DebugLog.Printf("snapshoter: queue after merge try :%v\n", waitingForResults)
			config.MetrSnapshotterWaitQueueSize.Set(float64(len(waitingForResults)))
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

//Run performs the parallel computation of the payload denoted by config.WorkerPayloadCreator on the data defined by traceSource and traceParser
//According to config.SnapshotInterval, config.SnapshotSaver is called with periodic snapshots/intermediate results.
func (config *ComputationRuntime) Run(ctx context.Context, traceSource traceSource.TraceBlockReader, traceParser wfm.TraceParser) (WorkerPayload, error) {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	numTraceFiles := traceSource.TotalBlockCount()
	if config.SnapshotInterval > numTraceFiles {
		return nil, ErrInvSnapInterval
	}
	config.MetrInputFileCount.Set(float64(numTraceFiles))
	//access first block to parse number of traces per file (which is assumed to be the same for all files)
	testFile, _, err := traceSource.GetBlock(0)
	if err != nil {
		return nil, fmt.Errorf("failed to read test file : %v", err)
	}

	parsedTestFile, err := traceParser.ParseTraces(testFile, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to determine traces per file : %v", err)
	}
	tracesPerFile := len(parsedTestFile)
	datapointsPerTrace := len(parsedTestFile[0])
	config.InfoLog.Printf("Traces per file: %v\n", tracesPerFile)

	config.workerPayloadPool = NewWorkerPayloadPool(config.WorkerPayloadCreator, datapointsPerTrace)

	//Derive the amount of RAM to be used for the job buffer
	traceFileSizeInMB := len(testFile) / Mega
	jobQueueLen := 1
	//== null can happen for small test files
	if traceFileSizeInMB != 0 {
		jobQueueLen = maxInt(1, (config.BufferSizeInGB*1024)/traceFileSizeInMB)
	}
	config.InfoLog.Printf("jobQueueLen %v\n", jobQueueLen)

	jobs := make(chan *job, jobQueueLen)
	qualityControlJobs := make(chan *job, jobQueueLen)
	errorChan := make(chan error)
	resultChan := make(chan WorkerPayload, 2*config.ComputeWorkers)
	snapshotDeltaShardChan := make(chan snapshotDeltaShard, 2*config.ComputeWorkers)
	var computeWorkerWg, feederWorkerWg, snapshotWg sync.WaitGroup
	//0+snapshotInterval is first snapshotDeltaShard point
	fileIDXThreshForSnapshot := config.SnapshotInterval - 1
	config.DebugLog.Printf("initial snapshotDeltaShard point is %v\n", fileIDXThreshForSnapshot)
	finalSnapshot := make(chan WorkerPayload, 1)
	maxSnapshotIDX := int(math.Max(0, math.Floor(float64(numTraceFiles)/float64(config.SnapshotInterval))-1))

	//orchestrate snapshots, final snapshot (it it is reached) is published on finalSnapshot channel
	snapshotWg.Add(1)
	go config.snapshoter(ctx, &snapshotWg, maxSnapshotIDX, snapshotDeltaShardChan, errorChan, finalSnapshot)

	//stats ticker
	statsTicker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTicker.C:
				{
					config.InfoLog.Printf("Buffer Usage %v out of %v\n", len(jobs), cap(jobs))
					config.MetrSnapshotterDeltaShardQueueFreeSlots.Set(float64(cap(snapshotDeltaShardChan) - len(snapshotDeltaShardChan)))
				}
			}
		}
	}()

	computeWorkerSyncVars := syncVarsBundle{
		jobs:      jobs,
		errorChan: errorChan,
		wg:        &computeWorkerWg,
	}

	feederWorkerSyncVars := syncVarsBundle{
		jobs:      jobs,
		errorChan: errorChan,
		wg:        &feederWorkerWg,
	}

	//create quality control worker
	computeWorkerWg.Add(1)
	go config.qualityControl(ctx, numTraceFiles, datapointsPerTrace, wfm.Parser{}, errorChan, qualityControlJobs,
		&computeWorkerWg)

	//create ComputeWorkers
	for id := 0; id < config.ComputeWorkers; id++ {
		computeWorkerSyncVars.wg.Add(1)
		go config.computeWorker(ctx, id, tracesPerFile, config.SnapshotInterval, maxSnapshotIDX, resultChan, traceParser, computeWorkerSyncVars, snapshotDeltaShardChan)
	}

	//start feeder, we only support one
	feederWorkerSyncVars.wg.Add(1)
	go config.feederWorker(ctx, 0, numTraceFiles, traceSource, feederWorkerSyncVars, qualityControlJobs)

	//stores the first error send over errorChan
	var backgroundError error
	go func() {
		signaledShutdown := false
		for recError := range errorChan {
			config.ErrLog.Printf("Received error : %v", recError)
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

	feederWorkerSyncVars.wg.Wait()
	config.InfoLog.Printf("feeder finished\n")
	//all feeders must be done before we can close the jobs chan
	close(jobs)
	close(qualityControlJobs)
	config.InfoLog.Printf("closed jobs channel, waiting wor compute workers to finish\n")
	//wait for compute workers to finish remaining jobs
	computeWorkerSyncVars.wg.Wait()
	config.InfoLog.Printf("compute workers finished. Waiting for snapshoter\n")
	//all workers done, no one can send snapshotDeltaShard results anymore
	close(snapshotDeltaShardChan)
	//wait for snapshoter to drain  snapshotDeltaShardChan
	snapshotWg.Wait()
	config.InfoLog.Printf("snapshoter finished")
	close(finalSnapshot)
	//all workers done, no one can send errors anymore
	close(errorChan)
	//all workers are done, no one can send results anymore
	close(resultChan)

	if backgroundError != nil {
		return nil, backgroundError
	}

	config.InfoLog.Printf("Merging results\n")
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
		return nil, fmt.Errorf("failed to retrieve final result")

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
