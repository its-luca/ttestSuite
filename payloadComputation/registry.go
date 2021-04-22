//Package payloadComputation provides an repository of functions/statistical tests to be performed by
//trace file data
package payloadComputation

//Contains interface for WorkerPayload and API to instantiate different functions at runtime
//If you want to add a new test, implement it using the WorkerPayload interface and add a new mapping
//to availableTests to make it accessible from the command line

import "fmt"

//WorkerPayloadCreator is the common constructor type for WorkerPayload
type WorkerPayloadCreator func(datapointsPerTrace int) WorkerPayload

//WorkerPayload interface is an abstraction for computations to be performed on trace data.
//Conceptually the computation is split in two functions: update which adds new data and may change the state
//and Finalize, which produces the result of the computation and must be IDEMPOTENT, i.e. not change the state
//of the object. The Merge function is intended to allow running multiple instances in parallel and still be able
//to produce the total result
type WorkerPayload interface {
	//Name returns a descriptive name for the performed computation
	Name() string
	//MaxSubroutines returns an (approximation) for the maximal amount of subroutines used by this payload.
	//It is intended as a hint of how many parallel instances should be spawned
	MaxSubroutines() int
	//Update processes and adds fixed and random to the internal state
	Update(fixed, random [][]float64)
	//Finalize returns the result of the payload computation based on the current state
	Finalize() ([]float64, error)
	//Merge updates the state of this WorkerPayload with the one of other (equal to calling Update on all data
	//that has been added to other)
	Merge(other WorkerPayload) error
	//DeepCopy returns a copy of this worker payload and all of its internal state
	DeepCopy() WorkerPayload
}

//availableTests hand edited list of available tests. If you add a new test add
//it to the list
var availableTests = map[string]WorkerPayloadCreator{
	"ttest": WorkerPayloadCreator(NewBatchMeanAndVar),
}

//GetAvailablePayloads returns a slice with all valid payload names
//that may be passed to GetWorkerPayloadCreator
func GetAvailablePayloads() []string {
	names := make([]string, 0, len(availableTests))
	for key := range availableTests {
		names = append(names, key)
	}
	return names
}

//GetWorkerPayloadCreator returns the WorkerPayloadCreator that is registered
//for name or an error if name is not found
func GetWorkerPayloadCreator(name string) (WorkerPayloadCreator, error) {
	creator, ok := availableTests[name]
	if !ok {
		return nil, fmt.Errorf("unknown test")
	}

	return creator, nil
}