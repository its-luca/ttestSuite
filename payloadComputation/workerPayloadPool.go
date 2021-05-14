package payloadComputation

import "sync"

//WorkerPayloadPool wraps sync.Pool for WorkerPayload
type WorkerPayloadPool struct {
	pool sync.Pool
}

//NewWorkerPayloadPool creates a new pool of worker payloads with the same constructor arguments
func NewWorkerPayloadPool(creator WorkerPayloadCreator, datapointsPerTrace int) *WorkerPayloadPool {
	wpp := &WorkerPayloadPool{
		pool: sync.Pool{
			New: func() interface{} {
				return creator(datapointsPerTrace)
			},
		},
	}
	return wpp
}

//Get returns a WorkerPayload with wiped state (either newly allocated or Reset() called)
func (wpp *WorkerPayloadPool) Get() WorkerPayload {
	wp, ok := wpp.pool.Get().(WorkerPayload)
	if !ok {
		panic("WorkerPayloadPool encountered unexpected type ")
	}
	return wp
}

//Put wipes the state in wp and makes it available for others.
func (wpp *WorkerPayloadPool) Put(wp WorkerPayload) {
	//wipe state
	wp.Reset()
	//put back in pool
	wpp.pool.Put(wp)
}
