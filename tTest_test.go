package main

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type mockFailingTraceReader struct {
	blockCount    int
	failThreshold int
}

func (m mockFailingTraceReader) TotalBlockCount() int {
	return m.blockCount
}

func (m mockFailingTraceReader) GetBlock(nr int) ([]byte, []int, error) {
	if nr < m.failThreshold {
		return []byte{0, 1, 2, 3}, []int{0, 0, 1, 1}, nil
	}
	return nil, nil, fmt.Errorf("dummy reader error")
}

type mockTraceParser struct {
	frames [][]float64
}

func (m mockTraceParser) ParseTraces(_ []byte, _ [][]float64) ([][]float64, error) {
	return m.frames, nil
}

func (m mockTraceParser) GetNumberOfTraces(_ []byte) (int, error) {
	return len(m.frames), nil
}

func TestTTest_CleanShutdownAfterReaderCrash(t *testing.T) {

	mockSource := mockFailingTraceReader{
		blockCount:    2,
		failThreshold: 1,
	}

	mockParser := mockTraceParser{
		frames: [][]float64{{1, 2, 3, 4}, {5, 6, 7, 8}},
	}

	config := Config{
		ComputeWorkers: 2,
		FeederWorkers:  1,
		BufferSizeInGB: 1,
	}

	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan interface{})
	defer testCancel()
	var tTestErr error
	go func() {
		_, tTestErr = TTest(mockSource, mockParser, config)
		done <- nil
	}()

	select {
	case <-testCtx.Done():
		t.Errorf("TTtest is stuck upon unexpected TraceReader crash (on a very slow target this can be false positive, adjust timeout)")
	case <-done: //ttest call returned
		if tTestErr == nil {
			t.Errorf("expected error got none")
		}

	}
}
