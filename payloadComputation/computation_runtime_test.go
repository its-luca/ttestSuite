package payloadComputation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
	"ttestSuite/mocks"
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
	creator, err := GetWorkerPayloadCreator("ttest")
	if err != nil {
		t.Fatalf("failed to setup worker payload creator for test : %v\n", err)
	}

	config := ComputationConfig{
		ComputeWorkers:       2,
		BufferSizeInGB:       1,
		SnapshotInterval:     1,
		WorkerPayloadCreator: creator,
		SnapshotSaver: func(_ []float64, _ WorkerPayload, _ int) error {
			return nil
		},
	}

	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan interface{})
	defer testCancel()
	var tTestErr error
	go func() {
		_, tTestErr = Run(context.Background(), mockSource, mockParser, config)
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

type mockTest struct {
	sum float64
}

func (m *mockTest) Encode(_ io.Writer) error {
	panic("implement me")
}

func (m *mockTest) Decode(_ io.Reader) error {
	panic("implement me")
}

func (m *mockTest) Update(fixed, random [][]float64) {
	for _, innerSlice := range fixed {
		for _, v := range innerSlice {
			m.sum += v
		}
	}
	for _, innerSlice := range random {
		for _, v := range innerSlice {
			m.sum += v
		}
	}
}

func (m *mockTest) Finalize() ([]float64, error) {
	return []float64{m.sum}, nil
}

func (m *mockTest) Merge(payload WorkerPayload) error {
	asMockTest, ok := payload.(*mockTest)
	if !ok {
		return fmt.Errorf("tried to merge mock test with other type")
	}
	m.sum += asMockTest.sum
	return nil
}

func (m *mockTest) DeepCopy() WorkerPayload {
	return &mockTest{sum: m.sum}
}

func (m *mockTest) Name() string {
	return "mockTest"
}

func (m *mockTest) MaxSubroutines() int {
	return 0
}

func createMockTest(_ int) WorkerPayload {
	return &mockTest{sum: 0}
}

func TestTTest_SnapshotValues(t *testing.T) {
	blocks := [][][]float64{
		{[]float64{1, 1, 1, 1}},
		{[]float64{1, 1, 0, 0}},
		{[]float64{0, 0, 0, 0}},
		{[]float64{1, 1, 1, 1}},
		{[]float64{1, 1, 1, 1}},
		{[]float64{1, 1, 0, 0}},
		{[]float64{0, 0, 0, 0}},
		{[]float64{1, 1, 1, 1}},
	}

	//just to fulfil interface, does not matter for our mock function
	caseDataPerBlock := [][]int{
		{0},
		{0},
		{0},
		{0},
		{0},
		{0},
		{0},
		{0},
	}

	blockSource, parser := mocks.CreateFloatSourceParserPair(blocks, caseDataPerBlock, -1)

	type testInput struct {
		wantSnapshotValues [][]float64
		wantFinal          float64
		intervalSize       int
		wantErr            error
	}
	inputs := []testInput{
		{
			wantSnapshotValues: [][]float64{
				{4},
				{6},
				{6},
				{10},
				{14},
				{16},
				{16},
				{20},
			},
			wantFinal:    20.0,
			intervalSize: 1,
		},
		{
			wantSnapshotValues: [][]float64{
				{6},
				{10},
				{16},
				{20},
			},
			wantFinal:    20.0,
			intervalSize: 2,
		},
		{
			wantSnapshotValues: [][]float64{
				{20},
			},
			wantFinal:    20.0,
			intervalSize: 8,
		},

		{
			wantSnapshotValues: [][]float64{},
			wantFinal:          20.0,
			intervalSize:       9,
			wantErr:            ErrInvSnapInterval, //because interval is large than trace file count
		},
	}

	//to check for racy behaviour we process the test inputs multiple times
	for repetition := 0; repetition < 50; repetition++ {
		//try each input with different worker counts to detect racy behaviour
		for workerCount := 1; workerCount < 8; workerCount++ {
			for _, v := range inputs {
				gotSnapshotData := make([]WorkerPayload, 0)
				snapshotSaver := func(_ []float64, rawSnapshot WorkerPayload, _ int) error {
					gotSnapshotData = append(gotSnapshotData, rawSnapshot)
					return nil
				}
				config := ComputationConfig{
					ComputeWorkers:       workerCount,
					BufferSizeInGB:       1,
					SnapshotInterval:     v.intervalSize,
					WorkerPayloadCreator: createMockTest,
					SnapshotSaver:        snapshotSaver,
				}

				gotFinalValue, tTestErr := Run(context.Background(), blockSource, parser, config)
				if tTestErr != nil {
					//if we wanted this error we can accept the test at this point
					if v.wantErr != nil && errors.Is(tTestErr, v.wantErr) {
						continue
					}
					t.Fatalf("Unepxected error : %v\n", tTestErr)
				}

				if wantSnaps, gotSnaps := len(v.wantSnapshotValues), len(gotSnapshotData); wantSnaps != gotSnaps {
					t.Errorf("wanted %v snapshots got %v\n", wantSnaps, gotSnaps)
				}

				for i := range v.wantSnapshotValues {
					gotSnapResult, err := gotSnapshotData[i].Finalize()
					if err != nil {
						t.Fatalf("Unexpected error finalizing snapshotDeltaShard data at index %v : %v\n", i, err)
					}
					if gotSnapResult[0] != v.wantSnapshotValues[i][0] {
						t.Errorf("snapshot %v: want %v got %v\n", i, v.wantSnapshotValues[i][0], gotSnapResult[0])
					}
				}

				gotFinal, err := gotFinalValue.Finalize()
				if err != nil {
					t.Fatalf("unexpected error finalizing result : %v\n", err)
				}

				if gotFinal[0] != v.wantFinal {
					t.Errorf("want final %v got %v\n", v.wantFinal, gotFinal[0])
				}
			}
		}

	}

}
