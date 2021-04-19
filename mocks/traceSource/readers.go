package mockTraceSource

import "fmt"

type MockBlockReader struct {
	BlockCount       int
	Blocks           [][]byte
	CaseDataPerBlock [][]int
	//if > 0, return error for all blocks with a greater index
	FailAfter int
}

func (m MockBlockReader) TotalBlockCount() int {
	return m.BlockCount
}

func (m MockBlockReader) GetBlock(nr int) ([]byte, []int, error) {
	if m.FailAfter >= 0 && nr > m.FailAfter {
		return nil, nil, fmt.Errorf("programmed reader failure")
	}
	return m.Blocks[nr], m.CaseDataPerBlock[nr], nil
}
