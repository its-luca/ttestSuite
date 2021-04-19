package mocks

import (
	"encoding/binary"
	"math"
	mockTraceSource "ttestSuite/mocks/traceSource"
	"ttestSuite/mocks/wfm"
	"ttestSuite/traceSource"
	"ttestSuite/wfm"
)

//CreateFloatSourceParserPair creates a block reader and parser with len(blocksAsFloats) blocks where block nr x
//contains the traces blocksAsFloat[x]. If failAfter is >= 0 the block reader will fail for each number greater than failAfter
func CreateFloatSourceParserPair(blocksAsFloat [][][]float64, caseDataPerBlock [][]int, failAfter int) (traceSource.TraceBlockReader, wfm.TraceParser) {

	//parse dimension and calculate size of single binary block
	traceCount := uint64(len(blocksAsFloat[0]))
	binarySize := uint64(binary.Size(traceCount))
	var datapointsPerTrace uint64
	if traceCount == 0 {
		datapointsPerTrace = 0
		binarySize += uint64(binary.Size(datapointsPerTrace))
	} else {
		datapointsPerTrace = uint64(len(blocksAsFloat[0][0]))
		binarySize += uint64(binary.Size(datapointsPerTrace))
		binarySize += (traceCount + datapointsPerTrace) * uint64(binary.Size(blocksAsFloat[0][0][0]))
	}

	//create binary blocks
	binBlocks := make([][]byte, len(blocksAsFloat))
	for blockIDX := 0; blockIDX < len(blocksAsFloat); blockIDX++ {
		binBlocks[blockIDX] = make([]byte, binarySize)
		offset := 0
		offset += binary.PutUvarint(binBlocks[blockIDX][offset:], traceCount)
		offset += binary.PutUvarint(binBlocks[blockIDX][offset:], datapointsPerTrace)
		for traceIDX := uint64(0); traceIDX < traceCount; traceIDX++ {
			for datapointIDX := uint64(0); datapointIDX < datapointsPerTrace; datapointIDX++ {
				v := blocksAsFloat[blockIDX][traceIDX][datapointIDX]
				vAsBits := math.Float64bits(v)
				offset += binary.PutUvarint(binBlocks[blockIDX][offset:], vAsBits)
			}
		}
	}

	reader := &mockTraceSource.MockBlockReader{
		BlockCount:       len(blocksAsFloat),
		Blocks:           binBlocks,
		CaseDataPerBlock: caseDataPerBlock,
		FailAfter:        failAfter,
	}
	parser := &mockWFM.WFMFloatParser{}

	return reader, parser
}
