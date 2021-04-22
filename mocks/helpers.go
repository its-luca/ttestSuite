package mocks

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	mockTraceSource "ttestSuite/mocks/traceSource"
	"ttestSuite/mocks/wfm"
	"ttestSuite/traceSource"
	"ttestSuite/wfm"
)

//ParseCSVToFloat64 takes a raw csv file and parses each line as into a float64 slice
func ParseCSVToFloat64(csvData []byte) ([][]float64, error) {
	//parse csv to array
	scanner := bufio.NewScanner(bytes.NewReader(csvData))
	buf := make([]byte, 0, len(csvData))
	scanner.Buffer(buf, len(buf))
	scanner.Split(bufio.ScanLines)

	parsedData := make([][]float64, 0)
	for scanner.Scan() {
		line := scanner.Text()
		tokens := strings.Split(line, ",")
		row := make([]float64, 0, len(tokens))
		for _, token := range tokens {
			f, err := strconv.ParseFloat(strings.TrimSpace(token), 64)
			if err != nil {
				return nil, fmt.Errorf("Failed to parse test input %v to float64 : %v\n", token, err)
			}
			row = append(row, f)
		}
		parsedData = append(parsedData, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("Failed to parse test data faile : %v\n", err)
	}

	return parsedData, nil
}

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
