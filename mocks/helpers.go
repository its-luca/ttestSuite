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
func CreateFloatSourceParserPair(blocksAsFloat [][][]float64, caseDataPerBlock [][]int, failAfter int) (traceSource.TraceBlockReader, wfm.TraceParser, error) {
	traceCount := uint64(len(blocksAsFloat[0]))
	var datapointsPerTrace uint64
	if traceCount == 0 {
		datapointsPerTrace = 0
	} else {
		datapointsPerTrace = uint64(len(blocksAsFloat[0][0]))
	}
	//create binary blocks
	binBlocks := make([][]byte, len(blocksAsFloat))
	for blockIDX := 0; blockIDX < len(blocksAsFloat); blockIDX++ {
		buff := &bytes.Buffer{}
		if err := binary.Write(buff, binary.LittleEndian, traceCount); err != nil {
			return nil, nil, fmt.Errorf("failed to write traceCount : %v", err)
		}
		if err := binary.Write(buff, binary.LittleEndian, datapointsPerTrace); err != nil {
			return nil, nil, fmt.Errorf("failed to write datapointsPerTrace : %v", err)
		}
		for traceIDX := uint64(0); traceIDX < traceCount; traceIDX++ {
			for datapointIDX := uint64(0); datapointIDX < datapointsPerTrace; datapointIDX++ {
				v := blocksAsFloat[blockIDX][traceIDX][datapointIDX]
				vAsBits := math.Float64bits(v)
				if err := binary.Write(buff, binary.LittleEndian, vAsBits); err != nil {
					return nil, nil, fmt.Errorf("failed to write float entry %v : %v", datapointIDX, err)
				}
			}
		}
		binBlocks[blockIDX] = buff.Bytes()
	}

	reader := &mockTraceSource.MockBlockReader{
		BlockCount:       len(blocksAsFloat),
		Blocks:           binBlocks,
		CaseDataPerBlock: caseDataPerBlock,
		FailAfter:        failAfter,
	}
	parser := &mockWFM.WFMFloatParser{}

	return reader, parser, nil
}
