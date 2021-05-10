package mockWFM

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

//WFMFloatParser interprets the passed byte data as as follows
//<traceCount uint64>,<datapointsPerTrace uint64>,<traceCount*datapointsPerTrace float64 objects>
//followed by float64 objects. See CreateFloatSourceParserPair for easy instantiation
type WFMFloatParser struct{}

func (m WFMFloatParser) ParseTraces(raw []byte, buffer [][]float64) ([][]float64, error) {

	r := bytes.NewReader(raw)
	var traceCount, datapointsPerTrace uint64
	if err := binary.Read(r, binary.LittleEndian, &traceCount); err != nil {
		return nil, fmt.Errorf("failed to parse traceCount %v", err)
	}

	if err := binary.Read(r, binary.LittleEndian, &datapointsPerTrace); err != nil {
		return nil, fmt.Errorf("failed to parse datapoints per trace")
	}
	if buffer == nil {
		buffer = make([][]float64, traceCount)
	}

	for traceIDX := uint64(0); traceIDX < traceCount; traceIDX++ {
		if buffer[traceIDX] == nil {
			buffer[traceIDX] = make([]float64, datapointsPerTrace)
		}
		var value uint64
		for datapointIDX := uint64(0); datapointIDX < datapointsPerTrace; datapointIDX++ {
			if err := binary.Read(r, binary.LittleEndian, &value); err != nil {
				return nil, fmt.Errorf("failed to parse float entry %v in trace %v : %v\n", datapointIDX, traceIDX, err)
			}
			//offset += n
			valueAsFloat := math.Float64frombits(value)
			buffer[traceIDX][datapointIDX] = valueAsFloat
		}
	}
	return buffer, nil
}

func (m WFMFloatParser) GetNumberOfTraces(raw []byte) (int, error) {
	offset := 0
	traceCount, byteSize := binary.Uvarint(raw[offset:])
	if byteSize <= 0 {
		return 0, fmt.Errorf("failed to parse traceCount field, read only %v bytes expected %v\n", byteSize, binary.Size(traceCount))
	}
	return int(traceCount), nil
}
