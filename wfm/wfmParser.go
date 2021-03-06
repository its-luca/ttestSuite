package wfm

//extracts trace data from tektronix wfm files

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	offsetOfOffsetCurveBuffer4BInt = 0x010
	offsetOfNumberOfFF4BUint       = 0x048
	offsetOfFormat4BInt            = 0x0f0
	//relative to start of curve buffer
	//offsetOfPrechargeStart4BUint = 0x332
	//relative to start of curve buffer
	offsetOfOffsetDataStart4BUint = 0x336
	//voltage = (datapoint*scale)+offset
	offsetOfDimScale8BDouble = 0x0a8
	//voltage = (datapoint*scale)+offset
	offsetOfDimOffset8Double      = 0x0b0
	offsetHorizontalDimSize4BUint = 0x1f8
	//value for NaN
	//offsetOfNValue4B              = 0x0f8
	offsetOfOffsetPostchargeStart = 0x33a
	offsetOfOffsetPostchargeStop  = 0x33e
	offsetSetType                 = 0x04e
)

type TraceParser interface {
	ParseTraces([]byte, [][]float64) ([][]float64, error)
	GetNumberOfTraces([]byte) (int, error)
}

//Parser is an adapter to the TraceParser interface
type Parser struct{}

func (p Parser) GetNumberOfTraces(bytes []byte) (int, error) {
	return GetNumberOfTraces(bytes)
}

func (p Parser) ParseTraces(raw []byte, frames [][]float64) ([][]float64, error) {
	return ParseTraces(raw, frames)
}

func GetNumberOfTraces(rawWFM []byte) (int, error) {
	if binary.LittleEndian.Uint32(rawWFM[offsetSetType:offsetSetType+4]) != uint32(1) {
		return 0, fmt.Errorf("not a fastframe file")
	}
	//according to wfm spec +1 gives the number of frames
	return int(binary.LittleEndian.Uint32(rawWFM[offsetOfNumberOfFF4BUint:offsetOfNumberOfFF4BUint+4])) + 1, nil
}

//ParseTraces extracts trace data from a tektronix wfm file. Very rough state; has basically zero error checks right now but
func ParseTraces(rawWFM []byte, frames [][]float64) ([][]float64, error) {

	numberOfTraces, err := GetNumberOfTraces(rawWFM)
	if err != nil {
		return nil, err
	}
	//log.Printf("Number of fast frames is %v\n", numberOfTraces)

	datapointsPerFF := int(binary.LittleEndian.Uint32(rawWFM[offsetHorizontalDimSize4BUint : offsetHorizontalDimSize4BUint+4]))
	//log.Printf("Datapoints per fast frame is  %v\n", datapointsPerFF)

	formatIdentifier := int(binary.LittleEndian.Uint32(rawWFM[offsetOfFormat4BInt : offsetOfFormat4BInt+4]))
	if formatIdentifier != 0 {
		return nil, fmt.Errorf("format identifier has unexpected value. this will be resolved in future version")
	}
	//log.Printf("Format identifier is   %v\n", formatIdentifier)

	//naNValue := uint16(binary.LittleEndian.Uint32(rawWFM[offsetOfNValue4B:offsetOfNValue4B+4]))
	//log.Printf("naNValue  is  %v\n",naNValue)

	yScale := math.Float64frombits(binary.LittleEndian.Uint64(rawWFM[offsetOfDimScale8BDouble : offsetOfDimScale8BDouble+8]))
	yOffset := math.Float64frombits(binary.LittleEndian.Uint64(rawWFM[offsetOfDimOffset8Double : offsetOfDimOffset8Double+8]))

	offsetCurveBuffer := int(binary.LittleEndian.Uint32(rawWFM[offsetOfOffsetCurveBuffer4BInt : offsetOfOffsetCurveBuffer4BInt+4]))
	offsetDataStart := int(binary.LittleEndian.Uint32(rawWFM[offsetOfOffsetDataStart4BUint : offsetOfOffsetDataStart4BUint+4]))
	offsetPostStartOffset := int(binary.LittleEndian.Uint32(rawWFM[offsetOfOffsetPostchargeStart : offsetOfOffsetPostchargeStart+4]))
	offsetPostStopOffset := int(binary.LittleEndian.Uint32(rawWFM[offsetOfOffsetPostchargeStop : offsetOfOffsetPostchargeStop+4]))

	postChargeBytes := offsetPostStopOffset - offsetPostStartOffset
	preChargeBytes := offsetDataStart
	//log.Printf("postcharge length is %v\n",postChargeBytes)
	//log.Printf("precharge length is %v\n",preChargeBytes)
	//log.Printf("data start = %x post start = %x\n => expecting %v datapoints",offsetDataStart,offsetPostStartOffset,(offsetPostStartOffset-offsetDataStart)/2)

	if frames == nil {
		frames = make([][]float64, numberOfTraces)
	}

	var start, end int
	var rawFrame []byte
	for frameIDX := range frames {
		start = offsetCurveBuffer + (frameIDX+1)*preChargeBytes + (frameIDX * datapointsPerFF * 2) + frameIDX*postChargeBytes
		end = start + (datapointsPerFF * 2)
		rawFrame = rawWFM[start:end]
		if frames[frameIDX] == nil {
			frames[frameIDX] = make([]float64, datapointsPerFF)
		}
		var rawValue int16
		for i := range frames[frameIDX] {
			rawValue = int16(binary.LittleEndian.Uint16(rawFrame[2*i : 2*i+2]))
			frames[frameIDX][i] = (float64(rawValue) * yScale) + yOffset
		}
	}
	return frames, nil
}
