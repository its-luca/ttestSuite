package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"math"
)

const (
	offsetOfOffsetCurveBuffer4BInt = 0x010
	offsetOfNumberOfFF4BUint       = 0x048
	offsetOfFormat4BInt            = 0x0f0
	//relative to start of curve buffer
	offsetOfPrechargeStart4BUint = 0x332
	//relative to start of curve buffer
	offsetOfOffsetDataStart4BUint = 0x336
	//voltage = (datapoint*scale)+offset
	offsetOfDimScale8BDouble = 0x0a8
	//voltage = (datapoint*scale)+offset
	offsetOfDimOffset8Double      = 0x0b0
	offsetHorizontalDimSize4BUint = 0x1f8
	//value for NaN
	offsetOfNValue4B              = 0x0f8
	offsetOfOffsetPostchargeStart = 0x33a
	offsetOfOffsetPostchargeStop  = 0x33e
)

//based on tectronix wfm spec, has basically zero error checks right now but seems to work
func wfmToTraces(rawWFM []byte) ([][]float64, error) {

	numberOfFF := binary.LittleEndian.Uint32(rawWFM[offsetOfNumberOfFF4BUint : offsetOfNumberOfFF4BUint+4])
	//log.Printf("Number of fast frames is %v\n",numberOfFF)

	datapointsPerFF := int(binary.LittleEndian.Uint32(rawWFM[offsetHorizontalDimSize4BUint : offsetHorizontalDimSize4BUint+4]))
	//log.Printf("Datapoints per fast frame is  %v\n",datapointsPerFF)

	formatIdentifier := int(binary.LittleEndian.Uint32(rawWFM[offsetOfFormat4BInt : offsetOfFormat4BInt+4]))
	if formatIdentifier != 0 {
		return nil, fmt.Errorf("format identifier has unexpected value. this will be resolved in future version")
	}
	//log.Printf("Format identifier is   %v\n",formatIdentifier)

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
	//log.Printf("data start = %x post start = %x\n => expecting %vdatapoints",offsetDataStart,offsetPostStartOffset,(offsetPostStartOffset-offsetDataStart)/2)

	frames := make([][]float64, numberOfFF)
	for frameIDX, _ := range frames {
		start := offsetCurveBuffer + (frameIDX+1)*preChargeBytes + (frameIDX * datapointsPerFF * 2) + frameIDX*postChargeBytes
		end := start + (datapointsPerFF * 2)
		rawFrame := rawWFM[start:end]
		frames[frameIDX] = make([]float64, datapointsPerFF)
		for i, _ := range frames[frameIDX] {
			rawValue := binary.LittleEndian.Uint16(rawFrame[2*i : 2*i+2])
			frames[frameIDX][i] = (float64(rawValue) * yScale) + yOffset
		}
	}
	return frames, nil
}

//array with num traces entries. 1 stands for random case, 0 for fixed case
func parseCaseLog(rawLog []byte) ([]int, error) {
	scanner := bufio.NewScanner(bytes.NewReader(rawLog))
	scanner.Split(bufio.ScanLines)

	caseLog := make([]int, 0)
	for scanner.Scan() {
		line := scanner.Text()

		switch line {
		case "1":
			caseLog = append(caseLog, 1)
			break
		case "0":
			caseLog = append(caseLog, 0)
			break
		default:
			return nil, fmt.Errorf("unexpected entry %v in case log", line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning for lines : %v\n", err)
	}
	return caseLog, nil
}

func main() {
	rawWFM, err := ioutil.ReadFile("./sample.wfm")
	if err != nil {
		log.Fatalf("Failed to read wfm file : %v\n", err)
	}
	frames, err := wfmToTraces(rawWFM)
	if err != nil {
		log.Fatal("Failed to parse wfm file")
	}

	rawCaseFile, err := ioutil.ReadFile("./sample-case-log.txt")
	if err != nil {
		log.Fatalf("Failed to read case file : %v\n", err)
	}
	caseLog, err := parseCaseLog(rawCaseFile)
	if err != nil {
		log.Fatalf("Failed to parse case file : %v\n", err)
	}

	if lenLog, lenFrames := len(caseLog), len(frames); lenLog != lenFrames {
		log.Fatalf("Log length (%v) and frame count (%v) don't match\n", lenLog, lenFrames)
	}

	for i, _ := range frames {
		log.Printf("Trace %v is case %v, first entry %v last entry %v\n", i, caseLog[i], frames[i][0], frames[i][len(frames[i])-1])

	}

}
