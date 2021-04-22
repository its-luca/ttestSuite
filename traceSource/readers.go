package traceSource

import (
	"bufio"
	"bytes"
	"fmt"
)

//ParseCaseLog expects the raw bytes of case log file ( num traces lines containing either 1 or 0. 1 stands for random case, 0 for fixed case
func ParseCaseLog(rawLog []byte) ([]int, error) {
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
			return nil, fmt.Errorf("unexpected entry %v in case log line %v", line, len(caseLog))
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning for lines : %v\n", err)
	}
	return caseLog, nil
}

//TraceBlockReader is the common interface providing raw(unparsed) trace file data
type TraceBlockReader interface {
	//TotalBlockCount returns the total number of trace blocks/files
	TotalBlockCount() int
	//GetBlock reads the block identified by nr and returns raw file along with case markers
	GetBlock(nr int) ([]byte, []int, error)
}
