package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"ttestSuite/tPlot"
)

func main() {

	in := flag.String("in", "", "Path to a csv file with t-test results as floats")
	thresh := flag.Float64("thresh", 6, "Threshold value for plot")

	flag.Parse()

	if *in == "" {
		fmt.Printf("Please set \"in\" parameter!\n")
		flag.PrintDefaults()
		return
	}

	inFile, err := os.Open(*in)
	if err != nil {
		fmt.Printf("Failed to open input file %v : %v\n", *in, err)
		return
	}
	defer func() {
		if err := inFile.Close(); err != nil {
			log.Printf("Failed to close %v : %v", inFile.Name(), err)
		}
	}()

	csvReader := csv.NewReader(bufio.NewReader(inFile))
	tValuesAsStrings, err := csvReader.Read()
	if err != nil {
		fmt.Printf("Failed to parse csv file : %v\n", err)
		return
	}
	tValues := make([]float64, len(tValuesAsStrings))
	for i := range tValuesAsStrings {
		tValues[i], err = strconv.ParseFloat(tValuesAsStrings[i], 64)
		if err != nil {
			log.Printf("Failed to parse %v-th entry %v to float : %v\n", i, tValuesAsStrings[i], err)
			return
		}
	}

	p, err := tPlot.Plot(tValues, *thresh)
	if err != nil {
		log.Fatalf("Failed to create plot : %v\n", err)
	}

	err = p.Save(800, 600, "t-plot.png")
	if err != nil {
		log.Panic(err)
	}
}
