package main

//CLI interface to generate deterministic random number sequences and save them to a csv file. Intention is to
//create large test inputs that we can check with external tools like matlab without needing to explicity store
//them in the code or a file, as we can regenerate them by calling the deterministic rng with the same seed
import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"ttestSuite/testUtils"
)

func main() {
	seed := flag.Int64("seed", 42, "Seed for pseudo RNG")
	outPath := flag.String("out", "", "where to store output")
	count := flag.Int("count", 100, "how many numbers should be generated")

	flag.Parse()

	if *outPath == "" {
		fmt.Println("Set \"out\"!")
		flag.PrintDefaults()
		os.Exit(1)
	}

	buf := testUtils.DRNGFloat64Slice(*count, *seed)

	outFile, err := os.Create(*outPath)
	if err != nil {
		log.Fatalf("Failed to create out file : %v", err)
	}
	defer func() {
		if err := outFile.Close(); err != nil {
			log.Printf("Failed to close outFile : %v", err)
		}
	}()
	csvWriter := csv.NewWriter(outFile)
	defer csvWriter.Flush()
	bufAsString := make([]string, len(buf))
	for i, v := range buf {
		bufAsString[i] = fmt.Sprintf("%f", v)
	}

	if err := csvWriter.Write(bufAsString); err != nil {
		log.Fatalf("Failed to write as csv : %v", err)
	}
}
