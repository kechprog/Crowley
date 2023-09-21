package main

import (
	"fmt"
	"os"
)

func main() {
	var outputFile string
	var ticker string

	if len(os.Args) > 1 {
		ticker = os.Args[1]
	} else {
		fmt.Println("No ticker provided")
		os.Exit(1)
	}

	if len(os.Args) > 2 {
		outputFile = os.Args[2]
	} else {
		outputFile = fmt.Sprintf("%s.csv", ticker)
	}

	fmt.Printf("Downloading data for %s, into %s\n", ticker, outputFile)
	RunPolygonStocks(&ticker, &outputFile)
}
