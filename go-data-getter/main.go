package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"

	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
)

var jobsDone int = 0
var jobsDoneMutex sync.Mutex

func retrieveData(client *polygon.Client, ticker *string, start time.Time, end time.Time, wg *sync.WaitGroup, outputStream chan<- models.Agg) {
	defer wg.Done()

	var K50 int = 50 * 1000 // the fuck I'm supposed to do

	params := models.ListAggsParams{
		Ticker:     *ticker,
		Multiplier: 1,
		Timespan:   "minute",
		From:       models.Millis(start),
		To:         models.Millis(end),
		Limit:      &K50,
	}

	resp := client.ListAggs(context.TODO(), &params, models.APIKey("RpIjU8LJmuQ89fsUplw3a4sFhmAIvRaT"))
	if resp.Err() != nil {
		fmt.Println("Error retrieving data", resp.Err())
		return
	}

	for resp.Next() {
		outputStream <- resp.Item()
	}

	jobsDoneMutex.Lock()
	jobsDone += 1
	jobsDoneMutex.Unlock()
}

func main() {
	client := polygon.New("RpIjU8LJmuQ89fsUplw3a4sFhmAIvRaT")
	var outputFile string
	var ticker string
	var jobCounter int = 0
	now := time.Now()
	start := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	outputStream := make(chan models.Agg, 1000000)
	var wg sync.WaitGroup

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

	/* add jobs */
	for start.Before(now) {
		end := start.AddDate(0, 0, 1)
		wg.Add(1)
		jobCounter += 1
		go retrieveData(client, &ticker, start, end, &wg, outputStream)
		start = end
	}

	go func() {
		wg.Wait()
		close(outputStream)
	}()

	// Create and open CSV file
	file, err := os.Create(outputFile)
	if err != nil {
		fmt.Println("Could not create CSV file", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)

	// Write header
	writer.Write([]string{"Time", "Open", "High", "Low", "Close", "Volume"})
	for resp := range outputStream {

		fmt.Printf("Downloaded %f records\r", float64(jobsDone)/float64(jobCounter)*100)
		row := []string{
			time.Time(resp.Timestamp).String(),
			fmt.Sprintf("%f", resp.Open),
			fmt.Sprintf("%f", resp.High),
			fmt.Sprintf("%f", resp.Low),
			fmt.Sprintf("%f", resp.Close),
			fmt.Sprintf("%f", resp.Volume),
		}
		writer.Write(row)
		writer.Flush()
	}
}