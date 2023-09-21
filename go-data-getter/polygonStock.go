package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"time"

	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
	"go.uber.org/atomic"
)

func retrieveData(client *polygon.Client, ticker *string, start time.Time, end time.Time, outputStream chan<- models.Agg, jobsDone *atomic.Uint64) {
	defer jobsDone.Inc()
	var K50 int = 50 * 1000 // the fuck I'm supposed to do

	params := models.ListAggsParams{
		Ticker:     *ticker,
		Multiplier: 1,
		Timespan:   "minute",
		From:       models.Millis(start),
		To:         models.Millis(end),
		Limit:      &K50,
	}

	apiKey := "eOeKEunwSPrd56Of7Bwpn7ScHIxRRcKC"
	resp := client.ListAggs(context.TODO(), &params, models.APIKey(apiKey))
	if resp.Err() != nil {
		fmt.Println("Error retrieving data", resp.Err())
		return
	}

	for resp.Next() {
		outputStream <- resp.Item()
	}
}

func RunPolygonStocks(ticker *string, outputPath *string) {
	totalJobs := 0
	jobsDone := atomic.NewUint64(0)

	apiKey := "eOeKEunwSPrd56Of7Bwpn7ScHIxRRcKC"

	client := polygon.New(apiKey)

	now := time.Now()
	start := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)

	outputStream := make(chan models.Agg, 1000000)

	/* add jobs */
	for start.Before(now) {
		end := start.AddDate(0, 0, 1)
		totalJobs += 1
		go retrieveData(client, ticker, start, end, outputStream, jobsDone)
		start = end
	}

	// Create and open CSV file
	file, err := os.Create(*outputPath)
	if err != nil {
		fmt.Println("Could not create CSV file", err)
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Write([]string{"Time", "Open", "High", "Low", "Close", "Volume"})

	/* wait for jobs to finish */
	for resp := range outputStream {

		fmt.Printf("Downloaded %f records\r", float64(jobsDone.Load())/float64(totalJobs)*100)
		if jobsDone.Load() == uint64(totalJobs) {
			os.Exit(0)
		}
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
