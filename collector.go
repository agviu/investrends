package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/agviu/investrends/collector"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	// Declare variables that can be altered by the command line interface.
	var dbName string
	var apiKeyPath string
	var production bool
	var currencyListPath string

	flag.StringVar(&dbName, "db-name", "./crypto.sqlite", "Path to the sqlite database file, name icluded")
	flag.StringVar(&apiKeyPath, "api-key-file", "apikey.txt", "Path to the text file that contains the API Key")
	flag.StringVar(&currencyListPath, "currency-list-file", "digital_currency_list.csv", "Path to the CSV files that stores the list of currencies. Check: https://www.alphavantage.co/digital_currency_list/")
	flag.BoolVar(&production, "prod", false, "Indicates if the program will run in production mode.")
	flag.Parse()

	// Create a collector with values passed by CLI (or default values)
	c, err := collector.NewCollector(dbName, apiKeyPath,
		"https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_WEEKLY&symbol=%s&market=EUR&apikey=%s",
		currencyListPath, production)
	if err != nil {
		log.Fatal("unable to create collector object: ", err.Error())
	}

	// Run the collector procedure.
	err = collector.Run(c, 5)
	if err != nil {
		// @todo: How to return an error from the program?
		log.Fatal("Unfortunately there was an error running the program.", err.Error())
	}

	fmt.Println("Program ran succesfully.")
}
