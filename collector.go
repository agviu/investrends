package main

import (
	"flag"
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
	var indexFilePath string
	var clearBlacklist bool
	var goroutine bool

	flag.StringVar(&dbName, "db-name", "./crypto.sqlite", "Path to the sqlite database file, name icluded")
	flag.StringVar(&apiKeyPath, "api-key-file", "apikey.txt", "Path to the text file that contains the API Key")
	flag.StringVar(&currencyListPath, "currency-list-file", "digital_currency_list.csv", "Path to the CSV files that stores the list of currencies. Check: https://www.alphavantage.co/digital_currency_list/")
	flag.BoolVar(&production, "prod", false, "Indicates if the program will run in production mode.")
	flag.StringVar(&indexFilePath, "index-path", "index.txt", "Path to the text file where the index is stored.")
	flag.BoolVar(&clearBlacklist, "clear-blacklist", false, "Clear the blacklist before starting the collection.")
	flag.BoolVar(&goroutine, "goroutine", false, "Specify if it should use goroutines for processing.")
	flag.Parse()

	// Create a collector with values passed by CLI (or default values)
	c, err := collector.NewCollector(dbName, apiKeyPath,
		"https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_WEEKLY&symbol=%s&market=EUR&apikey=%s",
		currencyListPath, production, indexFilePath)
	if err != nil {
		log.Fatalln("unable to create collector object: ", err.Error())
	}

	// Run the collector procedure.
	var processed int
	if goroutine {
		processed, err = collector.RunGoRoutines(c, 5, clearBlacklist, true)
	} else {
		processed, err = collector.Run(c, 5, clearBlacklist)

	}
	if err != nil {
		log.Fatal("Unfortunately there was an error running the program.", err.Error())
	}

	log.Println("Processed", processed, "items")
	log.Println("Program ran succesfully.")
}
