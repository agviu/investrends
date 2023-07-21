package main

import (
	"fmt"
	"log"

	"github.com/agviu/investrends/collector"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	c, err := collector.NewCollector("./crypto.sqlite", "apikey.txt",
		"https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_WEEKLY&symbol=%s&market=EUR&apikey=%s",
		"digital_currency_list.csv")
	if err != nil {
		log.Fatal("unable to create collector object")
		return
	}

	err = collector.Run(c, 5)
	if err != nil {
		switch err.(type) {
		// case DataError:
		// @todo: Log stuff
		default:
			log.Fatal("There has been an error")
		}

		fmt.Println("Unfortunately there was an error running the program.")
		return
	}

	fmt.Println("Program ran succesfully.")
}
