/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"log"

	"github.com/agviu/investrends/collector"
	"github.com/spf13/cobra"
)

// collectorCmd represents the collector command
var collectorCmd = &cobra.Command{
	Use:   "collector",
	Short: "Collects asset's value from an external resource.",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Declare variables that can be altered by the command line interface.
		var dbName string
		var apiKeyPath string
		var production bool
		var currencyListPath string
		var indexFilePath string
		var clearBlacklist bool
		var goroutine bool

		dbName, _ = cmd.Flags().GetString("db-name")
		apiKeyPath, _ = cmd.Flags().GetString("api-key-file")
		currencyListPath, _ = cmd.Flags().GetString("currency-list-file")
		production, _ = cmd.Flags().GetBool("prod")
		indexFilePath, _ = cmd.Flags().GetString("index-path")
		clearBlacklist, _ = cmd.Flags().GetBool("clear-blacklist")
		goroutine, _ = cmd.Flags().GetBool("goroutine")

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
	},
}

func init() {
	rootCmd.AddCommand(collectorCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// collectorCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// collectorCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	collectorCmd.Flags().String("db-name", "./crypto.sqlite", "Path to the sqlite database file, name included")
	collectorCmd.Flags().String("api-key-file", "apikey.txt", "Path to the text file that contains the API Key")
	collectorCmd.Flags().String("currency-list-file", "digital_currency_list.csv", "Path to the CSV files that stores the list of currencies")
	collectorCmd.Flags().Bool("prod", false, "Indicates if the program will run in production mode.")
	collectorCmd.Flags().String("index-path", "index.txt", "Path to the text file where the index is stored.")
	collectorCmd.Flags().Bool("clear-blacklist", false, "Clear the blacklist before starting the collection.")
	collectorCmd.Flags().Bool("goroutine", false, "Specify if it should use goroutines for processing.")
}
