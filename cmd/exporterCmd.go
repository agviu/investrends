package cmd

import (
	"fmt"
	"log"

	"github.com/agviu/investrends/exporter"
	"github.com/spf13/cobra"
)

// Define variables to hold the flag values
var dbName string
var jsonOutputPath string

// exporterCmd represents the exporter command
var exporterCmd = &cobra.Command{
	Use:   "exporter",
	Short: "Exports data from a SQLite database to a JSON file",
	Long: `exporter is a command-line utility that exports data from a specified SQLite database file
to a JSON file. It requires two arguments: the path to the SQLite file and the path for the output JSON file.`,
	Run: func(cmd *cobra.Command, args []string) {

		// Call the ExportToJSON function with the provided arguments
		err := exporter.ExportToJSON(dbName, jsonOutputPath)
		if err != nil {
			log.Fatalf("Failed to export data: %v", err)
		}

		fmt.Printf("Data exported successfully from '%s' to '%s'\n", dbName, jsonOutputPath)
	},
}

func init() {
	rootCmd.AddCommand(exporterCmd)

	// Here you will define your flags and configuration settings.

	// Define the named flags for the exporterCmd
	exporterCmd.Flags().StringVarP(&dbName, "db-name", "d", "", "Path to the sqlite database file")
	exporterCmd.Flags().StringVarP(&jsonOutputPath, "json", "j", "", "Path to the output JSON file")

	// Mark the flags as required
	exporterCmd.MarkFlagRequired("db-name")
	exporterCmd.MarkFlagRequired("json")
}
