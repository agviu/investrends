package exporter

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3" // Import the SQLite driver anonymously to enable database/sql to use it without directly interacting with it.
)

// PriceEntry represents a single price entry with its associated week and value.
type PriceEntry struct {
	YearWeek string  `json:"year.week"` // The week of the year in "YYYY.WW" format.
	Value    float64 `json:"value"`     // The price value.
}

// CryptoOutput aggregates all prices for a single cryptocurrency symbol.
type CryptoOutput struct {
	Code     string       `json:"code"`     // The cryptocurrency symbol.
	Prices   []PriceEntry `json:"prices"`   // A list of price entries.
	Category string       `json:"category"` // The category of the data, e.g., "crypto".
	Mode     string       `json:"mode"`     // The mode of aggregation, e.g., "year.week".
}

// timestampToYearWeek converts a timestamp string to a "year.week" format.
func timestampToYearWeek(ts string) (string, error) {
	t, err := time.Parse("2006-01-02", ts) // Parse the timestamp.
	if err != nil {
		return "", err // Return an error if parsing fails.
	}
	_, week := t.ISOWeek()                             // Get the ISO week number.
	return fmt.Sprintf("%d.%02d", t.Year(), week), nil // Return formatted "year.week" string.
}

// fetchData queries the database for price data and organizes it into a map of CryptoOutput structs.
func fetchData(db *sql.DB) (map[string]*CryptoOutput, error) {
	query := "SELECT symbol, timestamp, value FROM crypto_prices" // SQL query to fetch data.
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying database: %w", err)
	}
	defer rows.Close()

	results := make(map[string]*CryptoOutput) // Map to hold the results, keyed by symbol.

	for rows.Next() {
		var symbol, timestamp string
		var value float64
		if err := rows.Scan(&symbol, &timestamp, &value); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		yearWeek, err := timestampToYearWeek(timestamp) // Convert timestamp to "year.week".
		if err != nil {
			return nil, fmt.Errorf("error converting timestamp: %w", err)
		}

		// Initialize a new CryptoOutput for the symbol if it doesn't already exist.
		if _, exists := results[symbol]; !exists {
			results[symbol] = &CryptoOutput{
				Code:     symbol,
				Prices:   []PriceEntry{},
				Category: "crypto",
				Mode:     "year.week",
			}
		}

		// Append the new price entry to the symbol's prices.
		results[symbol].Prices = append(results[symbol].Prices, PriceEntry{YearWeek: yearWeek, Value: value})
	}

	return results, nil // Return the organized data.
}

// writeJSON takes the organized data and writes it to a JSON file specified by filePath.
func writeJSON(data map[string]*CryptoOutput, filePath string) error {
	// Open or create the file for writing, truncating it if it already exists.
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error opening JSON file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "    ") // Set indentation for pretty JSON formatting.

	// Convert the map to a slice for a more natural JSON array format.
	var outputs []CryptoOutput
	for _, output := range data {
		outputs = append(outputs, *output)
	}

	// Encode the data as JSON and write it to the file.
	if err := encoder.Encode(outputs); err != nil {
		return fmt.Errorf("error encoding data to JSON: %w", err)
	}

	return nil // Return nil on success.
}

// ExportToJSON orchestrates the data export process: fetching from the database and writing to JSON.
func ExportToJSON(dbPath, outputPath string) error {
	db, err := sql.Open("sqlite3", dbPath) // Open the SQLite database.
	if err != nil {
		return fmt.Errorf("error opening database: %w", err)
	}
	defer db.Close() // Ensure the database is closed when done.

	data, err := fetchData(db) // Fetch data from the database.
	if err != nil {
		return err // Return early if there's an error.
	}

	// Write the fetched data to the specified JSON file.
	if err := writeJSON(data, outputPath); err != nil {
		return err // Return early if there's an error.
	}

	fmt.Println("Data exported successfully to", outputPath) // Indicate success.
	return nil                                               // Return nil on success.
}
