package exporter

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type PriceEntry struct {
	YearWeek string  `json:"year.week"`
	Value    float64 `json:"value"`
}

type CryptoOutput struct {
	Code     string       `json:"code"`
	Prices   []PriceEntry `json:"prices"`
	Category string       `json:"category"`
	Mode     string       `json:"mode"`
}

func timestampToYearWeek(ts string) (string, error) {
	t, err := time.Parse("2006-01-02", ts)
	if err != nil {
		return "", err
	}
	_, week := t.ISOWeek()
	return fmt.Sprintf("%d.%02d", t.Year(), week), nil
}

func fetchData(db *sql.DB) (map[string]*CryptoOutput, error) {
	query := "SELECT symbol, timestamp, value FROM crypto_prices"
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying database: %w", err)
	}
	defer rows.Close()

	results := make(map[string]*CryptoOutput)

	for rows.Next() {
		var symbol, timestamp string
		var value float64
		if err := rows.Scan(&symbol, &timestamp, &value); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		yearWeek, err := timestampToYearWeek(timestamp)
		if err != nil {
			return nil, fmt.Errorf("error converting timestamp: %w", err)
		}

		if _, exists := results[symbol]; !exists {
			results[symbol] = &CryptoOutput{
				Code:     symbol,
				Prices:   []PriceEntry{},
				Category: "crypto",
				Mode:     "year.week",
			}
		}

		results[symbol].Prices = append(results[symbol].Prices, PriceEntry{YearWeek: yearWeek, Value: value})
	}

	return results, nil
}

func writeJSON(data map[string]*CryptoOutput, filePath string) error {
	// Open the file, truncating if it already exists and creating if it doesn't
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error opening JSON file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "    ") // For pretty JSON formatting

	// Convert the map to a slice since JSON arrays are more common for this kind of data
	var outputs []CryptoOutput
	for _, output := range data {
		outputs = append(outputs, *output)
	}

	if err := encoder.Encode(outputs); err != nil {
		return fmt.Errorf("error encoding data to JSON: %w", err)
	}

	return nil
}

func ExportToJSON(dbPath, outputPath string) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("error opening database: %w", err)
	}
	defer db.Close()

	data, err := fetchData(db)
	if err != nil {
		return err
	}

	if err := writeJSON(data, outputPath); err != nil {
		return err
	}

	fmt.Println("Data exported successfully to", outputPath)
	return nil
}
