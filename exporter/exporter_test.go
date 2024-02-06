package exporter

import (
	"encoding/json"
	"os"
	"testing"
)

// Assuming ExportToJSON, timestampToYearWeek, and other necessary functions are correctly implemented
func TestExportToJSON(t *testing.T) {
	// Temporary output file for testing
	outputPath := "../test_output.json"

	// Assuming you have a setup to either mock the database or use an existing test database
	dbPath := "../crypto.sqlite" // Adjust the path as necessary

	// Execute the ExportToJSON function with the test database and output path
	err := ExportToJSON(dbPath, outputPath)
	if err != nil {
		t.Fatalf("ExportToJSON failed: %v", err)
	}

	// Ensure the output file is removed after the test
	defer os.Remove(outputPath)

	// Read and verify the output file content
	file, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	// Unmarshal the JSON output into the expected structure
	var output []CryptoOutput // Use a slice if expecting multiple symbols in output
	err = json.Unmarshal(file, &output)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	// Perform specific checks on the output
	// For example, verify if the output contains expected symbols, and the "category" and "mode" are constant
	if len(output) == 0 {
		t.Errorf("Expected output to contain data, but got empty")
	} else {
		for _, crypto := range output {
			if crypto.Category != "crypto" {
				t.Errorf("Expected category to be 'crypto', got '%s'", crypto.Category)
			}
			if crypto.Mode != "year.week" {
				t.Errorf("Expected mode to be 'year.week', got '%s'", crypto.Mode)
			}
			if len(crypto.Prices) == 0 {
				t.Errorf("Expected 'prices' to have entries for symbol %s", crypto.Code)
			} else {
				// New checks for the Prices entries
				for _, price := range crypto.Prices {
					if price.YearWeek == "" {
						t.Errorf("Expected 'YearWeek' to be non-empty for symbol %s", crypto.Code)
					}
					if price.Value <= 0 {
						t.Errorf("Expected 'Value' to be greater than zero for symbol %s", crypto.Code)
					}
				}
			}
		}
	}
}
