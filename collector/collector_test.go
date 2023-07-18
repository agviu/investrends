package collector

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"testing"
)

// @todo: the keyApi needs to be read from the file, as it cannot
// be committed.
func TestGetRawValuesFromSymbolAPI(t *testing.T) {
	var symbols = []string{"BTC", "ADA", "AIR", "ETH", "SLR"}

	c, err := NewCollector("../crypto.sqlite", "../apikey.txt", "https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_WEEKLY&symbol=%s&market=EUR&apikey=%s", "../digital_currency_list.csv", ReadCurrencyList)
	if err != nil {
		t.Log("unable to create  collector")
		t.Fail()
	}

	for _, symbol := range symbols {
		t.Logf("Retrieving value for %v", symbol)
		_, err := c.GetRawValuesFromSymbolAPI(symbol)

		if err != nil {
			switch err.(type) {
			case DataError:
				// The data is unreadable, but the test can continue.
				t.Log("Data not readable for symbol", symbol+"Error was:"+err.Error())
			case ConnectionError:
				t.Fail()
			default:
				t.Fail()
			}
		} else {
			t.Log("Data was valid for symbol", symbol)
		}
	}
}

// Tests that the function for getting the API key works properly.
func TestGetApiKey(t *testing.T) {
	apiKeyFilePath := "../apikey.txt"

	var apiKey, err = getApiKey(apiKeyFilePath)
	if err != nil {
		t.Log("Api key could not be loaded", err)
		t.Fail()
	}
	if len(apiKey) != 16 {
		t.Log("API Key does not meet the format (16 lenght string)")
		t.Fail()
	}

	apiKeyFilePath = "apikey_non_existing.txt"

	_, err = getApiKey(apiKeyFilePath)
	if err == nil {
		t.Log("Api key should not be loaded", err)
		t.Fail()
	}
}

// Tests that the list of currencies can be properly loaded, and contain
// the expected amount of data.
func TestReadCurrencyList(t *testing.T) {
	_, err := ReadCurrencyList("non_existing_csv.csv")
	if err == nil {
		t.Log("Non error returned when non existing file")
		t.Fail()
	}

	records, err := ReadCurrencyList("../digital_currency_list.csv")
	if err != nil {
		t.Log(err.Error())
		t.Fail()
	}

	if len(records) == 0 {
		t.Fatal("The list should not be empty")
	} else {
		t.Log("Number of records found is", len(records))
		if len(records) != 576 {
			t.Log("The number of records has changed. You updated the file but not the test.")
		}
	}

	for i, row := range records {
		if len(row) != 2 {
			t.Log("The row", i, "does not have exactly 2 values")
			t.Fail()
		}
	}
}

// Tests that the database can be created.
func TestSetupDb(t *testing.T) {
	db_name := "../crypto.sqlite"

	sqlStmt := `
	THIS IS RUBBISH, DB SHOULD RETURN AN ERROR
	`
	_, err := setUpDb(db_name, sqlStmt)
	if err == nil {
		t.Fatal("Query was wrong and an error should have been received.")
	} else {
		t.Log("Database properly returned an error")
	}

	sqlStmt = `
	CREATE TABLE IF NOT EXISTS crypto_data_test (
		id INTEGER PRIMARY KEY,
    	symbol TEXT,
    	timestamp TEXT,
    	value REAL,
    	UNIQUE(symbol, timestamp)
	);
	`
	db, err := setUpDb(db_name, sqlStmt)
	if err != nil {
		t.Log("The create table statement returned an unexpected error")
		t.Fail()
	}
	defer db.Close()
	defer func() {
		t.Log("Deleting the table created for the test.")
		db.Exec("DROP TABLE IF EXISTS crypto_data_test")
	}()

	result, err := db.Exec(`INSERT INTO crypto_data_test (symbol, timestamp, value) VALUES (?, ?, ?)`, "A-SYMBOL", "THE-TIMESTAMP", "THE-VALUE")
	if err != nil {
		t.Log("There was an error trying to write data to the database.")
		t.FailNow()
	}

	n, err := result.RowsAffected()
	if err != nil {
		t.Log("There was an error reading the result returned from the database")
		t.Fail()
	}
	if n != 1 {
		t.Log("The table should contain exactly one item, the number was", n)
		t.Fail()
	}
}

// Tests getting valus from the JSON and store them in our CryptoDataCurated struct
func TestExtractDataFromValues(t *testing.T) {
	// Open the JSON file.
	jsonFile, err := os.Open("datatest/sample_response.json")
	if err != nil {
		log.Fatal(err)
	}
	defer jsonFile.Close()

	// Read the file into a byte slice.
	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		log.Fatal(err)
	}

	// Create a map to hold the JSON data.
	var result CryptoDataRaw

	// Unmarshal the byte slice into the map.
	err = json.Unmarshal(byteValue, &result)
	if err != nil {
		t.Log("unable to unmarshal data", err)
		t.Fail()
	}

	values, err := ExtractDataFromValues(result, 30, "BTC")
	if err != nil {
		t.Log("It was not possible to extract the data. Error:", err)
		t.Fail()
	}

	// Let's check some random values to see if they match the JSON.
	for _, value := range values {
		switch value.date {
		case "2023-06-04":
			if value.value != 24718.22543600 {
				t.Log("Wrong value for date", value.date)
				t.Fail()
			}
		case "2023-06-11":
			if value.value != 23633.73138000 {
				t.Log("Wrong value for date", value.date)
				t.Fail()
			}
		case "2023-06-18":
			if value.value != 24011.51665200 {
				t.Log("Wrong value for date", value.date)
				t.Fail()
			}
		case "2023-05-21":
			if value.value != 24383.27624800 {
				t.Log("Wrong value for date", value.date)
				t.Fail()
			}
		case "2023-05-14":
			if value.value != 24538.10239200 {
				t.Log("Wrong value for date", value.date)
				t.Fail()
			}
		case "2023-03-26":
			if value.value != 25495.67438000 {
				t.Log("Wrong value for date", value.date)
				t.Fail()
			}
		}

	}
}

func TestStoreData(t *testing.T) {
	db_name := "../crypto.sqlite"

	sqlStmt := `
	CREATE TABLE IF NOT EXISTS crypto_data_test (
		symbol TEXT NOT NULL,
		timestamp TEXT NOT NULL,
		value REAL NOT NULL
	);
	`

	db, err := setUpDb(db_name, sqlStmt)
	if err != nil {
		t.Log("The create table statement returned an unexpected error")
		t.Fail()
	}
	defer db.Close()
	defer func() {
		t.Log("Deleting the table created for the test.")
		db.Exec("DROP TABLE IF EXISTS crypto_data_test")
	}()

	data := []CryptoDataCurated{
		{
			symbol: "BTC",
			date:   "2023-03-08",
			value:  45000,
		},
		{
			symbol: "ETH",
			date:   "2023-03-09",
			value:  3000,
		},
		{
			symbol: "USDT",
			date:   "2023-03-10",
			value:  1.00,
		},
	}
	err = StoreData(db, data, "crypto_data_test")
	if err != nil {
		t.Log("It was not possible to store data:", err)
		t.Fail()
	}
}

func MockReadCurrencyList(filePath string) ([][]string, error) {
	return [][]string{
		{"currency code", "currency name"},
		{"BTC", ""},
		{"ADA", "ADA"},
		{"AIR", "AIR"},
		{"ETH", "Ethereum"},
		{"SLR", "Solarium"},
	}, nil
}

func TestRun(t *testing.T) {

	c, err := NewCollector("../crypto.sqlite", "../apikey.txt", "https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_WEEKLY&symbol=%s&market=EUR&apikey=%s", "../digital_currency_list.csv", MockReadCurrencyList)
	if err != nil {
		t.Log("unable to create  collector")
		t.Fail()
	}

	err = c.Run()
	if err != nil {
		t.Log("there was a problem running run", err.Error())
		t.Fail()
	}
}
