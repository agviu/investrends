package collector

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"os"
	"testing"
)

// The MockCollector is a wrapper around Collector
type MockCollector struct {
	Collector
}

// Return a new MockCollector object, for tests.
func NewMockCollector(dbFilePath string, apiKeyFilePath string, apiUrl string, currencyListFilePath string) (MockCollector, error) {
	apiKey, err := getApiKey(apiKeyFilePath)
	if err != nil {
		var mc MockCollector
		return mc, err
	}

	mc := MockCollector{
		Collector: Collector{
			DbFilePath:           dbFilePath,
			ApiKey:               apiKey,
			CurrencyListFilePath: currencyListFilePath,
			ApiUrl:               apiUrl,
			ApiKeyFilePath:       apiKeyFilePath,
		},
	}

	return mc, nil
}

// Init a collector with default values useful for our tests.
func initCollector() (Collector, error) {
	return NewCollector("../crypto.sqlite", "../apikey.txt", "https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_WEEKLY&symbol=%s&market=EUR&apikey=%s", "../digital_currency_list.csv", false)
}

// Tests that we can extract the raw values from a request, for several symbols.
func TestGetRawValuesFromSymbolAPI(t *testing.T) {
	// var symbols = []string{"BTC", "ADA", "AIR", "ETH", "SLR"}
	var symbols = []string{"LIMIT", "NO-SYMBOL", "ALL-GOOD"}
	var url string

	c, err := initCollector()
	if err != nil {
		t.Log("unable to create  collector")
		t.Fail()
	}

	var mc MockCollector
	mc.Collector = c

	for _, symbol := range symbols {
		t.Logf("Retrieving value for %v", symbol)
		if symbol == "LIMIT" {
			url = "datatest/limit_achieved_response.json"
		} else if symbol == "NO-SYMBOL" {
			url = "datatest/non_symbol_response.json"
		} else if symbol == "ALL-GOOD" {
			url = "datatest/sample_response.json"
		}

		response, err := mc.GetGetDataFunc()(url)
		if err != nil {
			t.Logf("Failed to open the resource for %v: %v", url, err.Error())
			t.Fail()
		}

		_, status := GetRawValuesFromResponse(response)

		switch status {
		case missingSymbol:
			if symbol != "NO-SYMBOL" {
				t.Logf("Received missing symbol without expecting.")
				t.Fail()
			}
		case limitReached:
			if symbol != "LIMIT" {
				t.Logf("Received limit reached without being expected")
				t.Fail()
			}
		case allGood:
			if symbol != "ALL-GOOD" {
				t.Logf("Received all-good without being expected")
				t.Fail()
			}
		default:
			t.Logf("We received a non expected status code: %v", status)
			t.Fail()
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
	c, err := initCollector()
	if err != nil {
		t.Log("error creating the collector")
		t.Fail()
	}

	c.CurrencyListFilePath = "non_existing_csv.csv"
	_, err = c.ReadCurrencyList()
	if err == nil {
		t.Log("Non error returned when non existing file")
		t.Fail()
	}

	c.CurrencyListFilePath = "../digital_currency_list.csv"
	records, err := c.ReadCurrencyList()
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
	c, err := initCollector()
	if err != nil {
		t.Log("error creating the collector")
		t.Fail()
	}

	sqlStmt := `
	THIS IS RUBBISH, DB SHOULD RETURN AN ERROR
	`
	_, err = c.setUpDb(sqlStmt)
	if err == nil {
		t.Fatal("Query was wrong and an error should have been received.")
	} else {
		t.Log("Database properly returned an error")
	}

	sqlStmt = `
	CREATE TABLE IF NOT EXISTS crypto_prices_test (
		id INTEGER PRIMARY KEY,
    	symbol TEXT,
    	timestamp TEXT,
    	value REAL,
    	UNIQUE(symbol, timestamp)
	);
	`
	db, err := c.setUpDb(sqlStmt)
	if err != nil {
		t.Log("The create table statement returned an unexpected error")
		t.Fail()
	}
	defer db.Close()
	defer func() {
		t.Log("Deleting the table created for the test.")
		db.Exec("DROP TABLE IF EXISTS crypto_prices_test")
	}()

	result, err := db.Exec(`INSERT INTO crypto_prices_test (symbol, timestamp, value) VALUES (?, ?, ?)`, "A-SYMBOL", "THE-TIMESTAMP", "THE-VALUE")
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
// The response here is complete, has all values, and the test should detect that too.
func TestExtractDataFromCompleteValues(t *testing.T) {
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

	values, _, err := ExtractDataFromValues(result, 30, "BTC")
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

// Test that we can extract data from and detect that it was incomplete (some dates in the past are missing)
func TestExtractDataFromIncompleteValues(t *testing.T) {
	// Open the JSON file.
	jsonFile, err := os.Open("datatest/non_complete_response.json")
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

	_, extracted, err := ExtractDataFromValues(result, 30, "BTC")
	if err != nil {
		t.Log("It was not possible to extract the data. Error:", err)
		t.Fail()
	}
	if extracted == 30 {
		t.Log("It should have extracted less than 30 items.")
		t.Fail()
	}
}

// Test that we can store data in the database.
func TestStoreData(t *testing.T) {
	c, err := initCollector()
	if err != nil {
		t.Log("unable to create  collector")
		t.Fail()
	}

	sqlStmt := `
	CREATE TABLE IF NOT EXISTS crypto_prices_test (
		symbol TEXT NOT NULL,
		timestamp TEXT NOT NULL,
		value REAL NOT NULL
	);
	`

	db, err := c.setUpDb(sqlStmt)
	if err != nil {
		t.Log("The create table statement returned an unexpected error")
		t.Fail()
	}
	defer db.Close()
	defer func() {
		t.Log("Deleting the table created for the test.")
		db.Exec("DROP TABLE IF EXISTS crypto_prices_test")
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
	err = StoreData(db, data, "crypto_prices_test")
	if err != nil {
		t.Log("It was not possible to store data:", err)
		t.Fail()
	}
}

// Mock of ReadCurrencyList, where we provide a very short list of currencies for the tests.
func (mc MockCollector) ReadCurrencyList() ([][]string, error) {
	return [][]string{
		{"currency code", "currency name"},
		{"BTC", ""},
		{"ADA", "ADA"},
		{"AIR", "AIR"},
		{"ETH", "Ethereum"},
		{"SLR", "Solarium"},
	}, nil
}

// Mock around setUpDb. We just return an empty pointer to a sql.DB, nothing else is needed.
func (mc MockCollector) setUpDb(sqlStmt string) (*sql.DB, error) {
	var db sql.DB
	return &db, nil
}

// Mock around GetRawValuesFromSymbolAPI. We just return a CryptoDataRaw as if it would have been
// retrieved from a request.
func (mc MockCollector) GetRawValuesFromSymbolAPI(symbol string) (CryptoDataRaw, error) {

	data := CryptoDataRaw{
		MetaData: struct {
			LastRefreshed string `json:"6. Last Refreshed"`
		}{
			LastRefreshed: "2023-07-05",
		},
		TimeSeries: map[string]struct {
			Close string `json:"4a. close (EUR)"`
		}{
			"2023-07-05": {
				Close: "32000.00",
			},
			"2023-06-28": {
				Close: "31000.00",
			},
			// Add more data points as necessary...
		},
	}

	return data, nil

}

// Mock for ExtractDataFromValues, return an empty slice of CryptoDataCurated
func MockExtractDataFromValues(cdr CryptoDataRaw, n int, symbol string) ([]CryptoDataCurated, int, error) {
	var cdc []CryptoDataCurated
	return cdc, n, nil
}

// Mock for GetExtractDataFromValuesFunc. We return the function to be used in our tests.
func (mc MockCollector) GetExtractDataFromValuesFunc() ExtractDataFromValuesFunc {
	return MockExtractDataFromValues
}

// Mock for StoreData. Return nil error, so everything went fine (it "stored" the data properly)
func MockStoreData(db *sql.DB, data []CryptoDataCurated, tableName string) error {
	return nil
}

// Mock for GetStoreDataFunc. We return fhe function to be used in our tests.
func (mc MockCollector) GetStoreDataFunc() StoreDataFunc {
	return MockStoreData
}

// Test for the main Run function.
// Using a Mock Collector, we run the Run function and test its result.
func TestRun(t *testing.T) {

	mc, err := NewMockCollector("../crypto.sqlite", "../apikey.txt", "https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_WEEKLY&symbol=%s&market=EUR&apikey=%s", "../digital_currency_list.csv")
	if err != nil {
		t.Log("unable to create collector")
		t.Fail()
	}

	err = Run(mc, 10)
	if err != nil {
		t.Log("there was a problem running Run", err.Error())
		t.Fail()
	}
}

// Mock around GetGetDataFunc. We return a function that reads from a JSON instead of http.Get.
func (mc MockCollector) GetGetDataFunc() GetDataFunc {
	return func(resource string) ([]byte, error) {
		var response []byte
		jsonFile, err := os.Open(resource)
		if err != nil {
			return response, err
		}
		defer jsonFile.Close()

		// Read the file into a byte slice.
		return io.ReadAll(jsonFile)
	}
}
