package collector

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type CollectorInterface interface {
	GetRawValuesFromSymbolAPI(symbol string) (CryptoDataRaw, error)
	ReadCurrencyList() ([][]string, error)
	setUpDb(sqlStmt string) (*sql.DB, error)
	GetStoreDataFunc() StoreDataFunc
	GetExtractDataFromValuesFunc() ExtractDataFromValuesFunc
}

// The data as it comes from the API is stored here.
type CryptoDataRaw struct {
	MetaData struct {
		LastRefreshed string `json:"6. Last Refreshed"`
	} `json:"Meta Data"`
	TimeSeries map[string]struct {
		Close string `json:"4a. close (EUR)"`
	} `json:"Time Series (Digital Currency Weekly)"`
}

// The data that can be processed is stored here.
type CryptoDataCurated struct {
	symbol string
	date   string
	value  float64
}

type ExtractDataFromValuesFunc func(cdr CryptoDataRaw, n int, symbol string) ([]CryptoDataCurated, error)

type StoreDataFunc func(db *sql.DB, data []CryptoDataCurated, tableName string) error

// Configuration values for this program.
type Collector struct {
	DbFilePath           string
	ApiKey               string
	ApiKeyFilePath       string
	ApiUrl               string
	CurrencyListFilePath string
}

// Creates a new Collector struct.
func NewCollector(dbFilePath string, apiKeyFilePath string, apiUrl string,
	currencyListFilePath string) (Collector, error) {
	apiKey, err := getApiKey(apiKeyFilePath)
	if err != nil {
		var c Collector
		return c, err
	}
	c := Collector{
		DbFilePath:           dbFilePath,
		ApiKey:               apiKey,
		CurrencyListFilePath: currencyListFilePath,
		ApiUrl:               apiUrl,
		ApiKeyFilePath:       apiKeyFilePath,
	}

	return c, nil
}

// wrapper around the real function, needed for tests.
func (c Collector) GetStoreDataFunc() StoreDataFunc {
	return StoreData
}

// wrapper around the real function, needed for tests.
func (c Collector) GetExtractDataFromValuesFunc() ExtractDataFromValuesFunc {
	return ExtractDataFromValues
}

// Connects to the Alpha Vantage API and gets the latest values for a given symbol.
func (c Collector) GetRawValuesFromSymbolAPI(symbol string) (CryptoDataRaw, error) {
	var cryptoData CryptoDataRaw
	// Fetch data for each symbol
	url := fmt.Sprintf(c.ApiUrl, symbol, c.ApiKey)
	resp, err := http.Get(url)
	if err != nil {
		return cryptoData, ConnectionError{Msg: "Failed to fetch data from API:" + err.Error()}
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return cryptoData, ConnectionError{Msg: "Failed to read data from the response:" + err.Error()}
	}
	if strings.Contains(string(body), "Error") {
		return cryptoData, DataError{Msg: "Failed to read valid data when using symbol " + symbol + "Body was:" + string(body)}
	}

	err = json.Unmarshal(body, &cryptoData)
	if err != nil {
		return cryptoData, DataError{Msg: "Failed to parse API response: " + err.Error()}
	}

	return cryptoData, nil
}

// Main function that runs functionality and returns error if something went wrong.
// This function does the following:
//   - Sets up database (if not done before).
//   - Connects to API to retrieve data. It does it in a loop, 5 each time, and wait a minute
//     This is for respect the API limit (5 requests per minute max).
//   - Process the data, storing it in the database.
func Run(c CollectorInterface, n int) error {

	records, err := c.ReadCurrencyList()
	if err != nil {
		return err
	}

	db, err := c.setUpDb("")
	if err != nil {
		return DbError{Msg: "Error setting up the database"}
	}
	// return nil
	for i, record := range records {
		if i == 0 {
			// First row is a header, not useful
			continue
		}

		if i > 0 && i%n == 0 { // Pause every n requests to comply with rate limit
			time.Sleep(time.Minute)
		}

		symbol := string(record[0])

		fmt.Println("Processing for ... ", symbol)
		raw, err := c.GetRawValuesFromSymbolAPI(symbol)
		if err != nil {
			switch err.(type) {
			case DataError:
				// The data is unreadable, but the loop can continue.
				// Somehow the API returns Data error for certain symbols.
				log.Print("Data from symbol", symbol, "is erroneus")
			default:
				log.Fatalf("Failed to fetch data from API: %v", err)
				return err
			}
			continue
		}

		curatedData, err := c.GetExtractDataFromValuesFunc()(raw, 25, symbol)
		if err != nil {
			log.Print("Unable to extract data from raw response.")
			continue
		}

		err = c.GetStoreDataFunc()(db, curatedData, "crypto_prices")
		if err != nil {
			log.Print("unable to store data in the database")
			continue
		}

		fmt.Println(" DONE.")
	}

	return err
}

// Gets the API key, from a file in filePath
func getApiKey(filePath string) (string, error) {
	var apiKey string
	data, err := os.ReadFile(filePath)
	if err != nil {
		return apiKey, FileSystemError{Msg: "Error reading the apiKey file. Is it missing?"}
	}
	apiKey = string(data)
	t := len(apiKey)
	if t != 16 {
		return apiKey, DataError{Msg: "The apiKey does not have the proper format."}
	}

	return apiKey, nil
}

// Reads the list of currencies from a file in filePath.
func (c Collector) ReadCurrencyList() ([][]string, error) {
	var records [][]string

	// Read CSV file
	file, err := os.Open(c.CurrencyListFilePath)
	if err != nil {
		return records, FileSystemError{Msg: "Error while reading the currency list file"}
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	records, err = csvReader.ReadAll()
	if err != nil {
		return records, DataError{Msg: "Error while processing the currency list file"}
	}

	return records, nil
}

// Set's up database, creating the table if not done before.
func (c Collector) setUpDb(sqlStmt string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", c.DbFilePath)
	if err != nil {
		return db, FileSystemError{Msg: "Error reading the database file. Is it missing?"}
	}
	// @todo: move this defer, out of this function
	// defer db.Close()

	if sqlStmt == "" {
		sqlStmt = `
		CREATE TABLE IF NOT EXISTS crypto_prices (
			id INTEGER PRIMARY KEY,
    		symbol TEXT,
    		timestamp TEXT,
    		value REAL,
    		UNIQUE(symbol, timestamp)
		);
		`
	}

	_, err = db.Exec(sqlStmt)
	if err != nil {
		return db, DbError{Msg: "Failed to create table: " + err.Error()}
		// log.Fatalf("Failed to create table: %v", err)
	}

	return db, nil
}

// This function retrieve the useful data from the raw data.
func ExtractDataFromValues(cdr CryptoDataRaw, n int, symbol string) ([]CryptoDataCurated, error) {
	var curatedData []CryptoDataCurated

	// Retrieve which is the last value generated. It's stored
	// in the metadata section of cdr.
	lastRaw := cdr.MetaData.LastRefreshed

	date, _, ok := strings.Cut(lastRaw, " ")
	if !ok {
		return curatedData, errors.New("unable to get last refreshed date from raw data")
	}
	const layout = "2006-01-02"
	t, err := time.Parse(layout, date)
	if err != nil {
		return curatedData, errors.New("unable to convert date from string to time.Time")
	}

	// As it is weekly, we check from last sunday.
	// Substracts the number of days until last sunday to start from there.
	t = t.AddDate(0, 0, -int(t.Weekday()))

	i := 1
	for i <= n {
		value, ok := cdr.TimeSeries[t.Format(layout)]
		if !ok {
			return curatedData, errors.New("unable to get the value from the last refreshed date from TimeSeries raw data")
		}

		// Build the CryptoDataCurated struct
		var curatedValue CryptoDataCurated
		curatedValue.value, err = strconv.ParseFloat(value.Close, 64)
		if err != nil {
			return curatedData, errors.New("unable to get the float value from the string")
		}
		curatedValue.date = t.Format(layout)
		curatedValue.symbol = symbol

		curatedData = append(curatedData, curatedValue)
		i++
		t = t.AddDate(0, 0, -7)
	}

	return curatedData, nil
}

// Stores the data in the database.
func StoreData(db *sql.DB, data []CryptoDataCurated, tableName string) error {
	if tableName == "" {
		tableName = "crypto_prices"
	}

	// Store data in SQLite database
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}
	insertQuery := "INSERT OR IGNORE INTO " + tableName + "(symbol, timestamp, value) values(?, ?, ?)"
	stmt, err := tx.Prepare(insertQuery)
	if err != nil {
		log.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	for _, curated := range data {
		_, err = stmt.Exec(curated.symbol, curated.date, curated.value)
		if err != nil {
			log.Fatalf("Failed to insert data into table: %v", err)
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
		return err
	}

	return nil
}
