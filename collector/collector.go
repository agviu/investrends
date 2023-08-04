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

const (
	allGood = iota
	limitReached
	missingDate
	missingSymbol
	jsonBroken
)

type CollectorInterface interface {
	// GetRawValuesFromSymbolAPI(symbol string) (CryptoDataRaw, error)
	ReadCurrencyList() ([][]string, error)
	setUpDb(sqlStmt string) (*sql.DB, error)
	GetStoreDataFunc() StoreDataFunc
	GetExtractDataFromValuesFunc() ExtractDataFromValuesFunc
	GetGetDataFunc() GetDataFunc
	GetURLFromSymbol(symbol string) string
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

type GetDataFunc func(resource string) ([]byte, error)

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

// Get data from a resource.
// In this case, it gets the data from a HTTP server.
func getData(resource string) ([]byte, error) {
	var response []byte
	resp, err := http.Get(resource)
	if err != nil {
		return response, ConnectionError{Msg: "Failed to fetch data from API:" + err.Error()}
	}

	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// Connects to the Alpha Vantage API and gets the latest values for a given symbol.
func GetRawValuesFromResponse(response []byte) (CryptoDataRaw, int) {
	var cryptoData CryptoDataRaw

	if strings.Contains(string(response), "Invalid API call.") {
		return cryptoData, missingSymbol
	}

	if strings.Contains(string(response), "You have reached the 100 requests/day limit") {
		return cryptoData, limitReached
	}

	err := json.Unmarshal(response, &cryptoData)
	if err != nil {
		return cryptoData, jsonBroken
	}

	return cryptoData, allGood
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

	index, err := readIndexFromFile("index.txt")
	if err != nil {
		// If the file doesn't exist yet, start from the beginning.
		index = 0
	}

	for i := index; i < len(records); i++ {

		err = writeIndexToFile(i, "index.txt")
		if err != nil {
			log.Print("Failed to write index to file: ", err)
			return err
		}

		if i == 0 {
			// First row is a header, not useful
			continue
		}

		if i > 0 && i%n == 0 { // Pause every n requests to comply with rate limit
			time.Sleep(time.Minute)
		}

		symbol := string(records[i][0])

		fmt.Println("Processing for ... ", symbol)
		url := c.GetURLFromSymbol(symbol)
		response, err := c.GetGetDataFunc()(url)
		raw, status := GetRawValuesFromResponse(response)
		if status != allGood {
			switch status {
			case missingSymbol:
				// The data is unreadable, but the loop can continue.
				// Somehow the API returns Data error for certain symbols.
				log.Printf("Data from symbol %v was not valid", symbol)
			case limitReached:
				log.Printf("Reached the limit for today.")
				return nil
			default:
				log.Printf("Failed to fetch data from API: %v", err)
			}
			continue
		}

		curatedData, err := c.GetExtractDataFromValuesFunc()(raw, 25, symbol)
		if err != nil {
			log.Print("Unable to extract data from raw response: ", err)
			continue
		}

		err = c.GetStoreDataFunc()(db, curatedData, "crypto_prices")
		if err != nil {
			log.Print("unable to store data in the database: ", err)
			continue
		}

		fmt.Println(" DONE.")
	}

	// Once finished, restart the index.
	err = writeIndexToFile(0, "index.txt")
	return err
}

// Returns the URL replacing the symbol in the placeholders.
func (c Collector) GetURLFromSymbol(symbol string) string {
	return fmt.Sprintf(c.ApiUrl, symbol, c.ApiKey)
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

func writeIndexToFile(i int, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(strconv.Itoa(i))
	if err != nil {
		return err
	}

	return nil
}

func readIndexFromFile(path string) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return 0, err
	}

	i, err := strconv.Atoi(string(bytes))
	if err != nil {
		return 0, err
	}

	return i, nil
}

func (c Collector) GetGetDataFunc() GetDataFunc {
	return getData
}
