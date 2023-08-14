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

// These are possible values returned by the API.
const (
	allGood = iota
	limitReached
	missingDate
	missingSymbol
	jsonBroken
)

type CollectorInterface interface {
	ReadCurrencyList() ([][]string, error)
	setUpDb(sqlStmt string) (*sql.DB, error)
	GetStoreDataFunc() StoreDataFunc
	GetExtractDataFromValuesFunc() ExtractDataFromValuesFunc
	GetGetDataFunc() GetDataFunc
	GetURLFromSymbol(symbol string) string
	isProduction() bool
	getIndexPath() string
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

// Defines some function types
type ExtractDataFromValuesFunc func(cdr CryptoDataRaw, n int, symbol string) ([]CryptoDataCurated, int, error)
type StoreDataFunc func(db *sql.DB, data []CryptoDataCurated, tableName string) error
type GetDataFunc func(resource string) ([]byte, error)

// Collector struct defines fields for storing configuration options.
type Collector struct {
	DbFilePath           string
	ApiKey               string
	ApiKeyFilePath       string
	ApiUrl               string
	CurrencyListFilePath string
	production           bool
	indexPath            string
}

// Creates a new Collector struct.
func NewCollector(dbFilePath string, apiKeyFilePath string, apiUrl string, currencyListFilePath string, production bool, indexPath string) (Collector, error) {
	// Read the apiKey from the file where it is stored.
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
		production:           production,
		indexPath:            indexPath,
	}

	return c, nil
}

// wrapper around the real function, needed for tests.
func (c Collector) GetStoreDataFunc() StoreDataFunc {
	return StoreData
}

func (c Collector) getIndexPath() string {
	return c.indexPath
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

// Tries to get raw values from an API's response.
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
//   - If the daily limit is reached (100 requests per day), it sleeps or finish, depends on configuration.
func Run(c CollectorInterface, n int, clear bool) (int, error) {

	records, err := c.ReadCurrencyList()
	if err != nil {
		return 0, err
	}

	db, err := c.setUpDb("")
	if err != nil {
		return 0, DbError{Msg: "Error setting up the database"}
	}
	defer db.Close()
	if clear {
		log.Print("Clearing the blacklist table")
		db.Exec("DELETE FROM blacklist")
	}

	index, err := readIndexFromFile(c.getIndexPath())
	if err != nil {
		// If the file doesn't exist yet, start from the beginning.
		log.Printf("No index found, start from the beggining")
		index = 0
	}

	processed := 0
	for i := index; i < len(records); i++ {

		err = writeIndexToFile(i, c.getIndexPath())
		if err != nil {
			log.Println("Failed to write index to file: ", err)
			return processed, err
		}

		if i == 0 {
			// First row is a header, not useful
			continue
		}

		if processed > 0 && processed%n == 0 { // Pause every n requests to comply with rate limit
			log.Println("Sleeping a minute...")
			time.Sleep(time.Minute)
		}

		symbol := string(records[i][0])

		bl, _ := IsBlacklisted(db, symbol, "")
		if bl {
			log.Print("The symbol ", symbol, " is blacklisted. Skipping")
			continue
		}

		fmt.Println("Processing for ... ", symbol)
		processed++
		url := c.GetURLFromSymbol(symbol)
		response, err := c.GetGetDataFunc()(url)
		if err != nil {
			log.Printf("There was an error trying to get a response from %v", url)
			return processed, err
		}
		raw, status := GetRawValuesFromResponse(response)
		if status != allGood {
			switch status {
			case missingSymbol:
				// The data is unreadable, but the loop can continue.
				// Somehow the API returns Data error for certain symbols.
				log.Printf("Data from symbol %v was not valid", symbol)
				log.Printf("Blacklisting it...")
				AddToBlacklist(db, symbol, "")
			case limitReached:
				log.Printf("Reached the limit for today.")
				if c.isProduction() {
					log.Printf("We will continue in 24 hours")
					time.Sleep(24 * time.Hour)
				} else {
					log.Printf("Finishing...")
					return processed, nil
				}
			default:
				log.Printf("Failed to fetch data from API: %v", err)
			}
			continue
		}

		curatedData, extracted, err := c.GetExtractDataFromValuesFunc()(raw, 25, symbol)
		if err != nil {
			log.Print("Unable to extract data from raw response: ", err)
			continue
		}
		if extracted != 25 {
			log.Printf("For symbol %v, only %v values were extracted as it was incomplete", symbol, extracted)
		}

		err = c.GetStoreDataFunc()(db, curatedData, "crypto_prices")
		if err != nil {
			log.Print("unable to store data in the database: ", err)
			continue
		}

		fmt.Println(" DONE.")
	}

	// Once finished, restart the index.
	err = writeIndexToFile(0, c.getIndexPath())
	return processed, err
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

	if sqlStmt == "" {
		sqlStmt = `
		CREATE TABLE IF NOT EXISTS crypto_prices (
			id INTEGER PRIMARY KEY,
    		symbol TEXT,
    		timestamp TEXT,
    		value REAL,
    		UNIQUE(symbol, timestamp)
		);
		CREATE TABLE IF NOT EXISTS blacklist (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			symbol VARCHAR(255) UNIQUE NOT NULL
		);
		`
	}

	_, err = db.Exec(sqlStmt)
	if err != nil {
		return db, DbError{Msg: "Failed to create tables: " + err.Error()}
		// log.Fatalf("Failed to create table: %v", err)
	}

	return db, nil
}

// This function retrieve the useful data from the raw data.
func ExtractDataFromValues(cdr CryptoDataRaw, n int, symbol string) ([]CryptoDataCurated, int, error) {
	var curatedData []CryptoDataCurated

	// Retrieve which is the last value generated. It's stored
	// in the metadata section of cdr.
	lastRaw := cdr.MetaData.LastRefreshed

	date, _, ok := strings.Cut(lastRaw, " ")
	if !ok {
		return curatedData, 0, errors.New("unable to get last refreshed date from raw data")
	}
	const layout = "2006-01-02"
	t, err := time.Parse(layout, date)
	if err != nil {
		return curatedData, 0, errors.New("unable to convert date from string to time.Time")
	}

	// As it is weekly, we check from last sunday.
	// Substracts the number of days until last sunday to start from there.
	t = t.AddDate(0, 0, -int(t.Weekday()))

	i := 1
	missing := 0
	for i <= n {
		value, ok := cdr.TimeSeries[t.Format(layout)]
		if !ok {
			missing++
			i++
			continue
		}

		// Build the CryptoDataCurated struct
		var curatedValue CryptoDataCurated
		curatedValue.value, err = strconv.ParseFloat(value.Close, 64)
		if err != nil {
			return curatedData, n - missing, errors.New("unable to get the float value from the string")
		}
		curatedValue.date = t.Format(layout)
		curatedValue.symbol = symbol

		curatedData = append(curatedData, curatedValue)
		i++
		t = t.AddDate(0, 0, -7)
	}

	return curatedData, n - missing, nil
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

// Updates the index file
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

// Reads the value from the index
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

// Wrapper around getData, useful for Mocking in tests
func (c Collector) GetGetDataFunc() GetDataFunc {
	return getData
}

// Wrapper around getData, useful for Mocking in tests
func (c Collector) isProduction() bool {
	return c.production
}

func AddToBlacklist(db *sql.DB, symbol string, table string) error {
	if table == "" {
		table = "blacklist"
	}

	stmt, err := db.Prepare(fmt.Sprintf("INSERT OR REPLACE INTO %s(symbol) VALUES(?)", table))
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(symbol)
	return err
}

func IsBlacklisted(db *sql.DB, symbol string, table string) (bool, error) {
	if table == "" {
		table = "blacklist"
	}
	var count int
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE symbol = ?", table), symbol).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
