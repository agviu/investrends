package main

import (
	"fmt"
	"log"

	"github.com/agviu/investrends/collector"
	_ "github.com/mattn/go-sqlite3"
)

// The data as it comes from the API is stored here.
// type CryptoDataRaw struct {
// 	MetaData struct {
// 		LastRefreshed string `json:"6. Last Refreshed"`
// 	} `json:"Meta Data"`
// 	TimeSeries map[string]struct {
// 		Close string `json:"4a. close (EUR)"`
// 	} `json:"Time Series (Digital Currency Weekly)"`
// }

// // The data that can be processed is stored here.
// type CryptoDataCurated struct {
// 	symbol string
// 	date   string
// 	value  float64
// }

// // Configuration values for this program.
// type InvestrendsConf struct {
// 	dbFilePath           string
// 	apiKeyFilePath       string
// 	apiUrl               string
// 	currencyListFilePath string
// }

// // Connects to the Alpha Vantage API and gets the latest values for a given symbol.
// func GetRawValuesFromSymbolAPI(symbol string, apiKey string) (CryptoDataRaw, error) {
// 	var cryptoData CryptoDataRaw
// 	// Fetch data for each symbol
// 	url := fmt.Sprintf("https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_WEEKLY&symbol=%s&market=EUR&apikey=%s", symbol, apiKey)
// 	resp, err := http.Get(url)
// 	if err != nil {
// 		return cryptoData, ConnectionError{Msg: "Failed to fetch data from API:" + err.Error()}
// 	}

// 	defer resp.Body.Close()
// 	body, err := io.ReadAll(resp.Body)
// 	if err != nil {
// 		return cryptoData, ConnectionError{Msg: "Failed to read data from the response:" + err.Error()}
// 	}
// 	if strings.Contains(string(body), "Error") {
// 		return cryptoData, DataError{Msg: "Failed to read valid data when using symbol " + symbol + "Body was:" + string(body)}
// 	}

// 	err = json.Unmarshal(body, &cryptoData)
// 	if err != nil {
// 		return cryptoData, DataError{Msg: "Failed to parse API response: " + err.Error()}
// 	}

// 	return cryptoData, nil
// }

// // Main function that runs functionality and returns error if something went wrong.
// // This function does the following:
// //   - Sets up database (if not done before).
// //   - Connects to API to retrieve data. It does it in a loop, 5 each time, and wait a minute
// //     This is for respect the API limit (5 requests per minute max).
// //   - Process the data, storing it in the database.
// func Run(c InvestrendsConf) error {

// 	var apiKey, err = getApiKey(c.apiKeyFilePath)
// 	if err != nil {
// 		return err
// 	}

// 	records, err := readCurrencyList(c.currencyListFilePath)
// 	if err != nil {
// 		return err
// 	}

// 	db, err := setUpDb(c.dbFilePath, "")
// 	if err != nil {
// 		return DbError{Msg: "Error setting up the database"}
// 	}

// 	for i, record := range records {
// 		if i == 0 {
// 			// First row is a header, not useful
// 			continue
// 		}

// 		if i > 0 && i%5 == 0 { // Pause every 5 requests to comply with rate limit
// 			time.Sleep(time.Minute)
// 		}

// 		symbol := string(record[0])

// 		raw, err := GetRawValuesFromSymbolAPI(symbol, apiKey)
// 		if err != nil {
// 			log.Fatalf("Failed to fetch data from API: %v", err)
// 			break
// 		}

// 		curatedData, err := ExtractDataFromValues(raw, 25, symbol)
// 		if err != nil {
// 			log.Fatal("Unable to extract data from raw response.")
// 			break
// 		}

// 		err = StoreData(db, curatedData, "crypto_data")
// 		if err != nil {
// 			log.Fatal("unable to store data in the database")
// 			break
// 		}
// 	}

// 	return err
// }

func main() {
	c, err := collector.NewCollector("./crypto.sqlite", "apikey.txt", "https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_WEEKLY&symbol=%s&market=EUR&apikey=%s", "digital_currency_list.csv", collector.ReadCurrencyList)
	if err != nil {
		log.Fatal("unable to create collector object")
		return
	}
	// c := collector.Collector{
	// 	DbFilePath:           "./crypto.sqlite",
	// 	ApiKeyFilePath:       "apikey.txt",
	// 	ApiUrl:               "https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_WEEKLY&symbol=%s&market=EUR&apikey=%s",
	// 	CurrencyListFilePath: "digital_currency_list.csv",
	// 	currencyList:         collector.ReadCurrencyList,
	// }

	err = c.Run(5)
	if err != nil {
		switch err.(type) {
		// case DataError:
		// @todo: Log stuff
		default:
			log.Fatal("There has been an error")
		}

		fmt.Println("Unfortunately there was an error running the program.")
		return
	}

	fmt.Println("Program ran succesfully.")
}

// // Gets the API key, from a file in filePath
// func getApiKey(filePath string) (string, error) {
// 	var apiKey string
// 	data, err := os.ReadFile(filePath)
// 	if err != nil {
// 		return apiKey, FileSystemError{Msg: "Error reading the apiKey file. Is it missing?"}
// 	}
// 	apiKey = string(data)
// 	t := len(apiKey)
// 	if t != 16 {
// 		return apiKey, DataError{Msg: "The apiKey does not have the proper format."}
// 	}

// 	return apiKey, nil
// }

// // Reads the list of currencies from a file in filePath.
// func readCurrencyList(filePath string) ([][]string, error) {
// 	var records [][]string

// 	// Read CSV file
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		return records, FileSystemError{Msg: "Error while reading the currency list file"}
// 	}
// 	defer file.Close()

// 	csvReader := csv.NewReader(file)
// 	records, err = csvReader.ReadAll()
// 	if err != nil {
// 		return records, DataError{Msg: "Error while processing the currency list file"}
// 	}

// 	return records, nil
// }

// // Set's up database, creating the table if not done before.
// func setUpDb(dbFilePath string, sqlStmt string) (*sql.DB, error) {
// 	db, err := sql.Open("sqlite3", dbFilePath)
// 	if err != nil {
// 		return db, FileSystemError{Msg: "Error reading the database file. Is it missing?"}
// 	}
// 	// @todo: move this defer, out of this function
// 	// defer db.Close()

// 	if sqlStmt == "" {
// 		sqlStmt = `
// 		CREATE TABLE IF NOT EXISTS crypto_data (
// 			id INTEGER PRIMARY KEY,
//     		symbol TEXT,
//     		timestamp TEXT,
//     		value REAL,
//     		UNIQUE(symbol, timestamp)
// 		);
// 		`
// 	}

// 	_, err = db.Exec(sqlStmt)
// 	if err != nil {
// 		return db, DbError{Msg: "Failed to create table: " + err.Error()}
// 		// log.Fatalf("Failed to create table: %v", err)
// 	}

// 	return db, nil
// }

// // This function retrieve the useful data from the raw data.
// func ExtractDataFromValues(cdr CryptoDataRaw, n int, symbol string) ([]CryptoDataCurated, error) {
// 	var curatedData []CryptoDataCurated

// 	// Retrieve which is the last value generated. It's stored
// 	// in the metadata section of cdr.
// 	lastRaw := cdr.MetaData.LastRefreshed

// 	date, _, ok := strings.Cut(lastRaw, " ")
// 	if !ok {
// 		return curatedData, errors.New("unable to get last refreshed date from raw data")
// 	}
// 	const layout = "2006-01-02"
// 	t, err := time.Parse(layout, date)
// 	if err != nil {
// 		return curatedData, errors.New("unable to convert date from string to time.Time")
// 	}

// 	i := 1
// 	for i <= n {
// 		value, ok := cdr.TimeSeries[t.Format(layout)]
// 		if !ok {
// 			return curatedData, errors.New("unable to get the value from the last refreshed date from TimeSeries raw data")
// 		}

// 		// Build the CryptoDataCurated struct
// 		var curatedValue CryptoDataCurated
// 		curatedValue.value, err = strconv.ParseFloat(value.Close, 64)
// 		if err != nil {
// 			return curatedData, errors.New("unable to get the float value from the string")
// 		}
// 		curatedValue.date = t.Format(layout)
// 		curatedValue.symbol = symbol

// 		curatedData = append(curatedData, curatedValue)
// 		i++
// 		t = t.AddDate(0, 0, -7)
// 	}

// 	return curatedData, nil
// }

// func StoreData(db *sql.DB, data []CryptoDataCurated, tableName string) error {
// 	if tableName == "" {
// 		tableName = "crypto_prices"
// 	}

// 	// Store data in SQLite database
// 	tx, err := db.Begin()
// 	if err != nil {
// 		log.Fatalf("Failed to begin transaction: %v", err)
// 	}
// 	insertQuery := "INSERT OR IGNORE INTO " + tableName + "(symbol, timestamp, value) values(?, ?, ?)"
// 	stmt, err := tx.Prepare(insertQuery)
// 	if err != nil {
// 		log.Fatalf("Failed to prepare statement: %v", err)
// 	}
// 	defer stmt.Close()

// 	for _, curated := range data {
// 		_, err = stmt.Exec(curated.symbol, curated.date, curated.value)
// 		if err != nil {
// 			log.Fatalf("Failed to insert data into table: %v", err)
// 			return err
// 		}
// 	}

// 	if err := tx.Commit(); err != nil {
// 		log.Fatalf("Failed to commit transaction: %v", err)
// 		return err
// 	}

// 	return nil
// }
