package main

// Default error struct, which other erros will reuse.
// type DefaultError struct {
// 	Msg string
// }

// func (e DefaultError) Error() string {
// 	return e.Msg
// }

// Error related to a problem connecting to the API, or reading the response.
type ConnectionError struct {
	Msg string
	// DefaultError
}

func (e ConnectionError) Error() string {
	return e.Msg
}

// Error related to the data received, like it's in wrong format or contains an error.
type DataError struct {
	// DefaultError
	Msg string
}

func (e DataError) Error() string {
	return e.Msg
}

// Error related to the file system, like not able to find a file or read from it.
type FileSystemError struct {
	Msg string
}

func (e FileSystemError) Error() string {
	return e.Msg
}

// Error related to the database.
type DbError struct {
	Msg string
}

func (e DbError) Error() string {
	return e.Msg
}
