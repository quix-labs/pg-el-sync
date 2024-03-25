package main

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
)

type Record struct {
	Reference string
	Data      map[string]any
}

func main() {
	for record := range getRecords() {
		record.Reference = "301"
		sendResponse(record)
	}

	return
}

func sendResponse(record *Record) {
	jsonResponse, err := json.Marshal(record)
	if err != nil {
		os.Exit(32)
	}
	_, err = os.Stdout.WriteString(string(jsonResponse) + "\n")
	if err != nil {
		os.Exit(32)
	}
}

func getRecords() <-chan *Record {
	lines := make(chan *Record)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(os.Stdin)
		for scan.Scan() {
			record := &Record{}
			rows := strings.Split(scan.Text(), "\n")
			for _, row := range rows {
				err := json.Unmarshal([]byte(row), record)
				if err != nil {
					os.Exit(5)
				}
				lines <- record
			}

		}
	}()
	return lines
}

func getRecordsAsString() <-chan string {
	lines := make(chan string)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(os.Stdin)
		for scan.Scan() {
			lines <- scan.Text()

		}
	}()
	return lines
}
