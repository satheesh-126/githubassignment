package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

// ProcessLogs reads multiple log files concurrently, extracts error lines,
// and writes them to a single output file.
func ProcessLogs(inputFiles []string, outputFile string) error {
	var wg sync.WaitGroup
	errorChan := make(chan string)

	// Writer goroutine
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)

		outFile, err := os.Create(outputFile)
		if err != nil {
			log.Printf("failed to create output file: %v", err)
			return
		}
		defer outFile.Close()

		writer := bufio.NewWriter(outFile)
		defer writer.Flush()

		for line := range errorChan {
			_, err := writer.WriteString(line + "\n")
			if err != nil {
				log.Printf("failed to write to output file: %v", err)
			}
		}
	}()

	// Spawn a goroutine for each input file
	for _, file := range inputFiles {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()

			f, err := os.Open(filename)
			if err != nil {
				log.Printf("failed to open file %s: %v", filename, err)
				return
			}
			defer f.Close()

			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				if strings.Contains(line, "ERROR") {
					errorChan <- line
				}
			}
			if err := scanner.Err(); err != nil {
				log.Printf("error reading file %s: %v", filename, err)
			}
		}(file)
	}

	// Wait for all reading goroutines to finish, then close the errorChan
	go func() {
		wg.Wait()
		close(errorChan)
	}()

	// Wait for writer to finish
	<-writerDone
	return nil
}

// Example usage
func main() {
	inputFiles := []string{"server1.log", "server2.log", "server3.log"}
	err := ProcessLogs(inputFiles, "errors.log")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Error logs extracted successfully.")
}
