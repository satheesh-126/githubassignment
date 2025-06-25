package main

import (
	"fmt"
	"time"
)

// worker function to compute square of a number and send it to the channel
func squareWorker(num int, results chan<- int) {
	time.Sleep(100 * time.Millisecond) // simulate some work
	results <- num * num
}

func main() {
	numbers := []int{2, 4, 6, 8, 10}
	results := make(chan int, len(numbers)) // buffered channel

	for _, num := range numbers {
		go squareWorker(num, results)
	}

	// Collect results
	for i := 0; i < len(numbers); i++ {
		square := <-results
		fmt.Println(square)
	}

	close(results)
}
