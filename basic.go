package main

import (
	"fmt"
)

func hasPairWithSum(arr []int, target int) bool {
	left, right := 0, len(arr)-1

	for left < right {
		sum := arr[left] + arr[right]
		if sum == target {
			return true
		} else if sum < target {
			left++
		} else {
			right--
		}
	}

	return false
}

func main() {
	arr := []int{1, 2, 3, 4, 6, 8, 9}
	target := 10

	if hasPairWithSum(arr, target) {
		fmt.Println("Pair found!")
	} else {
		fmt.Println("No pair found.")
	}
}
