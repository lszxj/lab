package main

import "fmt"

func main() {
	a := []int{1, 2, 3}
	b := make([]int, 0)
	copy(b, a[0:2])
	fmt.Println(b)
}
