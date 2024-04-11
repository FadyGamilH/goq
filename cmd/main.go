package main

import "log"

func main() {
	sum := int64(0)
	var i int64
	for i = 0; i <= 200; i++ {
		sum += i
	}
	log.Println(sum)
}
