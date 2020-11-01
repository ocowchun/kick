package main

import (
	"fmt"
	"time"

	"github.com/ocowchun/kick"
)

func doFib(num int) int {
	if num <= 1 {
		return 1
	} else {
		return doFib(num-1) + doFib(num-2)
	}
}

func fibPerform(arguments interface{}) error {
	num := int(arguments.(float64))
	result := doFib(num)
	fmt.Printf("Complete job %d  result: %d\n", num, result)
	return nil
}

func sleepPerform(arguments interface{}) error {
	num := int(arguments.(float64))
	seconds := time.Duration(num) * time.Second
	time.Sleep(seconds)
	return nil
}

func main() {
	fibJobDefinition := kick.NewJobDefinition("Fib", fibPerform, nil)
	sleepJobDefinition := kick.NewJobDefinition("Sleep", sleepPerform, nil)
	config := &kick.ServerConfiguration{
		WorkerCount: 2,
		RedisURL:    "localhost:63790",
	}
	s := kick.NewServer(config, []*kick.JobDefinition{fibJobDefinition, sleepJobDefinition})

	s.Enqueue("Fib", 13)
	s.Enqueue("Fib", 14)
	s.Enqueue("Fib", 15)

	s.Run()
}
