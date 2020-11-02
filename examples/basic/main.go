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
	jobDefinitions := []*kick.JobDefinition{fibJobDefinition, sleepJobDefinition}
	redisURL := "localhost:63790"
	c := kick.NewClient(redisURL, jobDefinitions)

	c.Enqueue("Fib", 13)
	c.Enqueue("Fib", 14)
	c.Enqueue("Fib", 15)
	c.Enqueue("Sleep", 3)
	c.Enqueue("Sleep", 3)
	c.EnqueueAt((3 * time.Second), "Fib", 20)

	config := &kick.ServerConfiguration{
		WorkerCount: 2,
		RedisURL:    redisURL,
	}
	s := kick.NewServer(config, jobDefinitions)
	s.Run()
}
