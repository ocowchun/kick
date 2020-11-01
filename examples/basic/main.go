package main

import (
	"fmt"
	"time"

	"github.com/ocowchun/kick"
)

type Fib struct {
}

func doFib(num int) int {
	if num <= 1 {
		return 1
	} else {
		return doFib(num-1) + doFib(num-2)
	}
}

func (f *Fib) Name() string {
	return "Fib"
}

func (f *Fib) Perform(arguments interface{}) error {
	num := arguments.(int)
	result := doFib(num)
	fmt.Printf("Complete job %d  result: %d\n", num, result)
	return nil
}

func (f *Fib) RetryAt(retryCount int) (bool, time.Duration) {
	return false, time.Duration(0)
}

func main() {
	job := &Fib{}
	s := kick.NewServer(2, []kick.JobDefinition{job})
	s.Enqueue(job.Name(), 15)
	s.Run()
}
