package main

import (
	"fmt"
	"sync"
)

func work(ready chan bool, stop chan bool, wg *sync.WaitGroup) {
	fmt.Println("work")
	wg.Add(1)
	for {
		select {
		case ready <- true:
			fmt.Println("send ready signal")
		case <-stop:
			fmt.Println("receive stop signal")
			wg.Done()
			return
		}
	}

}

func main() {
	var wg sync.WaitGroup
	ready := make(chan bool)
	stop := make(chan bool)
	go work(ready, stop, &wg)
	// <-ready
	stop <- true
	wg.Wait()

	// <-ready
	// stop <- true
	// sleep()
}
