package kick

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

// Fetcher is responsible to fetch jobs from redis
type Fetcher struct {
	queue       *Queue
	stopFetch   chan bool
	redisClient *redis.Client
	*sync.WaitGroup
}

func NewFetcher(queue *Queue) *Fetcher {
	return &Fetcher{
		queue,
		make(chan bool),
		queue.redisClient,
		&sync.WaitGroup{},
	}
}

func (f *Fetcher) Run(jobs chan *Job, workerReady chan bool) {
	f.Add(1)
	go f.fetchJobs(jobs, workerReady)
}

func (f *Fetcher) Close() {
	f.stopFetch <- true
}

func (f *Fetcher) fetchJobs(jobs chan *Job, workerReady chan bool) {
	for {
		select {
		case <-f.stopFetch:
			fmt.Println("Gracefully shutting fetcher")
			f.Done()
			return
		case <-workerReady:
			fmt.Println("Receive workerReady signal")
			res := f.redisClient.BRPopLPush(f.queue.name, f.queue.InprogressSetName(), 1*time.Second)
			bytes, err := res.Bytes()
			if err == nil {
				var job Job
				err = json.Unmarshal(bytes, &job)
				if err != nil {
					fmt.Println("error:", err)
				} else {
					jobs <- &job
				}
			}
		}
	}
}
