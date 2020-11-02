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

func (f *Fetcher) Run(s *Server, jobs chan *Job) {
	f.Add(1)
	go f.fetchJobs(s, jobs)
}

func (f *Fetcher) Close() {
	f.stopFetch <- true
}

func (f *Fetcher) fetchJobs(s *Server, jobs chan *Job) {
	for {
		select {
		case <-f.stopFetch:
			fmt.Println("Gracefully shutting fetcher")
			f.Done()
			return
		case <-s.workerReady:
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
