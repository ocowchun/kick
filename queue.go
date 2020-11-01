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

func (f *Fetcher) Run(s *Server) {
	f.Add(1)
	go f.fetchJobs(s)
}

func (f *Fetcher) Close() {
	f.stopFetch <- true
}

func (f *Fetcher) fetchJobs(s *Server) {
	for {
		select {
		case <-f.stopFetch:
			fmt.Println("Gracefully shutting fetcher")
			f.Done()
			return
		case <-time.After(1 * time.Second):
			fmt.Println("fetchJobs")
			count := len(s.idleWorkers)
			i := 0
			for i < count {
				res := f.redisClient.BRPopLPush(f.queue.name, f.queue.InprogressSetName(), 1*time.Second)
				bytes, err := res.Bytes()
				if err == nil {
					var job Job
					err = json.Unmarshal(bytes, &job)
					if err != nil {
						fmt.Println("error:", err)
					} else {
						s.jobs = append(s.jobs, job)
					}
				}
				i++
			}

		}
	}
}

// Poller is responsible to move jobs from scheduledSet to normal list
type Poller struct {
	queue       *Queue
	redisClient *redis.Client
	stopPoll    chan bool
	*sync.WaitGroup
}

func NewPoller(queue *Queue) *Poller {
	return &Poller{
		queue,
		queue.redisClient,
		make(chan bool),
		&sync.WaitGroup{},
	}
}

func (p *Poller) Run() {
	p.Add(1)
	go p.poll()
}

func (p *Poller) Close() {
	p.stopPoll <- true
}

func (p *Poller) poll() {

	for {
		select {
		case <-p.stopPoll:
			fmt.Println("Gracefully shutting poller")
			p.Done()
			return
		case <-time.After(1 * time.Second):
			z := redis.ZRangeBy{
				Min:   "-inf",
				Max:   fmt.Sprintf("%f", float64(time.Now().UnixNano())),
				Count: 10,
			}
			vals, err := p.redisClient.ZRangeByScore(p.queue.ScheduledSetName(), z).Result()
			if err != nil {
				fmt.Println(err)
			} else {
				for _, val := range vals {
					fmt.Println("enqueue schedule job", val)
					// TODO: ensure the ZREM operation is success
					p.redisClient.ZRem(p.queue.ScheduledSetName(), val)
					var job Job
					json.Unmarshal([]byte(val), &job)
					p.queue.EnqueueJob(time.Now().Add(0*time.Second), &job)
				}
			}
		}
	}
}

type Queue struct {
	fetcher     *Fetcher
	poller      *Poller
	redisClient *redis.Client
	name        string
}

func NewQueue(name string, redisClient *redis.Client) *Queue {
	queue := &Queue{
		name:        name,
		redisClient: redisClient,
	}
	queue.fetcher = NewFetcher(queue)
	queue.poller = NewPoller(queue)
	return queue
}

func (q *Queue) InprogressSetName() string {
	return q.name + "::InprogressSet"
}

func (q *Queue) ScheduledSetName() string {
	return q.name + "::ScheduledSet"
}

func (q *Queue) Run(server *Server) {
	q.fetcher.Run(server)
	q.poller.Run()
}

func (q *Queue) Close() {
	go q.fetcher.Close()
	go q.poller.Close()
	q.fetcher.Wait()
	q.poller.Wait()
}

func (q *Queue) EnqueueJob(performAt time.Time, job *Job) error {
	bytes, err := json.Marshal(job)
	if err != nil {
		return err
	}

	if performAt.Sub(time.Now()) > (1 * time.Second) {
		z := redis.Z{
			Score:  float64(performAt.UnixNano()),
			Member: bytes,
		}
		q.redisClient.ZAdd(q.ScheduledSetName(), z)
	} else {
		q.redisClient.LPush(q.name, bytes)
	}

	return nil
}

func (q *Queue) RemoveJobFromInprogress(job *Job) {
	bytes, _ := json.Marshal(job)
	q.redisClient.LRem(q.InprogressSetName(), 1, bytes)
}
