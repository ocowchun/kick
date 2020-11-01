package kick

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

// Fetcher is responsible to fetch jobs from redis
type Fetcher struct {
	stopFetch   chan bool
	shouldFetch chan bool
	redisClient *redis.Client
}

func NewFetcher(redisClient *redis.Client) *Fetcher {
	return &Fetcher{
		stopFetch:   make(chan bool),
		redisClient: redisClient,
	}
}

func (f *Fetcher) Run(s *Server) {
	go f.fetchJobs(s)
}

func (f *Fetcher) Close() {
	f.stopFetch <- true
}

func (f *Fetcher) fetchJobs(s *Server) {
	queueName := "defaultQueue"
	dest := queueName + "::inProgress"
	for {
		select {
		case <-f.stopFetch:
			fmt.Println("Gracefully shutting fetcher")
			return
		case <-time.After(1 * time.Second):
			fmt.Println("fetchJobs")
			count := len(s.idleWorkers)
			i := 0
			for i < count {
				res := f.redisClient.BRPopLPush(queueName, dest, 1*time.Second)
				fmt.Println("fetch job")
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
}

func NewPoller(queue *Queue) *Poller {
	return &Poller{
		queue:       queue,
		redisClient: queue.redisClient,
	}
}

func (p *Poller) Run() {
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
		fetcher:     NewFetcher(redisClient),
		redisClient: redisClient,
	}
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

// TODO: ensure all component gracefully shutdown
func (q *Queue) Close() {
	go q.fetcher.Close()
	go q.poller.Close()
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

func (q *Queue) removeJobFromInprogress(job *Job) {
	bytes, _ := json.Marshal(job)
	q.redisClient.LRem(q.InprogressSetName(), 1, bytes)
}
