package kick

import (
	"encoding/json"
	"time"

	"github.com/go-redis/redis"
)

type Queue struct {
	fetcher     *Fetcher
	poller      *Poller
	redisClient *redis.Client
	name        string
	jobReady    chan bool
}

func NewQueue(name string, redisClient *redis.Client) *Queue {
	queue := &Queue{
		name:        name,
		redisClient: redisClient,
		jobReady:    make(chan bool),
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
