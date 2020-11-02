package kick

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

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
					// TODO: ensure the ZREM operation is success
					p.redisClient.ZRem(p.queue.ScheduledSetName(), val)
					var job Job
					json.Unmarshal([]byte(val), &job)
					fmt.Printf("enqueue schedule job %s %s \n", job.JobDefinitionName, job.ID)
					p.queue.EnqueueJob(time.Now().Add(0*time.Second), &job)

				}
			}
		}
	}
}
