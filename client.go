package kick

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
)

type Client struct {
	redisClient      *redis.Client
	jobDefinitionMap map[string]*JobDefinition
	queue            *Queue
}

func NewClient(redisURL string, jobDefinitions []*JobDefinition) *Client {
	jobDefinitionMap := map[string]*JobDefinition{}
	for _, jobDefinition := range jobDefinitions {
		jobDefinitionMap[jobDefinition.Name()] = jobDefinition
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisURL,
		DB:   0, // use default DB
	})
	queue := NewQueue("defaultQueue", redisClient)

	return &Client{
		redisClient:      redisClient,
		jobDefinitionMap: jobDefinitionMap,
		queue:            queue,
	}
}

func (c *Client) Enqueue(jobDefinitionName string, arguments interface{}) error {
	return c.EnqueueAt(0*time.Second, jobDefinitionName, arguments)
}

func (c *Client) EnqueueAt(duration time.Duration, jobDefinitionName string, arguments interface{}) error {

	jobDefinition := c.jobDefinitionMap[jobDefinitionName]
	if jobDefinition == nil {
		return fmt.Errorf("Can't find job definition name `%s`", jobDefinitionName)
	}

	now := time.Now()
	retry, _ := jobDefinition.RetryAt(0)
	job := Job{
		ID:                uuid.New(),
		Retry:             retry,
		CreatedAt:         now,
		EnqueuedAt:        now.Add(duration),
		JobDefinitionName: jobDefinitionName,
		Arguments:         arguments,
		QueueName:         c.queue.name,
	}
	return c.queue.EnqueueJob(now.Add(duration), &job)
}
