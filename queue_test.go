package kick

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
)

func createTestingRedisClient() *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:63790",
		DB:   0,
	})
	redisClient.Del("testingQueue")
	redisClient.Del("testingQueue::InprogressSet")
	redisClient.Del("testingQueue::ScheduledSet")
	return redisClient
}

func TestQueueEnqueueJob(t *testing.T) {
	redisClient := createTestingRedisClient()
	q := NewQueue("testingQueue", redisClient)
	job := &Job{
		ID: uuid.New(),
	}

	q.EnqueueJob(time.Now(), job)

	str, _ := redisClient.LPop(q.name).Result()
	var actualJob Job
	json.Unmarshal([]byte(str), &actualJob)
	if actualJob.ID != job.ID {
		t.Errorf("it should return inprogress %s but get %s", job.ID, actualJob.ID)
	}
}

func TestQueueEnqueueJobWithFutureTime(t *testing.T) {
	redisClient := createTestingRedisClient()
	q := NewQueue("testingQueue", redisClient)
	job := &Job{
		ID: uuid.New(),
	}
	performAt := time.Now().Add(10 * time.Minute)

	q.EnqueueJob(performAt, job)

	z := redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%f", float64(performAt.UnixNano())),
		Count: 10,
	}
	items, _ := redisClient.ZRangeByScore(q.ScheduledSetName(), z).Result()
	if len(items) != 1 {
		t.Errorf("scheduledSet size should equal 1 but get %d", len(items))
	}
	var actualJob Job
	json.Unmarshal([]byte(items[0]), &actualJob)
	if actualJob.ID != job.ID {
		t.Errorf("it should return inprogress %s but get %s", job.ID, actualJob.ID)
	}
}
func TestQueueRemoveJobFromInprogress(t *testing.T) {
	redisClient := createTestingRedisClient()
	q := NewQueue("testingQueue", redisClient)
	job1 := &Job{
		ID: uuid.New(),
	}
	job2 := &Job{
		ID: uuid.New(),
	}
	bytes1, _ := json.Marshal(job1)
	bytes2, _ := json.Marshal(job2)
	redisClient.LPush(q.InprogressSetName(), bytes1)
	redisClient.LPush(q.InprogressSetName(), bytes2)

	q.RemoveJobFromInprogress(job1)

	size, _ := redisClient.LLen(q.InprogressSetName()).Result()
	if size != 1 {
		t.Errorf("inprogress list size should equal 1 but get %d", size)
	}
	str, _ := redisClient.LPop(q.InprogressSetName()).Result()
	var actualJob Job
	json.Unmarshal([]byte(str), &actualJob)
	if actualJob.ID != job2.ID {
		t.Errorf("it should return inprogress %s but get %s", job2.ID, actualJob.ID)
	}
}
