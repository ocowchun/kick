package kick

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
)

type JobResult struct {
	worker    *Worker
	job       *Job
	completed bool
	err       error
}

type Server struct {
	jobs             []Job
	isRunning        bool
	workers          []*Worker
	workerDone       chan *JobResult
	jobDefinitionMap map[string]*JobDefinition
	redisClient      *redis.Client
	queue            *Queue
	workerReady      chan bool
}

type ServerConfiguration struct {
	WorkerCount int
	RedisURL    string
}

func NewServer(config *ServerConfiguration, jobDefinitions []*JobDefinition) *Server {
	jobDefinitionMap := map[string]*JobDefinition{}
	for _, jobDefinition := range jobDefinitions {
		jobDefinitionMap[jobDefinition.Name()] = jobDefinition
	}

	i := 0
	workers := []*Worker{}
	workerDone := make(chan *JobResult)
	for i < config.WorkerCount {
		workers = append(workers, NewWorker(workerDone, jobDefinitionMap))
		i++
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: config.RedisURL,
		DB:   0, // use default DB
	})

	queue := NewQueue("defaultQueue", redisClient)

	return &Server{
		isRunning:        false,
		workers:          workers,
		workerDone:       workerDone,
		jobDefinitionMap: jobDefinitionMap,
		redisClient:      redisClient,
		queue:            queue,
		workerReady:      make(chan bool),
	}
}

func (s *Server) Enqueue(jobDefinitionName string, arguments interface{}) error {
	return s.EnqueueAt(0*time.Second, jobDefinitionName, arguments)
}

func (s *Server) EnqueueAt(duration time.Duration, jobDefinitionName string, arguments interface{}) error {

	jobDefinition := s.jobDefinitionMap[jobDefinitionName]
	if jobDefinition == nil {
		return fmt.Errorf("Can't find job definition name `%s`", jobDefinitionName)
	}
	queueName := "defaultQueue"
	now := time.Now()
	retry, _ := jobDefinition.RetryAt(0)
	job := Job{
		ID:                uuid.New(),
		Retry:             retry,
		CreatedAt:         now,
		EnqueuedAt:        now,
		JobDefinitionName: jobDefinitionName,
		Arguments:         arguments,
		QueueName:         queueName,
	}
	return s.queue.EnqueueJob(now.Add(duration), &job)
}

func (s *Server) handleWorkerDone(jobResult *JobResult) {
	maxRetryCount := 3

	if jobResult.completed {
		fmt.Printf("Complete job %s %s\n", jobResult.job.JobDefinitionName, jobResult.job.ID)
		s.queue.RemoveJobFromInprogress(jobResult.job)
	} else {
		job := jobResult.job

		fmt.Printf("Failed job %s %s\n", jobResult.job.JobDefinitionName, job.ID)
		if job.Retry && job.RetryCount < maxRetryCount {
			s.queue.RemoveJobFromInprogress(jobResult.job)
		} else if job.RetryCount == maxRetryCount {
			fmt.Printf("Job %s %s failed to much time \n", jobResult.job.JobDefinitionName, job.ID)
		}
	}
}

// Run kick server
func (s *Server) Run() error {
	if s.isRunning {
		return errors.New("Already running")
	}

	fmt.Println("kick server starting...")

	s.isRunning = true

	jobs := make(chan *Job)
	s.queue.Run(s, jobs)
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)

	for _, worker := range s.workers {
		go worker.Run(jobs)
	}

	s.workerReady <- true
	for {
		select {
		case jobResult := <-s.workerDone:
			s.handleWorkerDone(jobResult)
			s.workerReady <- true
		case <-term:
			fmt.Println("Gracefully shutting kick server")
			s.queue.Close()
			close(s.workerDone)
			for _, w := range s.workers {
				w.Close()
			}
			return nil
		}
	}
}
