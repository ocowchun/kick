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
	idleWorkers      []*Worker
	workerDone       chan *JobResult
	jobDefinitionMap map[string]*JobDefinition
	redisClient      *redis.Client
	queue            *Queue
}

type ServerConfiguration struct {
	WorkerCount int
	RedisURL    string
}

func NewServer(config *ServerConfiguration, jobDefinitions []*JobDefinition) *Server {
	i := 0
	idleWorkers := []*Worker{}
	workerDone := make(chan *JobResult)
	for i < config.WorkerCount {
		idleWorkers = append(idleWorkers, NewWorker(workerDone))
		i++
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: config.RedisURL,
		DB:   0, // use default DB
	})

	jobDefinitionMap := map[string]*JobDefinition{}
	for _, jobDefinition := range jobDefinitions {
		jobDefinitionMap[jobDefinition.Name()] = jobDefinition
	}
	queue := NewQueue("defaultQueue", redisClient)

	return &Server{
		isRunning:        false,
		idleWorkers:      idleWorkers,
		workerDone:       workerDone,
		jobDefinitionMap: jobDefinitionMap,
		redisClient:      redisClient,
		queue:            queue,
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

func (s *Server) consumeJob() error {
	for len(s.idleWorkers) > 0 && len(s.jobs) > 0 {
		worker, idleWorkers := s.idleWorkers[0], s.idleWorkers[1:]
		s.idleWorkers = idleWorkers
		job, jobs := s.jobs[0], s.jobs[1:]
		s.jobs = jobs

		jobDefinition := s.jobDefinitionMap[job.JobDefinitionName]
		if jobDefinition == nil {
			fmt.Printf("Can't find job definition %s", job.JobDefinitionName)
		} else {
			perform := jobDefinition.Perform
			go worker.Run(perform, &job)
		}
	}
	return nil
}

func (s *Server) handleWorkerDone(jobResult *JobResult) {
	maxRetryCount := 3

	s.idleWorkers = append(s.idleWorkers, jobResult.worker)
	if jobResult.completed {
		fmt.Printf("Complete job %s\n", jobResult.job.ID)
		s.queue.RemoveJobFromInprogress(jobResult.job)
	} else {
		job := jobResult.job

		fmt.Printf("Failed job %s\n", job.ID)
		if job.Retry && job.RetryCount < maxRetryCount {
			s.queue.RemoveJobFromInprogress(jobResult.job)
		} else if job.RetryCount == maxRetryCount {
			fmt.Printf("Job %s failed to much time \n", job.ID)
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
	shouldStop := false
	s.queue.Run(s)
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)

	for !shouldStop {
		select {
		case jobResult := <-s.workerDone:
			s.handleWorkerDone(jobResult)
		case <-time.After(1 * time.Second):
			s.consumeJob()
			fmt.Println("timeout 1")
		case <-term:
			fmt.Println("Gracefully shutting kick server")
			s.queue.Close()
			return nil
		}
	}
	return nil
}
