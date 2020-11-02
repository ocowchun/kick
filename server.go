package kick

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis"
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
	fetcher          *Fetcher
	poller           *Poller
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
		fetcher:          NewFetcher(queue),
		poller:           NewPoller(queue),
	}
}

func (s *Server) handleWorkerDone(jobResult *JobResult) {
	maxRetryCount := 3
	s.queue.RemoveJobFromInprogress(jobResult.job)

	if jobResult.completed {
		fmt.Printf("Complete job %s %s\n", jobResult.job.JobDefinitionName, jobResult.job.ID)
	} else {
		job := jobResult.job

		fmt.Printf("Failed job %s %s\n", job.JobDefinitionName, job.ID)
		if job.Retry && job.RetryCount < maxRetryCount {
			job.RetryCount++
			retry, d := s.jobDefinitionMap[job.JobDefinitionName].RetryAt(job.RetryCount)
			if retry {
				job.EnqueuedAt = time.Now().Add(d)
				s.queue.EnqueueJob(job.EnqueuedAt, job)
			}
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
	workerReady := make(chan bool)
	s.fetcher.Run(jobs, workerReady)
	s.poller.Run()

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)

	for _, worker := range s.workers {
		go worker.Run(jobs, workerReady)
	}

	for {
		select {
		case jobResult := <-s.workerDone:
			s.handleWorkerDone(jobResult)
		case <-term:
			fmt.Println("Gracefully shutting kick server")
			go s.fetcher.Close()
			go s.poller.Close()
			s.fetcher.Wait()
			s.poller.Wait()

			close(s.workerDone)
			for _, w := range s.workers {
				w.Close()
			}
			return nil
		}
	}
}
