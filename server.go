package kick

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type JobDefinition interface {
	Name() string
	Perform(arguments interface{}) error
	RetryAt(retryCount int) (bool, time.Duration)
}

type JobResult struct {
	worker    *Worker
	job       *Job
	completed bool
	err       error
}

type Worker struct {
	ID         uuid.UUID
	workerDone chan *JobResult
}

func NewWorker(workerDone chan *JobResult) *Worker {
	return &Worker{
		ID:         uuid.New(),
		workerDone: workerDone,
	}
}

func (w *Worker) Run(perform func(arguments interface{}) error, job *Job) {
	result := &JobResult{
		worker: w,
		job:    job,
	}

	result.err = perform(job.Arguments)
	result.completed = result.err == nil
	w.workerDone <- result
}

type Server struct {
	jobs             []Job
	isRunning        bool
	idleWorkers      []*Worker
	workerDone       chan *JobResult
	jobDefinitionMap map[string]JobDefinition
}

func NewServer(workerCount int, jobDefinitions []JobDefinition) *Server {
	i := 0
	idleWorkers := []*Worker{}
	workerDone := make(chan *JobResult)
	for i < workerCount {
		idleWorkers = append(idleWorkers, NewWorker(workerDone))
		i++
	}

	jobDefinitionMap := map[string]JobDefinition{}
	for _, jobDefinition := range jobDefinitions {
		jobDefinitionMap[jobDefinition.Name()] = jobDefinition
	}

	return &Server{
		isRunning:        false,
		idleWorkers:      idleWorkers,
		workerDone:       workerDone,
		jobDefinitionMap: jobDefinitionMap,
	}
}

type Job struct {
	ID                uuid.UUID   `json:"id"`
	Retry             bool        `json:"retry"`
	RetryCount        int         `json:"retryCount"`
	CreatedAt         time.Time   `json:"createdAt"`
	EnqueuedAt        time.Time   `json:"enqueuedAt"`
	Arguments         interface{} `json:"arguments"`
	JobDefinitionName string      `json:"jobDefinitionName"`
}

func (s *Server) Enqueue(jobDefinitionName string, arguments interface{}) error {
	jobDefinition := s.jobDefinitionMap[jobDefinitionName]
	if jobDefinition == nil {
		return fmt.Errorf("Can't find job definition name `%s`", jobDefinitionName)
	}

	now := time.Now()
	retry, _ := jobDefinition.RetryAt(0)
	job := Job{
		ID:                uuid.New(),
		Retry:             retry,
		CreatedAt:         now,
		EnqueuedAt:        now,
		JobDefinitionName: jobDefinitionName,
		Arguments:         arguments,
	}

	s.jobs = append(s.jobs, job)
	return nil

}

func (s *Server) consumeJob() error {
	for len(s.idleWorkers) > 0 && len(s.jobs) > 0 {
		worker, idleWorkers := s.idleWorkers[0], s.idleWorkers[1:]
		s.idleWorkers = idleWorkers
		job, jobs := s.jobs[0], s.jobs[1:]
		s.jobs = jobs

		jobDefinition := s.jobDefinitionMap[job.JobDefinitionName]
		if jobDefinition == nil {
			fmt.Printf("Can't find job definition %s", jobDefinition)
		} else {
			perform := jobDefinition.Perform
			go worker.Run(perform, &job)
		}
	}
	return nil
}

// Run kick server
func (s *Server) Run() error {
	if s.isRunning {
		return errors.New("Already running")
	}
	s.isRunning = true

	shouldStop := false
	for !shouldStop {
		select {
		case jobResult := <-s.workerDone:
			s.idleWorkers = append(s.idleWorkers, jobResult.worker)
			if jobResult.completed {
				fmt.Printf("Complete job %s\n", jobResult.job.ID)
			} else {
				fmt.Printf("Failed job %s\n", jobResult.job.ID)
			}
		case <-time.After(1 * time.Second):
			s.consumeJob()
			fmt.Println("timeout 1")
		}
	}
	return nil
}
