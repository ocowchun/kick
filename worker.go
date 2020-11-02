package kick

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

type Worker struct {
	ID               uuid.UUID
	workerDone       chan *JobResult
	jobDefinitionMap map[string]*JobDefinition
	shouldClose      chan bool
}

func NewWorker(workerDone chan *JobResult, jobDefinitionMap map[string]*JobDefinition) *Worker {
	return &Worker{
		ID:               uuid.New(),
		workerDone:       workerDone,
		jobDefinitionMap: jobDefinitionMap,
		shouldClose:      make(chan bool),
	}
}

func (w *Worker) Run(jobs chan *Job, workerReady chan bool) {
	fmt.Printf("Worker %s starts running...\n", w.ID)
	for {
		select {
		case <-w.shouldClose:
			fmt.Printf("Gracefully shutting worker %s\n", w.ID)
			return
		case workerReady <- true:

		case job := <-jobs:
			jobDefinition := w.jobDefinitionMap[job.JobDefinitionName]
			result := &JobResult{
				worker: w,
				job:    job,
			}
			if jobDefinition == nil {
				result.completed = false
				result.err = fmt.Errorf("Can't find job definition %s", job.JobDefinitionName)
				w.workerDone <- result
			} else {
				perform := jobDefinition.Perform

				result.err = runPerform(perform, job.Arguments)
				result.completed = result.err == nil
				w.workerDone <- result
			}
		}
	}
}

func (w *Worker) Close() {
	w.shouldClose <- true
}

func runPerform(perform func(arguments interface{}) error, arguments interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.New(fmt.Sprintln(e))
		}
	}()
	err = perform(arguments)
	return err
}
