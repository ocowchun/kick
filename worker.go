package kick

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

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

	result.err = runPerform(perform, job.Arguments)
	result.completed = result.err == nil
	w.workerDone <- result
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
