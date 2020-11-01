package kick

import "github.com/google/uuid"

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
