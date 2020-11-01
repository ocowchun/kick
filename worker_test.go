package kick

import (
	"errors"
	"fmt"
	"testing"
)

func TestWorkerRun(t *testing.T) {
	workerDone := make(chan *JobResult)
	worker := NewWorker(workerDone)
	perform := func(arguments interface{}) error {
		return nil
	}
	job := &Job{}

	go worker.Run(perform, job)

	result := <-workerDone
	if result.err != nil {
		t.Errorf("Expect result.err be nil but get %s", result.err)
	}
	if result.completed != true {
		t.Errorf("Expect result.completed = true but get false")
	}
}

func TestWorkerRunWithError(t *testing.T) {
	workerDone := make(chan *JobResult)
	worker := NewWorker(workerDone)
	err := errors.New("testing error")
	perform := func(arguments interface{}) error {
		return err
	}
	job := &Job{}

	go worker.Run(perform, job)

	result := <-workerDone
	if result.err == nil {
		t.Errorf("result.err should not be nil")
	}
	if result.completed != false {
		t.Errorf("Expected completed = false but get true")
	}
}

func TestWorkerRunWithPanic(t *testing.T) {
	workerDone := make(chan *JobResult)
	worker := NewWorker(workerDone)
	err := errors.New("testing error")
	perform := func(arguments interface{}) error {
		a := arguments.(int)
		fmt.Println(a)
		return err
	}
	job := &Job{}

	go worker.Run(perform, job)

	result := <-workerDone
	if result.err == nil {
		t.Errorf("result.err should not be nil")
	}
	if result.completed != false {
		t.Errorf("Expected completed = false but get true")
	}
}
