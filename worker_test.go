package kick

import (
	"errors"
	"fmt"
	"testing"
)

func TestWorkerRun(t *testing.T) {
	workerDone := make(chan *JobResult)
	jobDefinitionMap := map[string]*JobDefinition{}
	perform := func(arguments interface{}) error {
		return nil
	}
	jobDefinitionMap["testing"] = NewJobDefinition("testing", perform, nil)
	worker := NewWorker(workerDone, jobDefinitionMap)
	job := &Job{
		JobDefinitionName: "testing",
	}
	ch := make(chan *Job)

	go worker.Run(ch)
	ch <- job

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
	jobDefinitionMap := map[string]*JobDefinition{}
	err := errors.New("testing error")
	perform := func(arguments interface{}) error {
		return err
	}
	jobDefinitionMap["testing"] = NewJobDefinition("testing", perform, nil)
	worker := NewWorker(workerDone, jobDefinitionMap)
	job := &Job{
		JobDefinitionName: "testing",
	}
	ch := make(chan *Job)

	go worker.Run(ch)
	ch <- job

	result := <-workerDone
	if result.err != err {
		t.Errorf("expect result.err = %s but get %s", err, result.err)
	}
	if result.completed != false {
		t.Errorf("Expected completed = false but get true")
	}
}

func TestWorkerRunWithPanic(t *testing.T) {
	workerDone := make(chan *JobResult)
	jobDefinitionMap := map[string]*JobDefinition{}
	err := errors.New("testing error")
	perform := func(arguments interface{}) error {
		a := arguments.(int)
		fmt.Println(a)
		return err
	}
	jobDefinitionMap["testing"] = NewJobDefinition("testing", perform, nil)
	worker := NewWorker(workerDone, jobDefinitionMap)
	job := &Job{
		JobDefinitionName: "testing",
	}
	ch := make(chan *Job)

	go worker.Run(ch)
	ch <- job

	result := <-workerDone
	if result.err == nil {
		t.Errorf("result.err should not be nil")
	}
	if result.completed != false {
		t.Errorf("Expected completed = false but get true")
	}
}
