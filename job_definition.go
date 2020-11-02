package kick

import (
	"math"
	"time"
)

type JobDefinition struct {
	name    string
	perform func(arguments interface{}) error
	retryAt func(retryCount int) (bool, time.Duration)
}

func defaultRetryAt(retryCount int) (bool, time.Duration) {
	return true, time.Duration(math.Pow(2, float64(retryCount))) * time.Second
}

func NewJobDefinition(name string, perform func(arguments interface{}) error, retryAt func(retryCount int) (bool, time.Duration)) *JobDefinition {
	j := &JobDefinition{
		name:    name,
		perform: perform,
		retryAt: defaultRetryAt,
	}
	if retryAt != nil {
		j.retryAt = retryAt
	}

	return j
}

func (j *JobDefinition) Name() string {
	return j.name
}

func (j *JobDefinition) Perform(arguments interface{}) error {
	return j.perform(arguments)
}

func (j *JobDefinition) RetryAt(retryCount int) (bool, time.Duration) {
	return j.retryAt(retryCount)
}
