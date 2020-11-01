package kick

import (
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID                uuid.UUID   `json:"id"`
	Retry             bool        `json:"retry"`
	RetryCount        int         `json:"retryCount"`
	CreatedAt         time.Time   `json:"createdAt"`
	EnqueuedAt        time.Time   `json:"enqueuedAt"`
	Arguments         interface{} `json:"arguments"`
	JobDefinitionName string      `json:"jobDefinitionName"`
	QueueName         string      `json:"queueName"`
}
