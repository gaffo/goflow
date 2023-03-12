package RedisQueues

import (
	"fmt"
	"github.com/adjust/rmq/v4"
	"github.com/s8sg/goflow/core/sdk"
	"github.com/s8sg/goflow/runtime"
)

type RedisQueue struct {
	taskQueue rmq.Queue
}

func (r *RedisQueue) PublishBytes(data []byte) error {
	return r.taskQueue.PublishBytes(data)
}

var _ runtime.TaskQueue = &RedisQueue{}

type RedisQueueProvider struct {
	RedisUrl string
}

var _ sdk.QueueProvider = & RedisQueueProvider{}

func (r *RedisQueueProvider) OpenTaskQueue(queueId string) (runtime.TaskQueue, error) {
	connection, err := rmq.OpenConnection("goflow", "tcp", r.RedisUrl, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate connection, error %v", err)
	}
	taskQueue, err := connection.OpenQueue(queueId)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue, error %v", err)
	}
	return &RedisQueue{
		taskQueue: taskQueue,
	}, nil
}

