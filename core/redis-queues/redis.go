package RedisQueues

import (
	"fmt"
	"github.com/adjust/rmq/v4"
	"github.com/s8sg/goflow/runtime"
)

type RedisQueue struct {
	taskQueue rmq.Queue
}

func (r *RedisQueue) PublishBytes(data []byte) error {
	return r.taskQueue.PublishBytes(data)
}

var _ runtime.TaskQueue = &RedisQueue{}

func OpenTaskQueue(queueId, redisUrl string) (runtime.TaskQueue, error) {
	connection, err := rmq.OpenConnection("goflow", "tcp", redisUrl, 0, nil)
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

