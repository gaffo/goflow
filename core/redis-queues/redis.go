package RedisQueues

import (
	"fmt"
	"github.com/adjust/rmq/v4"
	"github.com/s8sg/goflow/core/sdk"
	"time"
)

type RedisQueue struct {
	taskQueue rmq.Queue
}

func (r *RedisQueue) SetPushQueue(queue sdk.TaskQueue) {
	r.taskQueue.SetPushQueue(r.taskQueue)
}

// consumerBridge is a bridge type to the rmq type
type consumerBridge struct {
	consumer sdk.QueueConsumer
}

func (c *consumerBridge) Consume(delivery rmq.Delivery) {
	c.consumer.Consume(delivery)
}

func (r *RedisQueue) AddConsumer(queue string, consumer sdk.QueueConsumer) (string, error) {
	c := &consumerBridge{
		consumer: consumer,
	}
	return r.taskQueue.AddConsumer(queue, c)
}

func (r *RedisQueue) StartConsuming(prefetch int64, delay time.Duration) error {
	return r.taskQueue.StartConsuming(prefetch, delay)
}

func (r *RedisQueue) PublishBytes(data []byte) error {
	return r.taskQueue.PublishBytes(data)
}

var _ sdk.TaskQueue = &RedisQueue{}

type RedisQueueProvider struct {
	RedisUrl string
}

func (r *RedisQueueProvider) StopAllConsuming() (<-chan struct{}, error) {
	conn, err := r.getConnection()
	if err != nil {
		return nil, err
	}
	return conn.StopAllConsuming(), nil
}

var _ sdk.QueueProvider = & RedisQueueProvider{}

func (r *RedisQueueProvider) OpenTaskQueue(queueId string) (sdk.TaskQueue, error) {
	connection, err := r.getConnection()
	if err != nil {
		return nil, err
	}
	taskQueue, err := connection.OpenQueue(queueId)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue, error %v", err)
	}
	return &RedisQueue{
		taskQueue: taskQueue,
	}, nil
}

func (r *RedisQueueProvider) getConnection() (rmq.Connection, error) {
	connection, err := rmq.OpenConnection("goflow", "tcp", r.RedisUrl, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate connection, error %v", err)
	}
	return connection, nil
}

