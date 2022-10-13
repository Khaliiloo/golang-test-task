package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang-test-task/internal/logger"
	queueMessage "golang-test-task/internal/message"
)

type Client struct {
	URL        string
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Queues     map[string]amqp.Queue
}

func NewClient(url string) *Client {
	return &Client{
		URL:    url,
		Queues: make(map[string]amqp.Queue),
	}
}

func (rmq *Client) Connect() {
	var err error
	rmq.Connection, err = amqp.Dial(rmq.URL)
	logger.LogError(err, "Failed to connect to RabbitMQ on "+rmq.URL)

	rmq.Channel, err = rmq.Connection.Channel()
	logger.LogError(err, "Failed to open a channel")
}

func (rmq *Client) SetQueue(name string) {
	var err error
	rmq.Queues[name], err = rmq.Channel.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	logger.LogError(err, "Failed to declare a queue")
}

func (rmq *Client) Publish(ctx context.Context, message interface{}, queueName string) error {
	if _, ok := rmq.Queues[queueName]; !ok {
		logger.LogError(fmt.Errorf(""), fmt.Sprintf("Can't find queue %s", queueName))
		return fmt.Errorf("can't find queue %s", queueName)
	}

	jsonMessage, err := json.Marshal(message)
	logger.LogError(err, fmt.Sprintf("Failed to marshal %s to JSON", message))
	if err != nil {
		return fmt.Errorf("failed to marshal %s to JSON, %q", message, err)
	}
	err = rmq.Channel.PublishWithContext(ctx,
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/json",
			Body:        jsonMessage,
		})

	logger.LogError(err, "Failed to publish a message")
	if err != nil {
		return fmt.Errorf("failed to marshal %s to JSON, %q", message, err)
	}
	return nil
}

func (rmq *Client) Consume(ctx context.Context, queueName string, redisClient *redis.Client) {
	if _, ok := rmq.Queues[queueName]; !ok {
		logger.LogError(fmt.Errorf(""), fmt.Sprintf("Can't find queue %s", queueName))
		return
	}

	message, err := rmq.Channel.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	logger.LogError(err, "Failed to register a consumer")
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Left MessageProcessor with %v\n", ctx.Err())
			return
		case msg := <-message:
			var messg queueMessage.Message
			err := json.Unmarshal(msg.Body, &messg)
			logger.LogError(err, "Can't unmarshal message body")
			redisClient.LPush(messg.Sender+"->"+messg.Receiver, msg.Body)
		}
	}
}

func (rmq *Client) Dispose() {
	rmq.Channel.Close()
	rmq.Connection.Close()
}
