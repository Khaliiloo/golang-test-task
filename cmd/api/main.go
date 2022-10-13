package main

import (
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
	"golang-test-task/internal/logger"
	messageQueue "golang-test-task/internal/message"
	"golang-test-task/internal/rabbitmq"
)

var RedisClient *redis.Client

func main() {
	// initialize RedisClient
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// create RabbitMQ client
	rabbitMQClient := rabbitmq.NewClient("amqp://user:password@rabbitmq:7001/")
	rabbitMQClient.Connect()
	rabbitMQClient.SetQueue("messageQueue")

	//
	r := gin.Default()
	v1 := r.Group("/v1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	v1.POST("/message", func(c *gin.Context) {
		var message messageQueue.Message
		if err := c.ShouldBind(&message); err != nil {
			logger.LogError(err, "")
			c.JSON(400, gin.H{"status": "Failed", "error": err.Error()})
			return
		}

		err := rabbitMQClient.Publish(ctx, message, "messageQueue")
		if err != nil {
			c.JSON(400, gin.H{"status": "Failed", "error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"status": "Success"})
	})

	v1.GET("/message/list", func(c *gin.Context) {
		var message messageQueue.Message
		if err := c.ShouldBind(&message); err != nil {
			logger.LogError(err, "")
		}
		jsonMessages, err := RedisClient.LRange(message.Sender+"->"+message.Receiver, 0, -1).Result()
		logger.LogError(err, "")
		if err != nil {
			c.JSON(400, gin.H{"status": "failed", "error": err})
			return
		}
		messages := make([]messageQueue.Message, len(jsonMessages))
		for index, msg := range jsonMessages {
			err := json.Unmarshal([]byte(msg), &message)
			if err != nil {
				c.JSON(400, gin.H{"status": "failed", "error": err})
				return
			}
			messages[index] = message

		}
		c.JSON(200, messages)

	})

	go rabbitMQClient.Consume(ctx, "messageQueue", RedisClient)
	r.Run()
}
