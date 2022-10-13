package logger

import "log"

func LogError(err error, msg string) {
	if err != nil {
		log.Printf("[RabbitMQClient] %s: %s\n", msg, err)
	}
}
