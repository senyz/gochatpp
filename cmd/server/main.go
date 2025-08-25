package main

import (
	"chat-app/internal/config"
	health "chat-app/internal/health"
	models "chat-app/internal/models"
	"context"

	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RealConfigLoader struct{}

func (r *RealConfigLoader) LoadConfig(configPath string) (cfg config.Config, err error) {
	cfg, err = config.LoadConfig(configPath)
	return cfg, err
}

// main.go
func main() {
	broker := &RealAMQPBroker{}
	configLoader := &RealConfigLoader{}
	healthServer := &health.RealHealthServer{}

	// Создаем контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := Run(ctx, os.Stderr, broker, configLoader, healthServer); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// func handleBroadcastMessage(ch Channel, msg amqp.Delivery, cfg config.Config) {

// handleBroadcastMessage рассылает broadcast всем активным пользователям
func HandleBroadcastMessage(ch models.Channel, msg amqp.Delivery, chatMsg models.ChatMessage, userQueues []models.Queue) {
	log.Printf("Processing broadcast from %s: %s", chatMsg.From, chatMsg.Message)

	// Валидация сообщения
	if chatMsg.From == "" || chatMsg.Message == "" || chatMsg.To == "" {
		log.Printf("Invalid message: empty from or message field")
		return
	}

	// Рассылаем сообщение ВСЕМ пользователям, включая отправителя
	successCount := 0
	for _, queue := range userQueues {
		err := ch.Publish(
			"",         // используем default exchange
			queue.Name, // отправляем напрямую в очередь пользователя
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        msg.Body, // оригинальное сообщение
				Headers: amqp.Table{
					"broadcast": true,
					"from":      chatMsg.From,
					"timestamp": time.Now().Format(time.RFC3339),
				},
			},
		)

		if err != nil {
			log.Printf("Failed to send to queue %s: %v", queue.Name, err)
		} else {
			successCount++
		}
	}

	log.Printf("Broadcast from %s delivered to %d users (including sender)",
		chatMsg.From, successCount)
}

// HandleMessage обрабатывает входящие сообщения из очереди chat_messages
func HandleMessage(ch models.Channel, delivery amqp.Delivery, cfg config.Config) {
	var chatMsg models.ChatMessage

	if err := json.Unmarshal(delivery.Body, &chatMsg); err != nil {
		log.Printf("Error parsing JSON: %v", err)
		return
	}
	chatQueue, err := GetActiveUserQueues(cfg)
	if err != nil {
		log.Printf("Error getting active user queues: %v", err)

	}
	// Валидация сообщения
	if chatMsg.From == "" || chatMsg.Message == "" {
		log.Printf("Invalid message: empty from or message field")
		return
	}

	// Определяем тип сообщения по routing key или полю Type
	if delivery.RoutingKey == "broadcast" || chatMsg.Type == "broadcast" {
		HandleBroadcastMessage(ch, delivery, chatMsg, chatQueue)
	} else {
		// Direct сообщения просто логируем (они уже доставлены напрямую)
		log.Printf("Direct message from %s to %s: %s",
			chatMsg.From, chatMsg.To, chatMsg.Message)
	}
}
