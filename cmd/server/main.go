package main

import (
	"chat-app/internal/config"
	models "chat-app/internal/models"
	"encoding/json"
	"fmt"
	"log"
	"os"

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
	healthServer := &RealHealthServer{}

	if err := run(os.Stderr, broker, configLoader, healthServer); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func HandleMessage(ch Channel, msg amqp.Delivery) {
	var chatMsg models.ChatMessage
	if err := json.Unmarshal(msg.Body, &chatMsg); err != nil {
		log.Printf("Message parsing error: %v", err)
		return
	}

	if chatMsg.To == "" {
		log.Printf("Empty recipient in message from %s", chatMsg.From)
		return
	}

	err := ch.Publish(
		"chat_direct",
		"user."+chatMsg.To,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        msg.Body,
		})

	if err != nil {
		log.Printf("Failed to deliver message to %s: %v", chatMsg.To, err)
	}
}
