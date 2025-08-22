package main

import (
	config "chat-app/internal/config"
	models "chat-app/internal/models"
	"chat-app/internal/rabbitmq"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Error loading config file: %v", err)
	}
	log.Printf("Configuration loaded: %+v\n", cfg)

	// Подключение к RabbitMQ
	conn, err := amqp.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		log.Fatalf("RabbitMQ connection error: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Channel creation error: %v", err)
	}
	defer ch.Close()

	// Объявление exchange
	if err := ch.ExchangeDeclare(
		cfg.Chat.Exchange,
		"direct",
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,   // arguments
	); err != nil {
		log.Fatalf("Exchange declaration error: %v", err)
	}

	// Запуск health check сервера в отдельной горутине
	healthServer := startHealthServer(cfg.App.ServerPort)
	defer healthServer.Close()

	// Обработка сигналов завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Слушаем очередь для обработки сообщений
	messages, err := ch.Consume(
		"chat_messages", // queue
		"chat_server",   // consumer
		true,            // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	if err != nil {
		log.Fatalf("Consume error: %v", err)
	}

	log.Println("Server started. Waiting for messages...")

	// Главный цикл обработки
	for {
		select {
		case msg := <-messages:
			go HandleMessage(ch, msg)
		case sig := <-sigChan:
			log.Printf("Received signal: %v. Shutting down...", sig)
			return
		}
	}
}

func HandleMessage(ch rabbitmq.Channel, msg amqp.Delivery) {
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

func startHealthServer(port int) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:         ":" + strconv.Itoa(port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	go func() {
		log.Printf("Health check server starting on port %d", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Health server error: %v", err)
		}
	}()

	return server
}
