package main

import (
	config "chat-app/internal/config"
	models "chat-app/internal/models"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// run функция, которую можно тестировать
func Run(ctx context.Context, stderr io.Writer, // Добавляем контекст первым параметром
	broker models.MessageBroker, configLoader models.ConfigLoader,
	healthServer models.HealthServer) error {

	// Проверка обязательных зависимостей
	if broker == nil || configLoader == nil || healthServer == nil {
		return fmt.Errorf("required dependencies are not provided")
	}

	log.SetOutput(stderr)

	// Используем переданный контекст вместо создания нового
	ctx, cancel := context.WithCancel(ctx) // Теперь на основе внешнего контекста
	defer cancel()

	cfg, err := configLoader.LoadConfig("config.yaml")
	if err != nil {
		return fmt.Errorf("error loading config file: %v", err)
	}
	log.Printf("Configuration loaded: %+v\n", cfg)

	// Подключение к RabbitMQ через интерфейс
	ch, err := broker.Dial(cfg.GetRabbitMQURL())
	if err != nil {
		return fmt.Errorf("RabbitMQ connection error: %v", err)
	}
	defer ch.Close()

	// Объявление exchange
	if err := ch.ExchangeDeclare(
		cfg.GetExchangeName(),
		"direct",
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,   // arguments
	); err != nil {
		return fmt.Errorf("exchange declaration error: %v", err)
	}

	// Запуск health check сервера
	if err := healthServer.StartHealthServer(ctx, cfg.GetServerPort()); err != nil {
		return fmt.Errorf("health server start error: %v", err)
	}
	defer healthServer.StopHealthServer()

	// Обработка сигналов завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Слушаем очередь для обработки сообщений
	messages, err := ch.Consume(
		"chat_messages", // queue
		"broadcast",     // consumer
		true,            // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	if err != nil {
		return fmt.Errorf("consume error: %v", err)
	}

	log.Println("Server started. Waiting for messages...")

	// Канал для отслеживания завершения обработчиков сообщений
	var wg sync.WaitGroup
	messageDone := make(chan struct{})

	// Запускаем обработчик сообщений в отдельной горутине
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(messageDone)

		for {
			select {
			case msg, ok := <-messages:
				if !ok {
					log.Println("Message channel closed")
					return
				}
				// Парсим сообщение перед передачей в обработчик
				var chatMsg models.ChatMessage
				if err := json.Unmarshal(msg.Body, &chatMsg); err != nil {
					log.Printf("Error parsing message: %v", err)
					continue
				}
				userQueues, err := GetActiveUserQueues(cfg)
				if err != nil {
					log.Printf("Error getting active user queues: %v", err)
				}

				wg.Add(1)
				go func(m amqp.Delivery) {
					defer wg.Done()
					HandleBroadcastMessage(ch, m, chatMsg, userQueues)
				}(msg)

			case <-ctx.Done(): // Слушаем внешний контекст
				log.Println("Stopping message processing")
				return
			}
		}
	}()

	// Ждем сигнала завершения или ошибки
	select {
	case sig := <-sigChan:
		log.Printf("Received signal: %v. Shutting down...", sig)
		cancel() // Отменяем контекст
	case <-messageDone:
		log.Println("Message processing stopped unexpectedly")
	case <-ctx.Done(): // Также слушаем внешний контекст
		log.Println("Context cancelled. Shutting down...")
	}

	// Ждем завершения всех обработчиков сообщений с таймаутом
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All messages processed")
	case <-time.After(30 * time.Second):
		log.Println("Timeout waiting for message processing")
	}

	return nil
}

func GetActiveUserQueues(cfg config.Config) ([]models.Queue, error) {
	// Используем API RabbitMQ Management для получения списка очередей
	managerURL := "http://" + cfg.GetRabbitMQURL() + ":15672/api/queues"

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", managerURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	req.SetBasicAuth(cfg.GetRabbitUser(), cfg.GetRabbitPass())

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get queues from RabbitMQ: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("RabbitMQ API returned status: %d", resp.StatusCode)
	}

	var queues []models.Queue
	if err := json.NewDecoder(resp.Body).Decode(&queues); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	// Фильтруем только очереди, связанные с чатом
	var chatQueues []models.Queue
	for _, queue := range queues {
		if isChatQueue(queue.Name) {
			chatQueues = append(chatQueues, queue)
		}
	}

	return chatQueues, nil
}

func isChatQueue(queueName string) bool {
	// Определяем, является ли очередь чатовой
	// Например, очереди, начинающиеся с "user_" или "chat_"
	return len(queueName) > 0 // Здесь можно добавить более сложную логику
}
