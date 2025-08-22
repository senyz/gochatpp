package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// run функция, которую можно тестировать
func run(args []string, stdout io.Writer, stderr io.Writer,
	broker MessageBroker, configLoader ConfigLoader,
	healthServer HealthServer) error {
	// проверки на nil
	if broker == nil {
		return fmt.Errorf("broker is nil")
	}
	if configLoader == nil {
		return fmt.Errorf("configLoader is nil")
	}
	if healthServer == nil {
		return fmt.Errorf("healthServer is nil")
	}

	log.SetOutput(stderr)

	// Создаем контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
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
	if err := healthServer.StartHealthServer(cfg.GetServerPort()); err != nil {
		return fmt.Errorf("health server start error: %v", err)
	}
	defer healthServer.StopHealthServer()

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

				wg.Add(1)
				go func(m amqp.Delivery) {
					defer wg.Done()
					HandleMessage(ch, m)
				}(msg)

			case <-ctx.Done():
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
