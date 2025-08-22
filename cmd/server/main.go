package main

import (
	config "chat-app/internal/config"
	models "chat-app/internal/models"
	"chat-app/internal/rabbitmq"
	"encoding/json"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	config := config.LoadConfig("config")
	log.Printf("Загрузка конфигурации: %v\n", config)

	// Подключение к RabbitMQ
	conn, err := amqp.Dial(config.RabbitMQURL)
	if err != nil {
		log.Fatalf("Ошибка подключения к RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Ошибка создания канала: %v", err)
	}
	defer ch.Close()

	// Объявление exchange
	if err := ch.ExchangeDeclare(
		config.ExchangeName,
		"direct",
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,   // arguments
	); err != nil {
		log.Fatalf("Ошибка объявления exchange: %v", err)
	}

	// Обработка сигналов завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Получен сигнал завершения. Закрытие соединения...")
		conn.Close()
	}()

	log.Println("Сервер запущен. Ожидание сообщений...")

	// Слушаем общую очередь для обработки сообщений
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
		log.Fatal(err)
	}
	for msg := range messages {
		go handleMessage(ch, msg)
	}
}

func handleMessage(ch rabbitmq.Channel, msg amqp.Delivery) {
	var chatMsg models.ChatMessage
	json.Unmarshal(msg.Body, &chatMsg)

	// Отправляем сообщение конкретному пользователю
	err := ch.Publish(
		"chat_direct",      // exchange
		"user."+chatMsg.To, // routing key
		false,              // mandatory
		false,              // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        msg.Body,
		})

	if err != nil {
		log.Printf("Failed to deliver message to %s: %v", chatMsg.To, err)
	}
}

func health() {
	// Создаем слушатель TCP-соединений
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Ошибка при создании сервера: %v", err)
	}
	defer listener.Close() // Гарантированное закрытие слушателя

	log.Println("Сервер запущен на :8080")

	// Бесконечный цикл для обработки соединений
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Ошибка принятия соединения: %v", err)
			continue
		}

		// Обработка каждого соединения в отдельной горутине
		go func(c net.Conn) {
			defer c.Close()

			buffer := make([]byte, 1024)
			n, err := c.Read(buffer)
			if err != nil {
				log.Printf("Ошибка чтения данных: %v", err)
				return
			}

			log.Printf("Получено сообщение: %s", buffer[:n])
			_, err = c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"))
			if err != nil {
				log.Printf("Ошибка отправки ответа: %v", err)
			}
		}(conn)
	}
}
