package main

import (
	"bufio"
	"chat-app/internal/models"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Enter username (user@domain): ")
	username, _ := reader.ReadString('\n')
	username = strings.TrimSpace(username)

	fmt.Print("Enter password: ")
	password, _ := reader.ReadString('\n')
	password = strings.TrimSpace(password)

	// Аутентификация (упрощенная)
	user := authenticate(username, password)
	if user == nil {
		log.Fatalf("Authentication failed for user: %s", username)
	}

	// Подключение к RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Ошибка подключения: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Ошибка создания канала: %v", err)
	}
	defer ch.Close()

	// Создание персональной очереди для пользователя
	queueName := "user_" + user.Username
	q, err := ch.QueueDeclare(
		queueName,
		false, // durable
		true,  // autoDelete (удаляется при отключении)
		false, // exclusive
		false, // noWait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("Ошибка создания очереди: %v", err)
	}

	// Привязка к exchange с правильным routing key
	routingKey := "user." + user.Username
	if err := ch.QueueBind(
		q.Name,
		routingKey,
		"chat_direct",
		false,
		nil,
	); err != nil {
		log.Fatalf("Ошибка привязки: %v", err)
	}

	// Получение сообщений
	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer
		true,  // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("Ошибка потребления: %v", err)
	}

	// Горутина для приема сообщений
	go func() {
		for msg := range msgs {
			var chatMsg models.ChatMessage
			if err := json.Unmarshal(msg.Body, &chatMsg); err != nil {
				log.Printf("Ошибка parsing сообщения: %v", err)
				continue
			}
			fmt.Printf("\n[%s]: %s\n> ", chatMsg.From, chatMsg.Message)
		}
	}()

	fmt.Printf("Добро пожаловать, %s! Для справки введите !/help\n", user.Username)

	// Основной цикл ввода
	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if shouldExit := handleCommand(ch, user.Username, input); shouldExit {
			break
		}
	}
}

func authenticate(username, password string) *User {
	// Упрощенная аутентификация - всегда успешна
	// В реальном приложении здесь была бы проверка базы данных
	return &User{
		Username: username,
		Password: password,
	}
}

func handleCommand(ch *amqp.Channel, username, input string) bool {
	if strings.HasPrefix(input, "!/") {
		parts := strings.SplitN(input[2:], " ", 3)
		cmd := parts[0]

		switch cmd {
		case "exit", "wq", "end":
			fmt.Println("Выход из чата...")
			return true

		case "help":
			showHelp()

		case "chat", "send", "messto":
			if len(parts) < 3 {
				fmt.Println("Использование: !/chat <username> <сообщение>")
				return false
			}
			sendMessage(ch, username, parts[1], parts[2])

		case "broadcast":
			if len(parts) < 2 {
				fmt.Println("Использование: !/broadcast <сообщение>")
				return false
			}
			broadcastMessage(ch, username, parts[1])

		default:
			fmt.Println("Неизвестная команда. Введите !/help для справки")
		}
	} else if input != "" {
		fmt.Println("Используйте !/chat <user> <message> для отправки сообщений")
	}
	return false
}

func showHelp() {
	fmt.Println(`
Доступные команды:
!/help          - Показать справку
!/chat <user> <message> - Отправить сообщение пользователю
!/send <user> <message> - Отправить сообщение пользователю  
!/messto <user> <message> - Отправить сообщение пользователю
!/broadcast <message>   - Отправить сообщение всем
!/exit          - Выйти из чата
!/wq            - Выйти из чата
!/end           - Выйти из чата
`)
}

func sendMessage(ch *amqp.Channel, from, to, message string) {
	chatMsg := models.ChatMessage{
		From:    from,
		To:      to,
		Message: message,
		Type:    "direct",
	}

	body, err := json.Marshal(chatMsg)
	if err != nil {
		log.Printf("Ошибка marshaling сообщения: %v", err)
		return
	}

	// Правильный routing key для сервера
	routingKey := "user." + to

	err = ch.Publish(
		"chat_direct", // exchange
		routingKey,    // routing key (user.username)
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)

	if err != nil {
		log.Printf("Ошибка отправки сообщения: %v", err)
	} else {
		fmt.Printf("Сообщение отправлено пользователю %s\n", to)
	}
}

func broadcastMessage(ch *amqp.Channel, from, message string) {
	chatMsg := models.ChatMessage{
		From:    from,
		To:      "all",
		Message: message,
		Type:    "broadcast",
	}

	body, err := json.Marshal(chatMsg)
	if err != nil {
		log.Printf("Ошибка marshaling сообщения: %v", err)
		return
	}

	err = ch.Publish(
		"chat_direct", // exchange
		"broadcast",   // routing key для broadcast
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)

	if err != nil {
		log.Printf("Ошибка отправки broadcast: %v", err)
	} else {
		fmt.Println("Broadcast сообщение отправлено")
	}
}

type User struct {
	Username string
	Password string
}
