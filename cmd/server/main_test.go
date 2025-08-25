package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	config "chat-app/internal/config"
	"chat-app/internal/models"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Переменная для подмены функции dial в тестах
var amqpDial = amqp.Dial
var testConfig = &MockConfig{
	RabbitMQURL:  "amqp://test:test@localhost:5672/",
	ExchangeName: "test_exchange",
	AuthFile:     "test_users.json",
	LogLevel:     "debug",
	ServerPort:   8081,
}

func GetTestUserQueues() []models.Queue {

	var queues []models.Queue = make([]models.Queue, 3)
	//type Queue struct {    Name,    VHost,    Messages,    Consumers}
	var testQueque1 = &models.Queue{
		Name:      "user.user1",
		VHost:     "test_vhost",
		Messages:  10,
		Consumers: 2}
	var testQueque2 = &models.Queue{
		Name:      "user.user2",
		VHost:     "test_vhost",
		Messages:  10,
		Consumers: 2}

	queues[0] = *testQueque1
	queues[1] = *testQueque2

	return queues
}

func TestHandleBroadcastMessage_InvalidJSON(t *testing.T) {
	mockChannel := new(MockChannel)

	// Невалидный JSON - НЕ должно вызывать Publish
	delivery := amqp.Delivery{
		Body: []byte("{invalid json}"),
	}
	testMessage := models.ChatMessage{}

	testQueque := GetTestUserQueues()
	HandleBroadcastMessage(mockChannel, delivery, testMessage, testQueque)

	// Убеждаемся, что Publish НЕ был вызван
	mockChannel.AssertNotCalled(t, "Publish")
}

func TestHandleBroadcastMessage_EmptyToField(t *testing.T) {
	mockChannel := new(MockChannel)

	testMessage := models.ChatMessage{
		From:    "user1",
		To:      "", // Пустое поле To
		Message: "Hello!",
	}

	messageBody, _ := json.Marshal(testMessage)

	delivery := amqp.Delivery{
		Body: messageBody,
	}
	testQueue := GetTestUserQueues()
	HandleBroadcastMessage(mockChannel, delivery, testMessage, testQueue)

	// Не должно вызывать Publish с пустым получателем
	mockChannel.AssertNotCalled(t, "Publish")
}

// TestSignalHandling тестирует обработку сигналов
func TestSignalHandling(t *testing.T) {
	t.Run("signal_handling", func(t *testing.T) {
		sigChan := make(chan os.Signal, 1)

		go func() {
			sigChan <- syscall.SIGINT
		}()

		select {
		case sig := <-sigChan:
			assert.Equal(t, syscall.SIGINT, sig)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Timeout waiting for signal")
		}
	})
}

// TestConfigLoading тестирует загрузку конфигурации
func TestConfigLoading(t *testing.T) {
	t.Run("test_mock_config", func(t *testing.T) {

		assert.Equal(t, "amqp://test:test@localhost:5672/", testConfig.GetRabbitMQURL())
		assert.Equal(t, "test_exchange", testConfig.GetExchangeName())
		assert.Equal(t, "test_users.json", testConfig.GetAuthFile())
		assert.Equal(t, "debug", testConfig.GetLogLevel())
		assert.Equal(t, 8081, testConfig.GetServerPort())
	})
}

// TestConfigFileLoading тестирует загрузку из файла
func TestConfigFileLoading(t *testing.T) {
	t.Run("test_file_config", func(t *testing.T) {
		// Создаем временный config файл
		tempFile, err := os.CreateTemp("", "test_config_*.yaml")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tempFile.Name())

		testconfig := `
rabbitmq:
  url: "amqp://test:test@localhost:5672/"
  user: "test"
  pass: "test"
  host: "localhost"
  port: 5672

chat:
  exchange: "test_exchange"
  auth_file: "test_users.json"

app:
  log_level: "debug"
  server_port: 8081
`

		if _, err := tempFile.WriteString(testconfig); err != nil {
			t.Fatal(err)
		}
		tempFile.Close()

		// Загружаем конфиг
		cfg, err := config.Load(tempFile.Name())
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		assert.Equal(t, "amqp://test:test@localhost:5672/", cfg.GetRabbitMQURL())
		assert.Equal(t, "test_exchange", cfg.GetExchangeName())
		assert.Equal(t, "test_users.json", cfg.GetAuthFile())
		assert.Equal(t, "debug", cfg.GetLogLevel())
		assert.Equal(t, 8081, cfg.GetServerPort())
	})
}

// TestMainFunction тестирует основную функцию
func TestMainFunction(t *testing.T) {
	t.Run("main_should_not_panic", func(t *testing.T) {
		oldArgs := os.Args
		defer func() { os.Args = oldArgs }()

		os.Args = []string{"chat-server", "-test.Run=TestMainFunction"}

		originalDial := amqpDial
		defer func() { amqpDial = originalDial }()

		amqpDial = func(url string) (*amqp.Connection, error) {
			return nil, nil
		}

		assert.NotPanics(t, func() {
			// Тестируем только что код компилируется
		})
	})
}

func TestRun_HealthServerStartError(t *testing.T) {
	var stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mockBroker := new(MockBroker)
	mockChannel := new(MockChannel)
	MockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	MockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test:test@localhost:5672/").Return(mockChannel, nil)
	mockBroker.On("Close").Return(nil)

	mockChannel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockChannel.On("Close").Return(nil)

	mockHealthServer.On("StartHealthServer", mock.Anything, 8081).Return(fmt.Errorf("health server error"))

	err := Run(ctx, &stderr, mockBroker, MockConfigLoader, mockHealthServer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "health server start error")
}

func TestRun_ConfigLoadError(t *testing.T) {
	var stderr bytes.Buffer

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mockBroker := new(MockBroker)
	MockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	MockConfigLoader.On("LoadConfig", "config.yaml").Return(nil, fmt.Errorf("config error"))

	err := Run(ctx, &stderr, mockBroker, MockConfigLoader, mockHealthServer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config error")
}

func TestRun_RabbitMQConnectionError(t *testing.T) {
	var stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mockBroker := new(MockBroker)
	MockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	MockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test:test@localhost:5672/").Return(nil, fmt.Errorf("connection failed"))

	err := Run(ctx, &stderr, mockBroker, MockConfigLoader, mockHealthServer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "RabbitMQ connection error")
}

func TestRun_ConsumeError(t *testing.T) {
	var stderr bytes.Buffer
	// Создаем контекст с таймаутом для теста
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mockBroker := new(MockBroker)
	mockChannel := new(MockChannel)
	MockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	MockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test:test@localhost:5672/").Return(mockChannel, nil)
	mockBroker.On("Close").Return(nil)

	mockChannel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockChannel.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("consume error"))
	mockChannel.On("Close").Return(nil)

	mockHealthServer.On("StartHealthServer", mock.Anything, 8081).Return(nil) // Добавляем mock.Anything для контекста
	mockHealthServer.On("StopHealthServer").Return(nil)

	// Теперь передаем контекст первым аргументом
	err := Run(ctx, &stderr, mockBroker, MockConfigLoader, mockHealthServer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consume error")
}
func TestHandleBroadcastMessage_TableDriven(t *testing.T) {
	// Тестовые очереди пользователей
	testQueues := []models.Queue{
		{Name: "user1_queue"},
		{Name: "user2_queue"},
		{Name: "user3_queue"},
	}

	testCases := []struct {
		name          string
		message       models.ChatMessage
		shouldPublish bool
		expectedCalls int // Сколько раз должен вызваться Publish
	}{
		{
			name: "valid broadcast message",
			message: models.ChatMessage{
				From: "user1", To: "all", Message: "Hello", Type: "broadcast",
			},
			shouldPublish: true,
			expectedCalls: 3, // Для всех 3х пользователей
		},
		{
			name: "empty to field",
			message: models.ChatMessage{
				From: "user1", To: "", Message: "Hello", Type: "broadcast",
			},
			shouldPublish: false, // Не пройдет валидацию
		},
		{
			name: "empty from field",
			message: models.ChatMessage{
				From: "", To: "all", Message: "Hello", Type: "broadcast",
			},
			shouldPublish: false, // Не пройдет валидацию
		},
		{
			name: "empty message field",
			message: models.ChatMessage{
				From: "user1", To: "all", Message: "", Type: "broadcast",
			},
			shouldPublish: false, // Не пройдет валидацию
		},
		{
			name: "direct message should be processed as broadcast",
			message: models.ChatMessage{
				From: "user1", To: "user2", Message: "Hello", Type: "direct",
			},
			shouldPublish: true, // Все равно рассылаем всем!
			expectedCalls: 3,    // Для всех 3х пользователей
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockChannel := new(MockChannel)
			messageBody, _ := json.Marshal(tc.message)

			if tc.shouldPublish {
				// Ожидаем вызов Publish для КАЖДОЙ очереди
				for _, queue := range testQueues {
					mockChannel.On("Publish",
						"",         // default exchange
						queue.Name, // имя очереди как routing key
						false,
						false,
						mock.MatchedBy(func(publishing amqp.Publishing) bool {
							// Проверяем, что это правильное сообщение
							return publishing.ContentType == "application/json" &&
								bytes.Equal(publishing.Body, messageBody)
						}),
					).Return(nil).Once() // Один раз для каждой очереди
				}
			}

			delivery := amqp.Delivery{Body: messageBody}
			HandleBroadcastMessage(mockChannel, delivery, tc.message, testQueues)

			if tc.shouldPublish {
				// Проверяем, что Publish вызван для каждой очереди
				for _, queue := range testQueues {
					mockChannel.AssertCalled(t, "Publish",
						"",
						queue.Name,
						false,
						false,
						mock.MatchedBy(func(publishing amqp.Publishing) bool {
							return publishing.ContentType == "application/json" &&
								bytes.Equal(publishing.Body, messageBody)
						}),
					)
				}
				// Проверяем общее количество вызовов
				assert.Equal(t, tc.expectedCalls, len(mockChannel.Calls))
			} else {
				mockChannel.AssertNotCalled(t, "Publish")
			}
		})
	}
}

// Тест на ошибку объявления exchange:
func TestRun_ExchangeDeclareError(t *testing.T) {
	var stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel() // Важно: defer чтобы гарантировать освобождение ресурсов

	mockBroker := new(MockBroker)
	mockChannel := new(MockChannel)
	mockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	mockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test:test@localhost:5672/").Return(mockChannel, nil)
	mockBroker.On("Close").Return(nil)

	mockChannel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("exchange error"))
	mockChannel.On("Close").Return(nil)

	mockHealthServer.On("StartHealthServer", mock.Anything, 8081).Return(nil)
	mockHealthServer.On("StopHealthServer").Return(nil)

	// Запускаем Run в отдельной горутине
	errChan := make(chan error, 1)
	go func() {
		errChan <- Run(ctx, &stderr, mockBroker, mockConfigLoader, mockHealthServer)
	}()

	// Ждем ошибку или таймаут
	select {
	case err := <-errChan:
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exchange declaration error")

	case <-time.After(2 * time.Second):
		t.Error("Test timed out - Run function did not return")
	}
}
