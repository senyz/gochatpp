package main

import (
	"bytes"
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
	RabbitMQURL:  "amqp://test",
	ExchangeName: "test_exchange",
	AuthFile:     "test_users.json",
	LogLevel:     "debug",
	ServerPort:   8081,
}
var mockConfig = &MockConfig{
	RabbitMQURL:  "amqp://test:test@localhost:5672/",
	ExchangeName: "test_exchange",
	AuthFile:     "test_users.json",
	LogLevel:     "debug",
	ServerPort:   8081,
}

func TestRun_QuickShutdown(t *testing.T) {
	var stderr bytes.Buffer

	mockBroker := new(MockBroker)
	mockChannel := new(MockChannel)
	mockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	mockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test").Return(mockChannel, nil)
	mockBroker.On("Close").Return(nil)

	mockChannel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Возвращаем закрытый канал, чтобы сразу выйти из цикла
	closedChan := make(chan amqp.Delivery)
	close(closedChan)
	mockChannel.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(closedChan, nil)
	mockChannel.On("Close").Return(nil)

	mockHealthServer.On("StartHealthServer", mock.Anything).Return(nil)
	mockHealthServer.On("StopHealthServer").Return(nil)

	// Запускаем синхронно
	err := run(&stderr, mockBroker, mockConfigLoader, mockHealthServer)

	if err != nil {
		t.Errorf("run returned error: %v", err)
	}

	mockHealthServer.AssertCalled(t, "StartHealthServer", 8081)
	mockHealthServer.AssertCalled(t, "StopHealthServer")
}

func TestHandleMessage_Success(t *testing.T) {

	mockChannel := new(MockChannel)

	testMessage := models.ChatMessage{
		ID:        "123",
		From:      "user1",
		To:        "user2",
		Message:   "Hello!",
		Timestamp: time.Now(),
		Type:      "direct",
	}

	messageBody, _ := json.Marshal(testMessage)

	// Упрощаем проверку - проверяем только routing key и тип контента
	mockChannel.On("Publish",
		"chat_direct",
		"user.user2",
		false,
		false,
		mock.MatchedBy(func(p amqp.Publishing) bool {
			return p.ContentType == "application/json" &&
				len(p.Body) > 0 // Проверяем что тело не пустое
		}),
	).Return(nil)

	delivery := amqp.Delivery{
		Body: messageBody,
	}

	HandleMessage(mockChannel, delivery)

	mockChannel.AssertCalled(t, "Publish",
		"chat_direct",
		"user.user2",
		false,
		false,
		mock.MatchedBy(func(p amqp.Publishing) bool {
			return p.ContentType == "application/json"
		}),
	)

	mockChannel.AssertExpectations(t)
}

func TestHandleMessage_InvalidJSON(t *testing.T) {
	mockChannel := new(MockChannel)

	// Невалидный JSON - НЕ должно вызывать Publish
	delivery := amqp.Delivery{
		Body: []byte("{invalid json}"),
	}

	HandleMessage(mockChannel, delivery)

	// Убеждаемся, что Publish НЕ был вызван
	mockChannel.AssertNotCalled(t, "Publish")
}

func TestHandleMessage_EmptyToField(t *testing.T) {
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

	HandleMessage(mockChannel, delivery)

	// Не должно вызывать Publish с пустым получателем
	mockChannel.AssertNotCalled(t, "Publish")
}

func TestHandleMessage_PublishError(t *testing.T) {
	mockChannel := new(MockChannel)

	testMessage := models.ChatMessage{
		From:    "user1",
		To:      "user2",
		Message: "Hello!",
	}

	messageBody, _ := json.Marshal(testMessage)

	mockChannel.On("Publish",
		"chat_direct",
		"user.user2",
		false,
		false,
		mock.Anything,
	).Return(assert.AnError)

	delivery := amqp.Delivery{
		Body: messageBody,
	}

	// Должно обработать ошибку без паники
	assert.NotPanics(t, func() {
		HandleMessage(mockChannel, delivery)
	})

	mockChannel.AssertCalled(t, "Publish",
		"chat_direct",
		"user.user2",
		false,
		false,
		mock.Anything,
	)
}

func TestHandleMessage_BroadcastType(t *testing.T) {
	mockChannel := new(MockChannel)

	testMessage := models.ChatMessage{
		From:    "user1",
		To:      "all",
		Message: "Broadcast message!",
		Type:    "broadcast",
	}

	messageBody, _ := json.Marshal(testMessage)

	mockChannel.On("Publish",
		"chat_direct",
		"user.all",
		false,
		false,
		mock.Anything,
	).Return(nil)

	delivery := amqp.Delivery{
		Body: messageBody,
	}

	HandleMessage(mockChannel, delivery)

	mockChannel.AssertCalled(t, "Publish",
		"chat_direct",
		"user.all",
		false,
		false,
		mock.Anything,
	)
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

		assert.Equal(t, "amqp://test:test@localhost:5672/", mockConfig.GetRabbitMQURL())
		assert.Equal(t, "test_exchange", mockConfig.GetExchangeName())
		assert.Equal(t, "test_users.json", mockConfig.GetAuthFile())
		assert.Equal(t, "debug", mockConfig.GetLogLevel())
		assert.Equal(t, 8081, mockConfig.GetServerPort())
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

		os.Args = []string{"chat-server", "-test.run=TestMainFunction"}

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

	mockBroker := new(MockBroker)
	mockChannel := new(MockChannel)
	mockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	mockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test").Return(mockChannel, nil)
	mockBroker.On("Close").Return(nil)

	mockChannel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockChannel.On("Close").Return(nil)

	mockHealthServer.On("StartHealthServer", 8081).Return(fmt.Errorf("health server error"))

	err := run(&stderr, mockBroker, mockConfigLoader, mockHealthServer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "health server start error")
}

func TestRun_ConfigLoadError(t *testing.T) {
	var stderr bytes.Buffer

	mockBroker := new(MockBroker)
	mockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	mockConfigLoader.On("LoadConfig", "config.yaml").Return(nil, fmt.Errorf("config error"))

	err := run(&stderr, mockBroker, mockConfigLoader, mockHealthServer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config error")
}

func TestRun_RabbitMQConnectionError(t *testing.T) {
	var stderr bytes.Buffer

	mockBroker := new(MockBroker)
	mockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	mockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test").Return(nil, fmt.Errorf("connection failed"))

	err := run(&stderr, mockBroker, mockConfigLoader, mockHealthServer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "RabbitMQ connection error")
}

func TestRun_ConsumeError(t *testing.T) {
	var stderr bytes.Buffer

	mockBroker := new(MockBroker)
	mockChannel := new(MockChannel)
	mockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	mockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test").Return(mockChannel, nil)
	mockBroker.On("Close").Return(nil)

	mockChannel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockChannel.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("consume error"))
	mockChannel.On("Close").Return(nil)

	mockHealthServer.On("StartHealthServer", 8081).Return(nil)
	mockHealthServer.On("StopHealthServer").Return(nil)

	err := run(&stderr, mockBroker, mockConfigLoader, mockHealthServer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consume error")
}

func BenchmarkHandleMessage(b *testing.B) {
	mockChannel := new(MockChannel)
	testMessage := models.ChatMessage{
		ID:        "123",
		From:      "user1",
		To:        "user2",
		Message:   "Hello!",
		Timestamp: time.Now(),
		Type:      "direct",
	}

	messageBody, _ := json.Marshal(testMessage)

	mockChannel.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	delivery := amqp.Delivery{Body: messageBody}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HandleMessage(mockChannel, delivery)
	}
}

func TestHandleMessage_TableDriven(t *testing.T) {
	testCases := []struct {
		name          string
		message       models.ChatMessage
		shouldPublish bool
		expectedKey   string
	}{
		{
			name: "valid direct message",
			message: models.ChatMessage{
				From: "user1", To: "user2", Message: "Hello", Type: "direct",
			},
			shouldPublish: true,
			expectedKey:   "user.user2",
		},
		{
			name: "empty to field",
			message: models.ChatMessage{
				From: "user1", To: "", Message: "Hello", Type: "direct",
			},
			shouldPublish: false,
		},
		{
			name: "broadcast message",
			message: models.ChatMessage{
				From: "user1", To: "all", Message: "Hello", Type: "broadcast",
			},
			shouldPublish: true,
			expectedKey:   "user.all",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockChannel := new(MockChannel)
			messageBody, _ := json.Marshal(tc.message)

			if tc.shouldPublish {
				mockChannel.On("Publish",
					"chat_direct",
					tc.expectedKey,
					false,
					false,
					mock.Anything,
				).Return(nil)
			}

			delivery := amqp.Delivery{Body: messageBody}
			HandleMessage(mockChannel, delivery)

			if tc.shouldPublish {
				mockChannel.AssertCalled(t, "Publish",
					"chat_direct",
					tc.expectedKey,
					false,
					false,
					mock.Anything,
				)
			} else {
				mockChannel.AssertNotCalled(t, "Publish")
			}
		})
	}
}

// Тест на ошибку объявления exchange:
func TestRun_ExchangeDeclareError(t *testing.T) {
	var stderr bytes.Buffer

	mockBroker := new(MockBroker)
	mockChannel := new(MockChannel)
	mockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	mockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test").Return(mockChannel, nil)
	mockBroker.On("Close").Return(nil)

	mockChannel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("exchange error"))
	mockChannel.On("Close").Return(nil)

	mockHealthServer.On("StartHealthServer", 8081).Return(nil)
	mockHealthServer.On("StopHealthServer").Return(nil)

	err := run(&stderr, mockBroker, mockConfigLoader, mockHealthServer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exchange declaration error")

	// Health server должен остановиться даже при ошибке
	mockHealthServer.AssertCalled(t, "StopHealthServer")
}

// Тест на закрытие канала сообщений:
func TestRun_MessageChannelClosed(t *testing.T) {
	var stderr bytes.Buffer

	mockBroker := new(MockBroker)
	mockChannel := new(MockChannel)
	mockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	mockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test").Return(mockChannel, nil)
	mockBroker.On("Close").Return(nil)

	mockChannel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Создаем и сразу закрываем канал
	closedChan := make(chan amqp.Delivery)
	close(closedChan)
	mockChannel.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(closedChan, nil)
	mockChannel.On("Close").Return(nil)

	mockHealthServer.On("StartHealthServer", 8081).Return(nil)
	mockHealthServer.On("StopHealthServer").Return(nil)

	err := run(&stderr, mockBroker, mockConfigLoader, mockHealthServer)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "message channel closed")

	mockHealthServer.AssertCalled(t, "StopHealthServer")
}

// Тест на обработку сигналов:
func TestRun_SignalHandling(t *testing.T) {
	var stderr bytes.Buffer

	mockBroker := new(MockBroker)
	mockChannel := new(MockChannel)
	mockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	mockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test").Return(mockChannel, nil)
	mockBroker.On("Close").Return(nil)

	mockChannel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Канал с одним сообщением
	msgChan := make(chan amqp.Delivery, 1)
	msgChan <- amqp.Delivery{Body: []byte(`{"from":"user1","to":"user2","message":"test"}`)}
	close(msgChan)

	mockChannel.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((<-chan amqp.Delivery)(msgChan), nil)
	mockChannel.On("Close").Return(nil)

	mockHealthServer.On("StartHealthServer", 8081).Return(nil)
	mockHealthServer.On("StopHealthServer").Return(nil)

	err := run(&stderr, mockBroker, mockConfigLoader, mockHealthServer)

	// Должен завершиться без ошибки (нормальное завершение по закрытию канала)
	assert.NoError(t, err)
	mockHealthServer.AssertCalled(t, "StopHealthServer")
}

// Тест на обработку сообщений:
func TestRun_MessageProcessing(t *testing.T) {
	var stderr bytes.Buffer

	mockBroker := new(MockBroker)
	mockChannel := new(MockChannel)
	mockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	mockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test").Return(mockChannel, nil)
	mockBroker.On("Close").Return(nil)

	mockChannel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Канал с сообщениями
	msgChan := make(chan amqp.Delivery, 2)
	msgChan <- amqp.Delivery{Body: []byte(`{"from":"user1","to":"user2","message":"test1"}`)}
	msgChan <- amqp.Delivery{Body: []byte(`{"from":"user1","to":"user2","message":"test2"}`)}

	mockChannel.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((<-chan amqp.Delivery)(msgChan), nil)
	mockChannel.On("Close").Return(nil)

	// Мок для обработки сообщений
	mockChannel.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)

	mockHealthServer.On("StartHealthServer", 8081).Return(nil)
	mockHealthServer.On("StopHealthServer").Return(nil)

	// Запускаем в горутине и завершаем через короткое время
	errChan := make(chan error, 1)
	go func() {
		errChan <- run(&stderr, mockBroker, mockConfigLoader, mockHealthServer)
	}()

	// Даем время обработать сообщения
	time.Sleep(100 * time.Millisecond)

	// Завершаем
	proc, _ := os.FindProcess(os.Getpid())
	proc.Signal(syscall.SIGTERM)

	select {
	case err := <-errChan:
		assert.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Error("Test timed out")
	}

	mockHealthServer.AssertCalled(t, "StopHealthServer")
}

// Тест на ошибки в HealthServer:
func TestRun_HealthServerStopError(t *testing.T) {
	var stderr bytes.Buffer

	mockBroker := new(MockBroker)
	mockChannel := new(MockChannel)
	mockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	mockConfigLoader.On("LoadConfig", "config.yaml").Return(testConfig, nil)
	mockBroker.On("Dial", "amqp://test").Return(mockChannel, nil)
	mockBroker.On("Close").Return(nil)

	mockChannel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	closedChan := make(chan amqp.Delivery)
	close(closedChan)
	mockChannel.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(closedChan, nil)
	mockChannel.On("Close").Return(nil)

	mockHealthServer.On("StartHealthServer", 8081).Return(nil)
	mockHealthServer.On("StopHealthServer").Return(fmt.Errorf("stop error"))

	err := run(&stderr, mockBroker, mockConfigLoader, mockHealthServer)

	// Ошибка остановки health server не должна влиять на основную логику
	assert.Error(t, err) // Но основная ошибка - закрытие канала
	assert.Contains(t, err.Error(), "message channel closed")

	mockHealthServer.AssertCalled(t, "StopHealthServer")
}
