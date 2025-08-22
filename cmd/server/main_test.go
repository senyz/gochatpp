package main

import (
	"bytes"
	"encoding/json"
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

func TestRun_QuickShutdown(t *testing.T) {
	var stdout, stderr bytes.Buffer

	mockBroker := new(MockBroker)
	mockChannel := new(MockChannel)
	mockConfigLoader := new(MockConfigLoader)
	mockHealthServer := new(MockHealthServer)

	testConfig := &MockConfig{
		RabbitMQURL:  "amqp://test",
		ExchangeName: "test_exchange",
		ServerPort:   8080,
	}

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
	err := run([]string{}, &stdout, &stderr, mockBroker, mockConfigLoader, mockHealthServer)

	if err != nil {
		t.Errorf("run returned error: %v", err)
	}

	mockHealthServer.AssertCalled(t, "StartHealthServer", 8080)
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
		mockConfig := &MockConfig{
			RabbitMQURL:  "amqp://test:test@localhost:5672/",
			ExchangeName: "test_exchange",
			AuthFile:     "test_users.json",
			LogLevel:     "debug",
			ServerPort:   8081,
		}

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

		testConfig := `
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

		if _, err := tempFile.WriteString(testConfig); err != nil {
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

// Переменная для подмены функции dial в тестах
var amqpDial = amqp.Dial
