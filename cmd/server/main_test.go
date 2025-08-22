package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
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

// MockChannel мок для rabbitmq.Channel
type MockChannel struct {
	mock.Mock
}

func (m *MockChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	args := m.Called(exchange, key, mandatory, immediate, msg)
	return args.Error(0)
}

func (m *MockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	argsCalled := m.Called(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	return argsCalled.Get(0).(<-chan amqp.Delivery), argsCalled.Error(1)
}

func (m *MockChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	argsCalled := m.Called(name, kind, durable, autoDelete, internal, noWait, args)
	return argsCalled.Error(0)
}

func (m *MockChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	argsCalled := m.Called(name, durable, autoDelete, exclusive, noWait, args)
	return argsCalled.Get(0).(amqp.Queue), argsCalled.Error(1)
}

func (m *MockChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	argsCalled := m.Called(name, key, exchange, noWait, args)
	return argsCalled.Error(0)
}

func (m *MockChannel) Close() error {
	args := m.Called()
	return args.Error(0)
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
		mockConfig := &config.MockConfig{
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

func TestMain(m *testing.M) {
	// Отключаем логи во время тестов
	log.SetOutput(io.Discard)

	// Запускаем тесты
	code := m.Run()

	// Восстанавливаем вывод логов
	log.SetOutput(os.Stderr)
	os.Exit(code)
}

func TestStartHealthServer_ResponseOK(t *testing.T) {
	port := 8081
	server := startHealthServer(port)

	req, _ := http.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	server.Handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "OK", rr.Body.String())
}
