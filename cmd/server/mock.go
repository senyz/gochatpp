package main

import (
	config "chat-app/internal/config"
	"chat-app/internal/models"
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/mock"
)

// MockChannel мок для rabbitmq.Channel
type MockChannel struct {
	mock.Mock
}

func (m *MockChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	callArgs := m.Called(exchange, key, mandatory, immediate, msg)
	return callArgs.Error(0)
}

func (m *MockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	callArgs := m.Called(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if callArgs.Get(0) == nil {
		return nil, callArgs.Error(1)
	}

	return callArgs.Get(0).(<-chan amqp.Delivery), callArgs.Error(1)
}

func (m *MockChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	callArgs := m.Called(name, kind, durable, autoDelete, internal, noWait, args)
	return callArgs.Error(0)
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

func (m *MockChannel) Dial(url string) (models.Channel, error) {
	args := m.Called(url)
	return args.Get(0).(models.Channel), args.Error(1)
}

// MockBroker implements MessageBroker
type MockBroker struct {
	mock.Mock
}

func (m *MockBroker) Dial(url string) (models.Channel, error) {
	args := m.Called(url)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(models.Channel), args.Error(1)

}

func (m *MockBroker) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Mock для тестов
type MockConfig struct {
	RabbitMQURL  string
	ExchangeName string
	AuthFile     string
	LogLevel     string
	ServerPort   int
	Pass         string
	User         string
}

func (m *MockConfig) GetRabbitUser() string {
	return m.RabbitMQURL
}
func (m *MockConfig) GetRabbitPass() string {
	return m.RabbitMQURL
}
func (m *MockConfig) GetRabbitMQURL() string {
	return m.RabbitMQURL
}

func (m *MockConfig) GetExchangeName() string {
	return m.ExchangeName
}

func (m *MockConfig) GetServerPort() int {
	return m.ServerPort
}
func (m *MockConfig) GetAuthFile() string {
	return "test_users.json"
}

func (m *MockConfig) GetLogLevel() string {
	return "debug"
}

type MockConfigLoader struct {
	mock.Mock
}

func (m *MockConfigLoader) LoadConfig(path string) (config.Config, error) {
	args := m.Called(path)

	// Правильная обработка возвращаемых значений
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(config.Config), args.Error(1)
}

type MockHealthServer struct {
	mock.Mock
	// Убираем реальные поля сервера - это же мок!
	// server   *http.Server
	// listener net.Listener
}

func (m *MockHealthServer) StartHealthServer(ctx context.Context, port int) error {
	args := m.Called(ctx, port) // Передаем оба аргумента
	return args.Error(0)        // Возвращаем только одну ошибку!
}

func (m *MockHealthServer) StopHealthServer() error {
	args := m.Called()
	return args.Error(0) // Возвращаем ошибку
}
