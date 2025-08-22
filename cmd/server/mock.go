package main

import (
	config "chat-app/internal/config"
	"net"
	"net/http"
	"strconv"

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

	return callArgs.Get(0).(chan amqp.Delivery), callArgs.Error(1)
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

func (m *MockChannel) Dial(url string) (Channel, error) {
	args := m.Called(url)
	return args.Get(0).(Channel), args.Error(1)
}

// MockBroker implements MessageBroker
type MockBroker struct {
	mock.Mock
}

func (m *MockBroker) Dial(url string) (Channel, error) {
	args := m.Called(url)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(Channel), args.Error(1)

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
	server   *http.Server
	listener net.Listener
}

func (m *MockHealthServer) StartHealthServer(port int) error {
	args := m.Called(port)
	if args.Error(0) != nil {
		return args.Error(0)
	}

	// Опционально: реально запускаем сервер для интеграционных тестов
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	m.server = &http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: mux,
	}

	// Проверяем доступность порта
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	m.listener = listener

	go func() {
		m.server.Serve(listener)
	}()

	return nil
}

func (m *MockHealthServer) StopHealthServer() error {
	args := m.Called()
	if m.server != nil && m.listener != nil {
		m.server.Close()
		m.listener.Close()
	}
	return args.Error(0)
}
