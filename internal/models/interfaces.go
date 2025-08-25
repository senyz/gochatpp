package models

import (
	config "chat-app/internal/config"
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Channel interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Close() error
}

type ConfigLoader interface {
	LoadConfig(path string) (config.Config, error)
}

type HealthServer interface {
	StartHealthServer(ctx context.Context, port int) error
	StopHealthServer() error
}

type MessageBroker interface {
	Dial(url string) (Channel, error)
	Close() error
}
