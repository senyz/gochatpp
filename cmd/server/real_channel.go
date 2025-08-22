// real_channel.go
package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type RealChannel struct {
	*amqp.Channel
}

func (c *RealChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return c.Channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (c *RealChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return c.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (c *RealChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return c.Channel.Publish(exchange, key, mandatory, immediate, msg)
}

func (c *RealChannel) Close() error {
	return c.Channel.Close()
}
