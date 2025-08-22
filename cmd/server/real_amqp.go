// real_amqp.go - реализация для production
package main

import amqp "github.com/rabbitmq/amqp091-go"

type RealAMQPBroker struct {
	conn *amqp.Connection
}

func (b *RealAMQPBroker) Dial(url string) (Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	b.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// Возвращаем обернутый канал
	return &RealChannel{Channel: ch}, nil
}

func (b *RealAMQPBroker) Close() error {
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}
