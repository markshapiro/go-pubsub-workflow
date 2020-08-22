package amqp

import (
	"sync"

	"github.com/streadway/amqp"
)

type AMQPConn struct {
	*amqp.Connection
	channel *amqp.Channel
}

var (
	once     sync.Once
	mutex    sync.Mutex
	instance *AMQPConn
)

func GetInstance() *AMQPConn {
	once.Do(func() {
		instance = &AMQPConn{}
	})
	return instance
}

func (conn *AMQPConn) Connect(url string) error {
	var err error
	conn.Connection, err = amqp.Dial(url)
	if err != nil {
		return err
	}

	conn.channel, err = conn.Channel()
	if err != nil {
		return err
	}

	return nil
}

func (conn *AMQPConn) CreateQueue(queueName string) error {
	_, err := conn.channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return err
	}

	return nil
}

func (conn *AMQPConn) GetChannel() *amqp.Channel {
	return conn.channel
}

func (conn *AMQPConn) Close() error {
	err := conn.channel.Close()
	if err != nil {
		return err
	}
	return conn.Connection.Close()
}
