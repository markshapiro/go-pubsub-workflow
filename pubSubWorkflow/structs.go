package pubSubWorkflow

import (
	amqpWrapper "go-pubsub-workflow/amqp"

	"github.com/go-redis/redis"
	"gopkg.in/mgo.v2/bson"
)

type message struct {
	CallId    string
	MessageId int64
	SessionId int64
	Subject   string
	Args      Args
}

func (m message) MarshalBinary() ([]byte, error) {
	return bson.Marshal(m)
}

func (m *message) UnmarshalBinary(data []byte) error {
	return bson.Unmarshal(data, m)
}

type Args struct {
	Data   string  `bson:"Data,omitempty"`
	Events []Event `bson:"Events,omitempty"`
}

type Publish struct {
	QueueId string
	Subject string
	Data    string `bson:"Data,omitempty"`
}

type EventTrigger struct {
	Events  []string
	Subject string
	Data    string `bson:"Data,omitempty"`
}

func (m EventTrigger) MarshalBinary() ([]byte, error) {
	return bson.Marshal(m)
}

func (m *EventTrigger) UnmarshalBinary(data []byte) error {
	return bson.Unmarshal(data, m)
}

type Event struct {
	Name string
	Data string `bson:"Data,omitempty"`
}

type storedResult struct {
	Publishes     []Publish      `bson:"Publishes,omitempty"`
	EventTriggers []EventTrigger `bson:"EventTriggers,omitempty"`
}

func (m storedResult) MarshalBinary() ([]byte, error) {
	return bson.Marshal(m)
}

func (m *storedResult) UnmarshalBinary(data []byte) error {
	return bson.Unmarshal(data, m)
}

type handlerInfo struct {
	subject   string
	handlerFn handlerFunc
}

type PubSubWorkflow interface {
	Connect(string, string) error
	StartListening() error
	Subscribe(string, handlerFunc) error
	Publish(string, string, ...string) error
	Close() error
}

type pubSubWorkflow struct {
	queueId   string
	amqpConn  *amqpWrapper.AMQPConn
	redisConn *redis.Client
	handlers  *[]handlerInfo
}

type handlerFunc func(string, []Event) ([]Publish, []EventTrigger, error)
