package pubSubWorkflow

import (
	"encoding/json"

	amqpWrapper "github.com/markshapiro/go-pubsub-workflow/amqp"

	"github.com/go-redis/redis"
)

type message struct {
	CallId    string
	MessageId int64
	SessionId int64
	Subject   string
	Args      Args
}

func (m message) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

func (m *message) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}

type Args struct {
	Data   string  `json:"Data,omitempty"`
	Events []Event `json:"Events,omitempty"`
}

const (
	Publish   = "publish"
	EmitEvent = "emitEvent"
)

type Action struct {
	Type    string
	QueueId string `json:"QueueId,omitempty"`
	Event   string `json:"Event,omitempty"`
	Subject string `json:"Subject,omitempty"`
	Data    string `json:"Data,omitempty"`
}

type PublishTrigger struct {
	Events           []string
	PublishTriggerId int64
	QueueId          string `json:"QueueId,omitempty"`
	Subject          string
	Data             string `json:"Data,omitempty"`
}

func (m PublishTrigger) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

func (m *PublishTrigger) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}

type Event struct {
	Name string
	Data string `json:"Data,omitempty"`
}

type storedResult struct {
	Actions         []Action         `json:"Actions,omitempty"`
	PublishTriggers []PublishTrigger `json:"PublishTriggers,omitempty"`
}

func (m storedResult) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

func (m *storedResult) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
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
	Reset() error
	Close() error
}

type pubSubWorkflow struct {
	queueId   string
	amqpConn  *amqpWrapper.AMQPConn
	redisConn *redis.Client
	handlers  *[]handlerInfo
}

type handlerFunc func(string, []Event) ([]Action, []PublishTrigger, error)
