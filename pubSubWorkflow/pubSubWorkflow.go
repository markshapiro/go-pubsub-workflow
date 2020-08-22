package pubSubWorkflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	amqpWrapper "go-pubsub-workflow/amqp"

	redisWrapper "go-pubsub-workflow/redis"

	"github.com/go-redis/redis"
	"github.com/streadway/amqp"
)

var (
	TASK_ALREADY_CREATED = errors.New("TASK_ALREADY_CREATED")
	HANDLER_NOT_FOUND    = errors.New("METHOD_NOT_FOUND")
)

func New(queueId string) PubSubWorkflow {
	amqpConn := amqpWrapper.GetInstance()
	var handlerInfos = []handlerInfo{}
	pubSubWorkflow := pubSubWorkflow{
		queueId,
		amqpConn,
		&redis.Client{},
		&handlerInfos,
	}
	return pubSubWorkflow
}

func (wf pubSubWorkflow) Connect(amqpUrl, redisUrl string) error {
	err := wf.amqpConn.Connect(amqpUrl)
	if err != nil {
		return err
	}
	*wf.redisConn = *redisWrapper.GetInstance(redisUrl)

	conf := wf.redisConn.ConfigGet("appendonly")

	if conf.Err() != nil {
		return conf.Err()
	}

	if conf.String() != "config get appendonly: [appendonly yes]" {
		fmt.Errorf("warning: redis appendonly is not set, it is recommended to set 'appendonly yes' for maximum durability")
	}

	if err != nil {
		return err
	}

	return nil
}

func (wf pubSubWorkflow) StartListening() error {
	err := wf.amqpConn.CreateQueue(wf.queueId)
	if err != nil {
		return err
	}
	amqpMsgs, err := wf.amqpConn.GetChannel().Consume(
		wf.queueId, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	for amqpMsg := range amqpMsgs {
		fmt.Println("Received a message: ", string(amqpMsg.Body))

		var msg message

		err := msg.UnmarshalBinary(amqpMsg.Body)
		if err != nil {
			return err
		}

		err = wf.processMsg(msg)
		if err != nil {
			if err == TASK_ALREADY_CREATED || err == HANDLER_NOT_FOUND {
				amqpMsg.Reject(false)
			} else {
				amqpMsg.Nack(false, true)
			}
			continue
		}

		amqpMsg.Ack(false)
	}

	return nil
}

func (wf pubSubWorkflow) processMsg(msg message) error {
	var handlerFn handlerFunc

	for _, handler := range *wf.handlers {
		if msg.Subject == handler.subject {
			handlerFn = handler.handlerFn
		}
	}

	if handlerFn == nil {
		return HANDLER_NOT_FOUND
	}

	cmd := wf.redisConn.HSetNX(fmt.Sprintf("%s.data", msg.CallId), "msgVerificationId", msg.MessageId)
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		return cmd.Err()
	}
	if cmd.Val() == false {
		getCmd := wf.redisConn.HGet(fmt.Sprintf("%s.data", msg.CallId), "msgVerificationId")
		if getCmd.Err() != nil && cmd.Err() != redis.Nil {
			return getCmd.Err()
		}

		firstMessageId, err := getCmd.Int64()
		if err != nil {
			return err
		}
		if firstMessageId != msg.MessageId {
			return TASK_ALREADY_CREATED
		}
	}

	nextPublishes, nextEvents, err := handlerFn(msg.Args.Data, msg.Args.Events)
	if err != nil {
		return err
	}

	result := storedResult{nextPublishes, nextEvents}

	storeCmd := wf.redisConn.HSetNX(fmt.Sprintf("%s.data", msg.CallId), "result", result)
	if storeCmd.Err() != nil && cmd.Err() != redis.Nil {
		return storeCmd.Err()
	}

	if cmd.Val() == false {
		getResultCmd := wf.redisConn.HGet(fmt.Sprintf("%s.data", msg.CallId), "result")
		if getResultCmd.Err() != nil && getResultCmd.Err() != redis.Nil {
			return getResultCmd.Err()
		}

		var prevStoredResult storedResult
		err = getResultCmd.Scan(&prevStoredResult)
		if err != nil {
			return err
		}

		nextPublishes = prevStoredResult.Publishes
		nextEvents = prevStoredResult.Events
	}

	return wf.triggerNextTasks(msg, nextPublishes, nextEvents)
}

func (wf pubSubWorkflow) triggerNextTasks(msg message, nextPublishes []Publish, nextEvents []Event) error {
	subjectCounts := make(map[string]int)

	fmt.Println(msg, "|", nextPublishes)

	for _, publish := range nextPublishes {
		nextCallId := fmt.Sprintf("%d.call.%s.%d", msg.MessageId, publish.Subject, subjectCounts[publish.Subject])
		nextMessageId, err := wf.getUniqueNum()
		if err != nil {
			return err
		}
		nextMsg := message{nextCallId, nextMessageId, publish.Subject, Args{publish.Data, nil}}
		err = wf.publish(nextMsg, publish.QueueId)
		if err != nil {
			return err
		}
		subjectCounts[publish.Subject]++
	}

	return nil
}

func (wf pubSubWorkflow) Publish(subject, data string, uniqueCallId ...string) error {
	var callId string

	if len(uniqueCallId) >= 1 {
		callId = uniqueCallId[0]
	} else {
		callIdNo, err := wf.getUniqueNum()
		if err != nil {
			return err
		}
		callId = fmt.Sprintf("%d", callIdNo)
	}

	messageId, err := wf.getUniqueNum()
	if err != nil {
		return err
	}

	err = wf.publish(message{callId, messageId, subject, Args{data, nil}}, "")
	if err != nil {
		return err
	}

	return nil
}

func (wf pubSubWorkflow) Subscribe(subject string, handler handlerFunc) error {
	if strings.Contains(subject, ".") {
		return errors.New("dot '.' symbol is reserved")
	}
	*wf.handlers = append(*wf.handlers, handlerInfo{subject, handler})
	return nil
}

func (wf pubSubWorkflow) getUniqueNum() (int64, error) {
	cmd := wf.redisConn.Incr("uniqueNum")
	if cmd.Err() != nil {
		return 0, cmd.Err()
	}
	return cmd.Result()
}

func (wf pubSubWorkflow) publish(msg message, queueId string) error {
	var err error
	msg.MessageId, err = wf.getUniqueNum()
	if err != nil {
		return err
	}

	bytes, err := msg.MarshalBinary()
	if err != nil {
		return err
	}

	if queueId == "" {
		queueId = wf.queueId
	}

	return wf.amqpConn.GetChannel().Publish(
		"",      // exchange
		queueId, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        bytes,
		})
}

func (wf pubSubWorkflow) Close() error {
	err := wf.amqpConn.GetChannel().Close()
	if err != nil {
		return err
	}
	err = wf.amqpConn.Close()
	if err != nil {
		return err
	}
	err = wf.redisConn.Close()
	if err != nil {
		return err
	}
	return nil
}

func toJSON(obj interface{}) ([]byte, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return b, err
}

func PublishNext(data ...string) []Publish {
	var result []Publish
	for ind := range data {
		if ind%2 == 0 {
			dotSymbol := strings.Index(data[ind], ".")
			if dotSymbol >= 0 {
				queueId := data[ind][:dotSymbol]
				topic := data[ind][dotSymbol+1:]
				result = append(result, Publish{queueId, topic, data[ind+1]})
			} else {
				result = append(result, Publish{"", data[ind], data[ind+1]})
			}
		}
	}
	return result
}
