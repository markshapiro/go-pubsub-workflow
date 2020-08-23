package pubSubWorkflow

import (
	"errors"
	"fmt"
	"strings"

	amqpWrapper "go-pubsub-workflow/amqp"

	redisWrapper "go-pubsub-workflow/redis"

	"github.com/go-redis/redis"
	"github.com/streadway/amqp"
)

var (
	CALL_ALREADY_CREATED = errors.New("CALL_ALREADY_CREATED")
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
		//fmt.Println("Received a message: ", string(amqpMsg.Body))

		var msg message

		err := msg.UnmarshalBinary(amqpMsg.Body)
		if err != nil {
			return err
		}

		err = wf.processMsg(msg)
		if err != nil {
			if err == CALL_ALREADY_CREATED || err == HANDLER_NOT_FOUND {
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

	cmd := wf.redisConn.HSetNX(fmt.Sprintf("call.%s.data", msg.CallId), "verificationId", msg.MessageId)
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		return cmd.Err()
	}
	if cmd.Val() == false {
		getCmd := wf.redisConn.HGet(fmt.Sprintf("call.%s.data", msg.CallId), "verificationId")
		if getCmd.Err() != nil && cmd.Err() != redis.Nil {
			return getCmd.Err()
		}

		firstMessageId, err := getCmd.Int64()
		if err != nil {
			return err
		}
		if firstMessageId != msg.MessageId {
			return CALL_ALREADY_CREATED
		}
	}

	nextActions, nextEventListeners, err := handlerFn(msg.Args.Data, msg.Args.Events)
	if err != nil {
		return err
	}

	for ind := range nextEventListeners {
		nextEventListeners[ind].EventListenerId, err = wf.getUniqueNum()
		if err != nil {
			return err
		}
	}

	result := storedResult{nextActions, nextEventListeners}

	storeCmd := wf.redisConn.HSetNX(fmt.Sprintf("call.%s.data", msg.CallId), "result", result)
	if storeCmd.Err() != nil && cmd.Err() != redis.Nil {
		return storeCmd.Err()
	}

	if cmd.Val() == false {
		getResultCmd := wf.redisConn.HGet(fmt.Sprintf("call.%s.data", msg.CallId), "result")
		if getResultCmd.Err() != nil && getResultCmd.Err() != redis.Nil {
			return getResultCmd.Err()
		}

		var prevStoredResult storedResult
		err = getResultCmd.Scan(&prevStoredResult)
		if err != nil {
			return err
		}

		nextActions = prevStoredResult.Actions
		nextEventListeners = prevStoredResult.EventListeners
	}

	return wf.processCallsAndApplyListeners(msg, nextActions, nextEventListeners)
}

func (wf pubSubWorkflow) processCallsAndApplyListeners(msg message, nextActions []Action, nextEventListeners []EventListener) error {

	for _, eventListener := range nextEventListeners {
		cmd := wf.redisConn.SAdd(fmt.Sprintf("session.%d.eventListeners", msg.SessionId), eventListener)
		if cmd.Err() != nil && cmd.Err() == redis.Nil {
			return cmd.Err()
		}
	}

	err := wf.publishCalls(msg, nextActions)
	if err != nil {
		return err
	}

	err = wf.emitEvents(msg, nextActions)
	if err != nil {
		return err
	}

	return nil
}

func (wf pubSubWorkflow) publishCalls(msg message, nextActions []Action) error {
	subjectCounts := make(map[string]int)
	for _, action := range nextActions {
		if action.Type == Publish {
			nextCallId := fmt.Sprintf("%d.publish.%s.%d", msg.MessageId, action.Subject, subjectCounts[action.Subject])
			nextMessageId, err := wf.getUniqueNum()
			if err != nil {
				return err
			}
			nextMsg := message{nextCallId, nextMessageId, msg.SessionId, action.Subject, Args{action.Data, nil}}
			err = wf.publish(nextMsg, action.QueueId)
			if err != nil {
				return err
			}
			subjectCounts[action.Subject]++
		}
	}
	return nil
}

func (wf pubSubWorkflow) emitEvents(msg message, nextActions []Action) error {
	var addEvents = []string{"'HSET'", fmt.Sprintf("'session.%d.events'", msg.SessionId)}

	for _, action := range nextActions {
		if action.Type == EmitEvent {
			addEvents = append(addEvents, "'"+action.Event+"'", "'"+action.Data+"'")
		}
	}

	cmd := wf.redisConn.Eval("return redis.call("+strings.Join(addEvents, ", ")+")", []string{})
	if cmd.Err() != nil && cmd.Err() == redis.Nil {
		return cmd.Err()
	}

	membersCmd := wf.redisConn.SMembers(fmt.Sprintf("session.%d.eventListeners", msg.SessionId))
	if membersCmd.Err() != nil && membersCmd.Err() == redis.Nil {
		return membersCmd.Err()
	}

	var eventListeners []EventListener

	err := membersCmd.ScanSlice(&eventListeners)
	if err != nil {
		return err
	}

	var handledListeners = make(map[int]bool)

	for _, action := range nextActions {
		if action.Type == EmitEvent {
			for ind, listener := range eventListeners {
				if handledListeners[ind] {
					continue
				}
				var commonEventExists = false
				for _, listeningingEvent := range listener.Events {
					if action.Event == listeningingEvent {
						commonEventExists = true
					}
				}
				if commonEventExists {
					var getEventsData = []string{}
					for _, listeningingEvent := range listener.Events {
						getEventsData = append(getEventsData, "'"+listeningingEvent+"'")
					}

					cmd := wf.redisConn.Eval("return redis.call('HMGET', "+fmt.Sprintf("'session.%d.events',", msg.SessionId)+strings.Join(getEventsData, ", ")+")", []string{})
					if cmd.Err() != nil && cmd.Err() == redis.Nil {
						return cmd.Err()
					}

					values := cmd.Val().([]interface{})

					var args []Event
					for valInd, val := range values {
						if val != nil {
							args = append(args, Event{listener.Events[valInd], val.(string)})
						}
					}

					if len(values) == len(args) {
						nextMessageId, err := wf.getUniqueNum()
						if err != nil {
							return err
						}

						nextCallId := fmt.Sprintf("%d.event.%d", msg.MessageId, listener.EventListenerId)
						nextMsg := message{nextCallId, nextMessageId, msg.SessionId, listener.Subject, Args{listener.Data, args}}
						err = wf.publish(nextMsg, listener.QueueId)
						if err != nil {
							return err
						}
						handledListeners[ind] = true
					}

				}
			}
		}
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

	sessionId, err := wf.getUniqueNum()
	if err != nil {
		return err
	}

	err = wf.publish(message{callId, messageId, sessionId, subject, Args{data, nil}}, "")
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
	cmd := wf.redisConn.Incr("pubSubWorkflow.uniqueNum")
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

func PublishNext(data ...string) []Action {
	var result []Action
	for ind := range data {
		if ind%2 == 0 {
			queueId, subject := getQueueAndSubject(data[ind])
			result = append(result, Action{Publish, queueId, "", subject, data[ind+1]})
		}
	}
	return result
}

func NewEventListener(dest string, data string, events ...string) EventListener {
	queueId, subject := getQueueAndSubject(dest)
	return EventListener{
		Events:  events,
		QueueId: queueId,
		Subject: subject,
		Data:    data,
	}
}

func getQueueAndSubject(dest string) (string, string) {
	dotSymbol := strings.Index(dest, ".")
	if dotSymbol >= 0 {
		return dest[:dotSymbol], dest[dotSymbol+1:]
	}
	return "", dest
}

func EmitEvents(data ...string) []Action {
	var result []Action
	for ind := range data {
		if ind%2 == 0 {
			result = append(result, Action{EmitEvent, "", data[ind], "", data[ind+1]})
		}
	}
	return result
}
